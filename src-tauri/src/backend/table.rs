use std::collections::HashMap;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{Error as RusqliteError, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{column_type, db};
use crate::util::error;






/// Creates a new table.
pub fn create(name: String) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Add metadata for the table
    trans.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (3);", [])?;
    let table_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_TABLE (OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name]
    )?;

    // Create the table
    let create_table_cmd: String = format!("
    CREATE TABLE TABLE{table_oid} (
        OID INTEGER PRIMARY KEY, 
        TRASH INTEGER NOT NULL DEFAULT 0
    ) STRICT;");
    trans.execute(&create_table_cmd, [])?;
    
    // Update the surrogate view
    update_surrogate_view(&trans, table_oid.clone())?;

    // Commit the transaction
    trans.commit()?;
    return Ok(table_oid);
}

/// Update the surrogate view for the table.
pub fn update_surrogate_view(trans: &Transaction, table_oid: i64) -> Result<(), error::Error> {
    let mut select_cols_cmd: String = String::from("t.OID");
    let mut select_tbls_cmd: String = format!("FROM TABLE{table_oid} t");
    let mut select_display_value: Vec<String> = Vec::new(); // The primary key columns
    let mut tbl_count: i64 = 1;

    // Load the column sort order
    struct TableOrderbyClause {
        column_oid: i64,
        sort_ascending: bool 
    }
    let mut table_orderby_clauses: Vec<TableOrderbyClause> = Vec::new();
    for orderby_clause_result in trans.prepare("
        SELECT 
            o.COLUMN_OID, 
            o.SORT_ASCENDING 
        FROM METADATA_TABLE_ORDERBY o
        INNER JOIN METADATA_TABLE_COLUMN c ON c.OID = o.COLUMN_OID
        WHERE o.TABLE_OID = ?1 AND c.TRASH = 0
        ORDER BY 
            o.SORT_ORDERING
        ")?
        .query_and_then(params![table_oid], 
        |row: &Row<'_>| -> Result<TableOrderbyClause, RusqliteError> {
            return Ok(TableOrderbyClause{
                column_oid: row.get(0)?,
                sort_ascending: row.get(1)?
            });
        })? {
        
        table_orderby_clauses.push(orderby_clause_result?);
    }
    let mut select_any_col: HashMap<i64, String> = HashMap::new();

    // Iterate over all columns of the table, building up the table's view
    db::query_iterate(trans, 
        "SELECT
            c.OID,
            c.TYPE_OID,
            t.MODE,
            c.IS_PRIMARY_KEY
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1 AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;", 
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get(0)?;
            let column_type: column_type::MetadataColumnType = column_type::MetadataColumnType::from_database(row.get(1)?, row.get(2)?);
            let select_col: String;
            match column_type {
                column_type::MetadataColumnType::Primitive(prim) => {
                    match prim {
                        column_type::Primitive::Any 
                        | column_type::Primitive::Boolean
                        | column_type::Primitive::Integer
                        | column_type::Primitive::Number
                        | column_type::Primitive::Text
                        | column_type::Primitive::JSON => {
                            select_col = format!("CAST(t.COLUMN{column_oid} AS TEXT) AS COLUMN{column_oid}");
                        },
                        column_type::Primitive::Date => {
                            select_col = format!("DATE(t.COLUMN{column_oid}) AS COLUMN{column_oid}");
                        },
                        column_type::Primitive::Timestamp => {
                            select_col = format!("DATETIME(t.COLUMN{column_oid}) AS COLUMN{column_oid}");
                        },
                        column_type::Primitive::File => {
                            select_col = format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE 'File' END AS COLUMN{column_oid}");
                        },
                        column_type::Primitive::Image => {
                            select_col = format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS COLUMN{column_oid}");
                        }
                    }
                },
                column_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_col = format!("t{tbl_count}.VALUE AS COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_col = format!("(SELECT '[' || GROUP_CONCAT(b.VALUE) || ']' FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID) AS COLUMN{column_oid}");
                },
                column_type::MetadataColumnType::Reference(referenced_table_oid) 
                | column_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_col = format!("t{tbl_count}.DISPLAY_VALUE AS COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_col = format!("(SELECT '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID) AS COLUMN{column_oid}");
                }
            }

            select_cols_cmd = format!("{select_cols_cmd}, {select_col}");
            if row.get::<_, bool>(3)? {
                select_display_value.push(select_col.clone());
            }
            if table_orderby_clauses.len() > 0 {
                select_any_col.insert(column_oid, select_col.clone());
            }
            return Ok(());
        }
    )?;

    // Build the ORDERBY clause, if there is one
    let mut select_order_cmd: String;
    if table_orderby_clauses.len() > 0 {
        select_order_cmd = String::from("ORDER BY");
        for orderby_clause in table_orderby_clauses {
            match select_any_col.get(&orderby_clause.column_oid) {
                Some(select_col) => {
                    if select_order_cmd.eq("ORDER BY") {
                        select_order_cmd = format!("{select_order_cmd} {select_col} {}", if orderby_clause.sort_ascending { "ASC" } else { "DESC" });
                    } else {
                        select_order_cmd = format!("{select_order_cmd}, {select_col} {}", if orderby_clause.sort_ascending { "ASC" } else { "DESC" });
                    }
                }
                None => {
                    return Err(error::Error::AdhocError(""));
                }
            }
        }
        select_cols_cmd = format!("ROW_NUMBER() OVER ({select_order_cmd}) AS ROW_INDEX, {select_cols_cmd}");
    } else {
        select_order_cmd = String::from("");
        select_cols_cmd = format!("ROW_NUMBER() OVER (ORDER BY t.OID) AS ROW_INDEX, {select_cols_cmd}");
    }

    let create_view_cmd: String = format!("
        CREATE VIEW TABLE{table_oid}_SURROGATE 
        AS 
        SELECT
            {select_cols_cmd} 
            , CASE WHEN t.TRASH = 0 THEN {} ELSE '— DELETED —' END AS DISPLAY_VALUE
        {select_tbls_cmd}
        {select_order_cmd}",

        if select_display_value.len() > 1 {
            format!("'{{ ' || {} || ' }}'", select_display_value.join(" || ', ' || "))
        } else if select_display_value.len() == 1 {
            select_display_value[0].clone()
        } else {
            String::from("— NO PRIMARY KEY —")
        }
    );
    trans.execute(&create_view_cmd, params![])?;
    return Ok(());
}

/// Flags a table as trash.
pub fn move_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute("UPDATE METADATA_TABLE SET TRASH = 1 WHERE OID = ?1;", params![table_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a table as trash.
pub fn unmove_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute("UPDATE METADATA_TABLE SET TRASH = 0 WHERE OID = ?1;", params![table_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Deletes the table with the given OID and all associated local columns.
/// Generally, this function should only be called after the table has been flagged as trash for reasonably long enough that the user could undo it if they wanted to.
pub fn delete(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Drop data from the table
    let drop_cmd: String = format!("DROP TABLE IF EXISTS TABLE{table_oid};");
    trans.execute(&drop_cmd, [])?;

    // Drop tables associated locally with the table
    // TODO

    // Finally, drop the table's metadata
    trans.execute("DELETE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = ?1;", params![table_oid])?;
    return Ok(());
}



#[derive(Serialize)]
/// The most bare-bones version of table metadata, used solely for populating the list of tables
pub struct BasicMetadata {
    oid: i64,
    name: String
}

/// Sends a list of tables through the provided channel.
pub fn send_metadata_list(table_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(&trans, 
        "SELECT 
            OID, 
            NAME 
        FROM METADATA_TABLE 
        WHERE TRASH = 0 
        ORDER BY NAME ASC;", [], 
        &mut |row| {
            table_channel.send(BasicMetadata {  
                oid: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}



#[derive(Serialize)]
struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    //data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>
}