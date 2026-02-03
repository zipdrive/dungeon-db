use std::collections::{BinaryHeap, HashMap, HashSet};
use std::i32::MAX;
use std::ops::Index;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{Error as RusqliteError, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{column_type, db, table};
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


/// Builds a query to select columns from a table.
pub fn build_table_query(trans: &Transaction, table_oid: i64) -> Result<String, error::Error> {
    let mut select_cols_cmd: String = String::from("t.OID");
    let mut select_tbls_cmd: String = format!("FROM TABLE{table_oid} t");
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
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1 AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;", 
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let column_type: column_type::MetadataColumnType = column_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?);
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
                            select_col = format!("CAST(t.COLUMN{column_oid} AS TEXT)");
                        },
                        column_type::Primitive::Date => {
                            select_col = format!("DATE(t.COLUMN{column_oid}, 'unixepoch')");
                        },
                        column_type::Primitive::Timestamp => {
                            select_col = format!("STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'unixepoch')");
                        },
                        column_type::Primitive::File => {
                            select_col = format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE 'File' END");
                        },
                        column_type::Primitive::Image => {
                            select_col = format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END");
                        }
                    }
                },
                column_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_col = format!("t{tbl_count}.VALUE");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_col = format!("(SELECT '[' || GROUP_CONCAT(b.VALUE) || ']' FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID)");
                },
                column_type::MetadataColumnType::Reference(referenced_table_oid) 
                | column_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_col = format!("COALESCE(t{tbl_count}.DISPLAY_VALUE, CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '— DELETED —' ELSE NULL END)");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_col = format!("(SELECT '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID)");
                }
            }

            select_cols_cmd = format!("{select_cols_cmd}, {select_col} AS COLUMN{column_oid}");
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
                    // If supposed to sort over a particular column but that column wasn't constructed, throw an error
                    return Err(error::Error::AdhocError(""));
                }
            }
        }
        select_cols_cmd = format!("ROW_NUMBER() OVER ({select_order_cmd}) AS ROW_INDEX, {select_cols_cmd}");
    } else {
        select_order_cmd = String::from("");
        select_cols_cmd = format!("ROW_NUMBER() OVER (ORDER BY t.OID) AS ROW_INDEX, {select_cols_cmd}");
    }

    // Create the new surrogate view
    let select_cmd: String = format!("
        SELECT
            {select_cols_cmd} 
        {select_tbls_cmd}
        WHERE t.TRASH = 0
        {select_order_cmd}"
    );
    return Ok(select_cmd);
}



#[derive(PartialEq, Eq)]
struct TableDependency {
    dependency_depth: i32,
    table_oid: i64 
}

impl PartialOrd for TableDependency {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dependency_depth.partial_cmp(&other.dependency_depth)
    }
}

impl Ord for TableDependency {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dependency_depth.cmp(&other.dependency_depth)
    }
}

/// Update the surrogate view for the table.
pub fn update_surrogate_view(trans: &Transaction, table_oid: i64) -> Result<(), error::Error> {
    // Drop the surrogate view and build up a directed graph of dependencies between the primary keys
    let empty_chain: Vec<i64> = Vec::new();
    let dependencies = drop_surrogate_view(trans, table_oid, &empty_chain)?;

    // Create a priority queue of dependencies
    let mut heap: BinaryHeap<TableDependency> = BinaryHeap::new();
    for (dependent_table_oid, dependent_table_depth) in dependencies {
        heap.push(TableDependency { 
            dependency_depth: dependent_table_depth, 
            table_oid: dependent_table_oid 
        });
    }

    // Recreate the surrogate view, then recreate the surrogate views for each dependent table
    loop {
        match heap.pop() {
            Some(dep) => {
                create_surrogate_view(trans, dep.table_oid)?;
            },
            None => {
                break;
            }
        }
    }
    return Ok(());
}

/// Drops the surrogate view for the specified table, as well as the surrogate views for any table referencing it in its primary key.
fn drop_surrogate_view(trans: &Transaction, table_oid: i64, above_table_oid: &Vec<i64>) -> Result<HashMap<i64, i32>, error::Error> {
    let mut found_dependencies: HashMap<i64, i32> = HashMap::new();
    found_dependencies.insert(table_oid, 0);
    let mut above_table_oid = above_table_oid.clone();
    above_table_oid.push(table_oid);

    // Query to find all tables dependent on the one being dropped
    for dependent_table_oid_result in trans.prepare("SELECT TABLE_OID FROM METADATA_TABLE_COLUMN WHERE TYPE_OID = ?1 AND IS_PRIMARY_KEY = 1")?
        .query_and_then(
            params![table_oid], 
            |row| {
                row.get::<_, i64>("TABLE_OID")
            }
        )? {

        // Drop all the dependent surrogate views
        let dependent_table_oid: i64 = dependent_table_oid_result?;
        if dependent_table_oid != table_oid { // Prevent infinite recursion in case of self-referencing tables
            // Check to make sure no infinite loop of primary keys referencing each other
            match above_table_oid.iter().position(|elem| *elem == dependent_table_oid) {
                Some(_) => {
                    // Terminate recursion, notate that there is a loop
                    return Err(error::Error::AdhocError("There is an infinite loop of primary keys that reference each other!"));
                },
                None => {
                    // Recurse deeper
                    for (found_dependent_table_oid, found_dependent_table_depth) in drop_surrogate_view(&trans, dependent_table_oid, &above_table_oid)? {
                        match found_dependencies.get_mut(&found_dependent_table_oid) {
                            Some(previously_found_dependent_table_maxdepth) => {
                                *previously_found_dependent_table_maxdepth = std::cmp::max(*previously_found_dependent_table_maxdepth, found_dependent_table_depth + 1);
                            },
                            None => {
                                found_dependencies.insert(found_dependent_table_oid, found_dependent_table_depth + 1);
                            }
                        }
                    }
                }
            }
        }
    }

    // Drop the requested surrogate view
    let drop_view_cmd: String = format!("DROP VIEW IF EXISTS TABLE{table_oid}_SURROGATE");
    trans.execute(&drop_view_cmd, [])?;

    // Return an ordered 
    return Ok(found_dependencies);
}

fn create_surrogate_view(trans: &Transaction, table_oid: i64) -> Result<(), error::Error> {
    let mut select_tbls_cmd: String = format!("FROM TABLE{table_oid} t");
    struct PrimaryKey {
        single_expr: String,
        json_expr: String
    }
    let mut select_display_value: Vec<PrimaryKey> = Vec::new(); // The primary key (column name, value, needs to be enclosed in quotes?) tuple
    let mut tbl_count: i64 = 1;

    // Iterate over all columns of the table, building up the table's view
    db::query_iterate(trans, 
        "SELECT
            c.OID,
            c.NAME,
            c.TYPE_OID,
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1 AND c.TRASH = 0 AND c.IS_PRIMARY_KEY = 1
        ORDER BY c.COLUMN_ORDERING;", 
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let column_name: String = row.get("NAME")?;
            let json_column_name: String = match serde_json::to_string(&column_name) {
                Ok(s) => s,
                Err(_) => {
                    return Err(error::Error::AdhocError("Couldn't serialize a String, for some reason."));
                }
            };
            let column_type: column_type::MetadataColumnType = column_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?);
            
            match column_type {
                column_type::MetadataColumnType::Primitive(prim) => {
                    match prim {
                        column_type::Primitive::Boolean => {
                            select_display_value.push(PrimaryKey {
                                single_expr: format!("CASE WHEN t.COLUMN{column_oid} = 1 THEN 'True' ELSE 'False' END"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} = 1 THEN 'true' ELSE 'false' END")
                            });
                        },
                        column_type::Primitive::Text => {
                            select_display_value.push(PrimaryKey {
                                single_expr: format!("t.COLUMN{column_oid}"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || t.COLUMN{column_oid} || '\"' ELSE 'null' END")
                            });
                        },
                        column_type::Primitive::Any 
                        | column_type::Primitive::Integer
                        | column_type::Primitive::Number
                        | column_type::Primitive::JSON => {
                            select_display_value.push(PrimaryKey { 
                                single_expr: format!("CAST(t.COLUMN{column_oid} AS TEXT)"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN CAST(t.COLUMN{column_oid} AS TEXT) ELSE 'null' END")
                            });
                        },
                        column_type::Primitive::Date => {
                            select_display_value.push(PrimaryKey { 
                                single_expr: format!("DATE(t.COLUMN{column_oid}, 'unixepoch')"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || DATE(t.COLUMN{column_oid}, 'unixepoch') || '\"' ELSE 'null' END") 
                            });
                        },
                        column_type::Primitive::Timestamp => {
                            select_display_value.push(PrimaryKey { 
                                single_expr: format!("STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'unixepoch')"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'unixepoch') || '\"' ELSE 'null' END") 
                            });
                        },
                        column_type::Primitive::File 
                        | column_type::Primitive::Image => {
                            select_display_value.push(PrimaryKey {
                                single_expr: format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE '{{}}' END"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '{{}}' ELSE 'null' END")
                            });
                        }
                    }
                },
                column_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("t{tbl_count}.VALUE"),
                        json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || t{tbl_count}.VALUE || '\"' ELSE 'null' END")
                    });
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("(SELECT '[' || GROUP_CONCAT(b.VALUE) || ']' FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID)"),
                        json_expr: format!("'{json_column_name}: ' || COALESCE('[' || (SELECT GROUP_CONCAT(b.VALUE) FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID) || ']', 'null')")
                    });
                },
                column_type::MetadataColumnType::Reference(referenced_table_oid) 
                | column_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("t{tbl_count}.DISPLAY_VALUE"),
                        json_expr: format!("'{json_column_name}: ' || t{tbl_count}.JSON_DISPLAY_VALUE")
                    });
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                },
                column_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("'[' || (SELECT GROUP_CONCAT(a.DISPLAY_VALUE) FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID) || ']'"),
                        json_expr: format!("'{json_column_name}: [' || (SELECT GROUP_CONCAT(a.JSON_DISPLAY_VALUE) FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID) || ']'")
                    });
                }
            }
            return Ok(());
        }
    )?;

    let json_display_value: String = if select_display_value.len() > 0 {
        format!("'{{ ' || {} || ' }}'",
            select_display_value.iter().map(|primary_key| primary_key.json_expr.clone()).collect::<Vec<String>>().join(" || ', ' || ")
        )
    } else {
        String::from("'{}'")
    };
    let standard_display_value: String = if select_display_value.len() > 1 {
        json_display_value.clone()
    } else if select_display_value.len() == 1 {
        select_display_value[0].single_expr.clone()
    } else {
        String::from("'— NO PRIMARY KEY —'")
    };

    // Create the new surrogate view
    let create_view_cmd: String = format!("
        CREATE VIEW TABLE{table_oid}_SURROGATE 
        AS 
        SELECT
            t.OID,
            CASE
                WHEN t.TRASH = 0 THEN {standard_display_value}
                ELSE '— DELETED —'
            END AS DISPLAY_VALUE,
            CASE
                WHEN t.TRASH = 0 THEN {json_display_value}
                ELSE 'null'
            END AS JSON_DISPLAY_VALUE
        {select_tbls_cmd}"
    );
    println!("{}", create_view_cmd);
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

/// Gets metadata for a specified table.
pub fn get_metadata(table_oid: &i64) -> Result<BasicMetadata, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let table_name: String = trans.query_one(
        "SELECT 
            NAME 
        FROM METADATA_TABLE 
        WHERE TRASH = 0 
        WHERE OID = ?1;", 
        params![table_oid], 
        |row| row.get::<_, String>("NAME")
    )?;
    return Ok(BasicMetadata {
        oid: table_oid.clone(),
        name: table_name
    });
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