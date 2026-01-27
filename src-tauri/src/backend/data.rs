use std::collections::{HashMap, LinkedList};

use rusqlite::{params, Row, Error as RusqliteError, OptionalExtension};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{db, table};
use crate::util::error;

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum Cell {
    RowStart {
        row_oid: i64
    },
    ColumnValue {
        column_oid: i64,
        display_value: String
    }
}

/// Insert a row into the data such that the OID places it before any existing rows with that OID.
pub fn insert(table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    let action = db::begin_db_action()?;

    // If OID is already in database, shift every row with OID >= row_oid up by 1
    let select_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID = ?1;");
    let existing_row_oid = action.trans.query_one(&select_cmd, params![row_oid], 
        |row| {
            return Ok(row.get::<_, i64>(0)?);
        }
    ).optional()?;

    match existing_row_oid {
        None => {
            // Insert with OID = row_oid
            let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
            action.trans.execute(&insert_cmd, params![row_oid])?;
            let row_oid = action.trans.last_insert_rowid();

            // Return the row_oid
            return Ok(row_oid);
        },
        Some(_) => {
            let existing_prev_row_oid = action.trans.query_one(&select_cmd, params![row_oid - 1], 
                |row| {
                    return Ok(row.get::<_, i64>(0)?);
                }
            ).optional()?;
            
            match existing_prev_row_oid {
                None => {
                    // Insert with OID = row_oid - 1
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    action.trans.execute(&insert_cmd, params![row_oid - 1])?;
                    let row_oid = action.trans.last_insert_rowid();

                    // Return the row_oid
                    return Ok(row_oid);
                },
                Some(_) => {
                    // Increment every OID >= row_oid up by 1 to make room for the new row
                    let select_all_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID >= ?1 ORDER BY OID DESC;");
                    action.query_iterate(&select_all_cmd, params![row_oid], 
                        &mut |row| {
                            let update_cmd = format!("UPDATE TABLE{table_oid} SET OID = OID + 1 WHERE OID = ?1;");
                            action.trans.execute(&update_cmd, params![row.get::<_, i64>(0)?])?;
                            return Ok(());
                        }
                    )?;

                    // Insert the row
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    action.trans.execute(&insert_cmd, params![row_oid])?;
                    let row_oid = action.trans.last_insert_rowid();

                    // Return the row_oid
                    return Ok(row_oid);
                }
            }
        }
    }
}

/// Push a row into the table with a default OID.
pub fn push(table_oid: i64) -> Result<i64, error::Error> {
    let action = db::begin_db_action()?;

    // Insert the row
    let insert_cmd = format!("INSERT INTO TABLE{table_oid} DEFAULT VALUES;");
    action.trans.execute(&insert_cmd, [])?;
    let row_oid = action.trans.last_insert_rowid();

    // Return the row OID
    return Ok(row_oid);
}

pub fn send_table_data(table_oid: i64, cell_channel: Channel<Cell>) -> Result<(), error::Error> {
    let action = db::begin_readonly_db_action()?;

    // Build the SELECT query
    let mut select_cmd_cols: String = String::from("SELECT t.OID");
    let mut select_cmd_tables: String = format!("FROM TABLE{table_oid} t");
    let mut ord: usize = 1;
    let mut table_num: i64 = 1;
    let mut ord_to_column_oid: HashMap<usize, i64> = HashMap::<usize, i64>::new();
    action.query_iterate(
        "SELECT 
            c.OID,
            c.TYPE_OID,
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE TABLE_OID = ?1
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get(0)?;
            let column_type_oid: i64 = row.get(1)?;
            match row.get::<_, i64>(2)? {
                0 => {
                    // Primitive type
                    select_cmd_cols = format!("{select_cmd_cols}, t.COLUMN{column_oid}");
                },
                1 => {
                    // Single-select dropdown (i.e. *-to-1 join with table of values)
                    select_cmd_cols = format!("{select_cmd_cols}, t{table_num}.VALUE");
                    select_cmd_tables = format!("{select_cmd_tables} LEFT JOIN TABLE{column_type_oid} t{table_num} ON t{table_num}.OID = t.COLUMN{column_oid}");
                    table_num += 1;
                },
                2 => {
                    // Multi-select dropdown (i.e. *-to-* join with table of values)
                    select_cmd_cols = format!("{select_cmd_cols}, (SELECT GROUP_CONCAT(b.VALUE) FROM TABLE{column_type_oid}_MULTISELECT b WHERE b.OID = t.OID GROUP BY b.OID) AS COLUMN{column_oid}");
                },
                3 | 4 => {
                    // Reference to row in other table
                    // Pull display value from TABLE0_SURROGATE view
                    select_cmd_cols = format!("{select_cmd_cols}, t{table_num}.DISPLAY_VALUE");
                    select_cmd_tables = format!("{select_cmd_tables} LEFT JOIN TABLE{column_type_oid}_SURROGATE t{table_num} ON t{table_num}.OID = t.COLUMN{column_oid}");
                    table_num += 1;
                },
                5 => {
                    // Child table
                    // Pull display values for items from TABLE0_SURROGATE view
                    select_cmd_cols = format!("{select_cmd_cols}, (SELECT GROUP_CONCAT(b.DISPLAY_VALUE) FROM TABLE{column_type_oid} a INNER JOIN TABLE{column_type_oid}_SURROGATE b ON b.OID = a.OID WHERE a.PARENT_OID = t.OID) AS COLUMN{column_oid}");
                },
                _ => {
                    return Err(error::Error::AdhocError("Unknown column type mode."));
                }
            }

            ord_to_column_oid.insert(ord, column_oid);
            ord += 1;
            return Ok(());
        }
    )?;
    let table_select_cmd = format!("{select_cmd_cols} {select_cmd_tables}");

    // Iterate over the results, sending each cell to the frontend
    action.query_iterate(
        &table_select_cmd, 
        [], 
        &mut |row| {
            // Start by sending the OID, which is the first ordinal
            cell_channel.send(Cell::RowStart { row_oid: row.get(0)? })?;

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for (ord, column_oid) in ord_to_column_oid.clone() {
                cell_channel.send(Cell::ColumnValue { column_oid: column_oid, display_value: row.get(ord)? })?;
            }

            // Conclude the row's iteration
            return Ok(());
        }
    )?;
    return Ok(());
}