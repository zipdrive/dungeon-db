use std::collections::{HashMap, HashSet, LinkedList};

use rusqlite::{Error as RusqliteError, OptionalExtension, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{column, column_type, db, table};
use crate::util::error;


#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum Cell {
    RowStart {
        row_oid: i64,
        row_index: i64
    },
    ColumnValue {
        column_oid: i64,
        column_type: column_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists {
        row_exists: bool
    },
    ColumnValue {
        column_oid: i64,
        column_type: column_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

/// Insert a row into the data such that the OID places it before any existing rows with that OID.
pub fn insert(table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // If OID is already in database, shift every row with OID >= row_oid up by 1
    let select_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID = ?1;");
    let existing_row_oid = trans.query_one(&select_cmd, params![row_oid], 
        |row| {
            return Ok(row.get::<_, i64>(0)?);
        }
    ).optional()?;

    match existing_row_oid {
        None => {
            // Insert with OID = row_oid
            let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
            trans.execute(&insert_cmd, params![row_oid])?;
            let row_oid = trans.last_insert_rowid();

            // Return the row_oid
            trans.commit()?;
            return Ok(row_oid);
        },
        Some(_) => {
            let existing_prev_row_oid = trans.query_one(&select_cmd, params![row_oid - 1], 
                |row| {
                    return Ok(row.get::<_, i64>(0)?);
                }
            ).optional()?;
            
            match existing_prev_row_oid {
                None => {
                    // Insert with OID = row_oid - 1
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    trans.execute(&insert_cmd, params![row_oid - 1])?;
                    let row_oid = trans.last_insert_rowid();

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                },
                Some(_) => {
                    // Increment every OID >= row_oid up by 1 to make room for the new row
                    let select_all_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID >= ?1 ORDER BY OID DESC;");
                    db::query_iterate(&trans, &select_all_cmd, params![row_oid], 
                        &mut |row| {
                            let update_cmd = format!("UPDATE TABLE{table_oid} SET OID = OID + 1 WHERE OID = ?1;");
                            trans.execute(&update_cmd, params![row.get::<_, i64>(0)?])?;
                            return Ok(());
                        }
                    )?;

                    // Insert the row
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    trans.execute(&insert_cmd, params![row_oid])?;
                    let row_oid = trans.last_insert_rowid();

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                }
            }
        }
    }
}

/// Push a row into the table with a default OID.
pub fn push(table_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Insert the row
    let insert_cmd = format!("INSERT INTO TABLE{table_oid} DEFAULT VALUES;");
    trans.execute(&insert_cmd, [])?;
    let row_oid = trans.last_insert_rowid();

    // Return the row OID
    trans.commit()?;
    return Ok(row_oid);
}

/// Delete the row with the given OID.
pub fn delete(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Delete the row
    let delete_cmd = format!("DELETE FROM TABLE{table_oid} WHERE OID = ?1;");
    trans.execute(&delete_cmd, params![row_oid])?;

    // Return the row OID
    trans.commit()?;
    return Ok(());
}

/// Attempts to update a value represented by a primitive in a table.
/// This applies to primitive types, single-select dropdown types, reference types, and object types.
/// Returns the previous value of the cell.
pub fn try_update_primitive_value(table_oid: i64, row_oid: i64, column_oid: i64, new_value: Option<String>) -> Result<Option<String>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Retrieve the previous value
    let select_cmd = format!("SELECT CAST(COLUMN{column_oid} AS TEXT) AS PRIOR_VALUE FROM TABLE{table_oid} WHERE OID = ?1;");
    let prev_value: Option<String> = trans.query_one(&select_cmd, params![row_oid],
        |row| { return Ok(row.get::<_, Option<String>>(0)?); })?;

    // Update the value
    let update_cmd = format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2;");
    trans.execute(
        &update_cmd,
        params![new_value, row_oid]
    )?;

    // Return OK
    trans.commit()?;
    println!("Updated value from {prev_value:?} to {new_value:?}");
    return Ok(prev_value);
}


struct Column {
    true_ord: Option<String>,
    display_ord: String,
    column_oid: i64,
    column_name: String,
    column_type: column_type::MetadataColumnType,
    is_nullable: bool,
    invalid_nonunique_oid: HashSet<i64>,
    is_primary_key: bool
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(trans: &Transaction, table_oid: i64, include_row_oid_clause: bool) -> Result<(String, LinkedList<Column>), error::Error> {
    // Build the SELECT query
    let mut select_cmd_cols: String = String::from("SELECT ts.*");
    let select_cmd_tables: String = format!("FROM TABLE{table_oid}_SURROGATE ts INNER JOIN TABLE{table_oid} t ON t.OID = ts.OID");
    let mut columns = LinkedList::<Column>::new();
    db::query_iterate(trans,
        "SELECT 
            c.OID,
            c.TYPE_OID,
            t.MODE,
            c.IS_NULLABLE,
            c.IS_UNIQUE,
            c.IS_PRIMARY_KEY,
            c.NAME
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1 AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get(0)?;
            let column_type: column_type::MetadataColumnType = column_type::MetadataColumnType::from_database(row.get(1)?, row.get(2)?);
            
            let enforce_uniqueness: bool = row.get(4)?;
            let mut invalid_nonunique_oid: HashSet<i64> = HashSet::<i64>::new();

            let display_ord: String = format!("COLUMN{column_oid}");
            let true_ord: Option<String>;
            match column_type {
                column_type::MetadataColumnType::Primitive(_) => {
                    // Primitive type
                    true_ord = Some(display_ord.clone());
                },
                column_type::MetadataColumnType::SingleSelectDropdown(_) 
                | column_type::MetadataColumnType::Reference(_)
                | column_type::MetadataColumnType::ChildObject(_) => {
                    // True column is OID referencing row in other table

                    // Pull true value from main table
                    select_cmd_cols = format!("{select_cmd_cols}, CAST(t.COLUMN{column_oid} AS TEXT) AS _COLUMN{column_oid}");
                    true_ord = Some(format!("_COLUMN{column_oid}"));
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                column_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    // Multi-select dropdown (i.e. *-to-* join with table of values)

                    // Pull true value as comma-separated list of integers from main table
                    select_cmd_cols = format!("{select_cmd_cols}, (SELECT GROUP_CONCAT(CAST(a.VALUE_OID AS TEXT)) AS VALUE FROM TABLE{column_type_oid}_MULTISELECT a WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID) AS _COLUMN{column_oid}");
                    true_ord = Some(format!("_COLUMN{column_oid}"));

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            WITH TABLE_SURROGATE AS (
                                SELECT 
                                    ROW_OID,
                                    GROUP_CONCAT(CAST(VALUE_OID AS TEXT)) AS COLUMN{column_oid}
                                FROM TABLE{column_type_oid}_MULTISELECT 
                                GROUP BY OID
                            )
                            SELECT t.ROW_OID AS OID FROM TABLE_SURROGATE t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE_SURROGATE
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                column_type::MetadataColumnType::ChildTable(_) => {
                    // Child table
                    true_ord = None;

                    // Don't even try to enforce uniqueness
                },
                _ => {
                    return Err(error::Error::AdhocError("Unknown column type mode."));
                }
            }

            // Push the column information
            columns.push_back(Column {
                true_ord: true_ord, 
                display_ord: display_ord,
                column_oid: column_oid,
                column_name: row.get(6)?,
                column_type: column_type,
                is_nullable: row.get(3)?,
                invalid_nonunique_oid: invalid_nonunique_oid,
                is_primary_key: row.get(5)?
            });
            return Ok(());
        }
    )?;
    return Ok((
        format!(
            "{select_cmd_cols} {select_cmd_tables} {}",
            if include_row_oid_clause { "WHERE t.OID = ?1" } else { "LIMIT ?1 OFFSET ?2" }
        ), 
        columns
    ));
}

/// Sends all cells for the table through a channel.
pub fn send_table_data(table_oid: i64, page_num: i64, page_size: i64, cell_channel: Channel<Cell>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, false)?;
    
    // Iterate over the results, sending each cell to the frontend
    db::query_iterate(&trans, 
        &table_select_cmd, 
        params![page_size, page_size * (page_num - 1)], 
        &mut |row| {
            // Start by sending the index and OID, which are the first and second ordinal respectively
            let row_index: i64 = row.get(0)?;
            let row_oid: i64 = row.get(1)?;
            cell_channel.send(Cell::RowStart {
                row_index: row_index,
                row_oid: row_oid 
            })?;

            let invalid_key: bool = false; // TODO

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(Cell::ColumnValue {
                    column_oid: column.column_oid, 
                    column_type: column.column_type.clone(), 
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // Conclude the row's iteration
            return Ok(());
        }
    )?;
    return Ok(());
}

/// Sends all cells for a row in the table through a channel.
pub fn send_table_row(table_oid: i64, row_oid: i64, cell_channel: Channel<RowCell>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, true)?;

    // Query for the specified row
    match trans.query_row_and_then(
        &table_select_cmd, 
        params![row_oid], 
        |row| -> Result<(), error::Error> {
            // Start by sending message that confirms the row exists
            cell_channel.send(RowCell::RowExists { row_exists: true })?;

            let invalid_key = false;

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(RowCell::ColumnValue {
                    column_oid: column.column_oid, 
                    column_type: column.column_type.clone(), 
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // 
            return Ok(());
        }
    ) {
        Err(error::Error::RusqliteError(e)) => {
            match e {
                RusqliteError::QueryReturnedNoRows => {
                    cell_channel.send(RowCell::RowExists { row_exists: false })?;
                    return Ok(());
                },
                _ => {
                    return Err(error::Error::from(e));
                }
            }
        },
        Err(e) => {
            return Err(e);
        }
        Ok(_) => {
            return Ok(());
        }
    }
}