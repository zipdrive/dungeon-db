use std::collections::{HashMap, HashSet, LinkedList};
use serde_json::{Result as SerdeJsonResult, Value};
use rusqlite::{Error as RusqliteError, OptionalExtension, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{column, column_type, db, table};
use crate::util::error;


#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
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
    },
    ReadOnlyValue {
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

/// Sends all cells for the table through a channel.
pub fn send_table_data(table_oid: i64, page_num: i64, page_size: i64, cell_channel: Channel<Cell>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, false)?;
    
    println!("{table_select_cmd}");

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