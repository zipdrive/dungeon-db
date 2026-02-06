use std::collections::{HashMap, HashSet, LinkedList};
use serde_json::{Result as SerdeJsonResult, Value};
use rusqlite::{Error as RusqliteError, OptionalExtension, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::{table_column, data_type, db, table};
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
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    },
    Subreport {
        subreport_oid: i64
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
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    },
    Subreport {
        subreport_oid: i64
    }
}


enum Column {
    Formula {
        column_oid: i64,
        column_name: String,
        display_ord: String,
        true_ord: Option<String>,
        readonly_ord: String
    },
    Subreport {
        column_oid: i64,
        column_name: String,
        subreport_oid: i64
    }
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(trans: &Transaction, rpt_oid: i64, include_row_oid_clause: bool, include_parent_row_oid_clause: bool) -> Result<(String, LinkedList<Column>), error::Error> {
    let base_table_oid: i64 = trans.query_one(
        "SELECT BASE_TABLE_OID FROM (
            SELECT
                RPT_OID,
                BASE_TABLE_OID
            FROM METADATA_RPT__REPORT

            UNION

            SELECT
                s.RPT_OID,
                c.TABLE_OID AS BASE_TABLE_OID
            FROM METADATA_RPT_COLUMN__SUBREPORT s
            INNER JOIN METADATA_RPT_PARAMETER__REFERENCED p ON p.RPT_PARAMETER_OID = s.RPT_PARAMETER__REFERENCED__OID
            INNER JOIN METADATA_TABLE_COLUMN c ON c.OID = p.COLUMN_OID
        ) WHERE RPT_OID = ?1", 
        params![rpt_oid], 
        |row| row.get(0)
    )?;

    let mut select_cols_cmd: String = String::from("t.OID");
    let mut select_tbls_cmd: String = format!("FROM TABLE{base_table_oid} t");
    let mut columns = LinkedList::<Column>::new();
    let mut tbl_count: usize = 1;
    let mut param_ref_set: HashSet<i64> = HashSet::new();

    db::query_iterate(trans,
        "SELECT 
            c.OID,
            c.NAME,
            f.FORMULA,
            s.RPT_OID
        FROM METADATA_RPT_COLUMN c
        LEFT JOIN METADATA_RPT_COLUMN__FORMULA f ON f.RPT_COLUMN_OID = c.OID
        LEFT JOIN METADATA_RPT_COLUMN__SUBREPORT s ON s.RPT_COLUMN_OID = s.OID
        WHERE c.RPT_OID = ?1 AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;",
        params![rpt_oid], 
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let formula_wrapper: Option<String> = row.get("FORMULA")?;
            let subreport_oid_wrapper: Option<i64> = row.get("RPT_OID")?;

            match formula_wrapper {
                Some(formula) => {
                    if subreport_oid_wrapper != None {
                        return Err(error::Error::AdhocError("Invalid database state detected - a report column cannot be both a formula and a subreport."));
                    }

                    // Evaluate the formula in the SQL query
                    // TODO
                },
                None => {
                    match subreport_oid_wrapper {
                        Some(subreport_oid) => {
                            // Register the subreport column details
                            columns.push_back(Column::Subreport { 
                                column_oid, 
                                column_name: row.get("NAME")?, 
                                subreport_oid
                            });
                        },
                        None => {
                            return Err(error::Error::AdhocError("Invalid database state detected - a report must be either a formula or a subreport."));
                        }
                    }
                }
            }

            return Ok(());
        }
    )?;

    // TODO
}