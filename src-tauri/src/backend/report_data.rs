use crate::backend::{data_type, db, table, table_column};
use crate::util::error;
use rusqlite::{params, Error as RusqliteError, OptionalExtension, Row, Transaction};
use serde::Serialize;
use serde_json::{Result as SerdeJsonResult, Value};
use std::collections::{HashMap, HashSet, LinkedList};
use tauri::ipc::Channel;

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Cell {
    RowStart {
        row_oid: i64,
        row_index: i64,
    },
    ColumnValue {
        column_oid: i64,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    Subreport {
        subreport_oid: i64,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists {
        row_exists: bool,
    },
    ColumnValue {
        column_oid: i64,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    Subreport {
        subreport_oid: i64,
    },
}

enum Column {
    Formula {
        column_oid: i64,
        column_name: String,
        display_ord: String,
        true_ord: Option<String>,
        readonly_ord: String,
    },
    Subreport {
        column_oid: i64,
        column_name: String,
        subreport_oid: i64,
    },
}

struct ReportQuery {
    base_table_oid: i64,
    select_cols_cmd: String,
    select_tbls_cmd: String,
    columns: Vec<Column>,
    param_table_oids: HashSet<i64>,
}

/*

impl ReportQuery {
    fn insert_column(&mut self, col_definition: String) {
        self.select_cols_cmd = format!("{}, {col_definition}", self.select_cols_cmd);
    }

    fn insert_param_table(&mut self, trans: &Transaction, param_oid: i64) -> Result<(), error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.param_table_oids.contains(&param_oid) {
            return Ok(());
        }

        // Then, make sure to add any parameter it is dependent on
        match trans.query_one(
            "SELECT
                r.REFERENCED_THROUGH_PARAMETER_OID,
                c.BASE_TABLE_OID,
                c.REFERENCED_TABLE_OID
            FROM METADATA_RPT_PARAMETER__REFERENCED r
            INNER JOIN (
                SELECT
                    RPT_PARAMETER_OID,
                    TABLE_OID AS BASE_TABLE_OID
                    TYPE_OID AS REFERENCED_TABLE_OID
                FROM METADATA_TABLE_COLUMN
                UNION
                SELECT
                    a.RPT_PARAMETER_OID,
                    b.TABLE_OID AS BASE_TABLE_OID,
                    b.TYPE_OID AS REFERENCED_TABLE_OID
                FROM METADATA_RPT_PARAMETER__REFERENCED a
                INNER JOIN METADATA_TABLE_COLUMN b ON b.OID = a.COLUMN_OID
            ) c ON c.RPT_PARAMETER_OID = r.REFERENCED_THROUGH_PARAMETER_OID
            WHERE r.RPT_PARAMETER_OID = ?1",
            params![param_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("REFERENCED_THROUGH_PARAMETER_OID")?,
                    row.get::<_, i64>("BASE_TABLE_OID")?,
                    row.get::<_, i64>("REFERENCED_TABLE_OID")?
                ))
            }
        ).optional()? {
            Some((parent_param_oid, parent_table_oid, child_table_oid)) => {
                // Make sure the parent parameter is added to the query
                self.insert_param_table(trans, parent_param_oid);

                // Add a join via that parent parameter
                if parent_table_oid == self.base_table_oid {
                    self.insert_table(format!("LEFT JOIN TABLE{child_table_oid} r{param_oid} ON t.COLUMN{} = r{param_oid}.OID"));
                } else {

                }
                self.param_table_oids.insert(param_oid);
            },
            None => {}
        }

        //
        return Ok(());
    }

    fn insert_table(&mut self, tbl_definition: String) {
        self.select_tbls_cmd = format!("{} {tbl_definition}", self.select_tbls_cmd);
    }
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(trans: &Transaction, rpt_oid: i64, include_row_oid_clause: bool, include_parent_row_oid_clause: bool) -> Result<(String, LinkedList<Column>), error::Error> {
    // Determine the table OID of the table that forms the basis for the report
    let (base_table_oid, mut subreport_base_parameter_oid) = trans.query_one(
        "SELECT BASE_TABLE_OID, SUBREPORT_BASE_PARAMETER_OID FROM (
            SELECT
                RPT_OID,
                BASE_TABLE_OID,
                NULL AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT__REPORT

            UNION

            SELECT
                s.RPT_OID,
                c.TABLE_OID AS BASE_TABLE_OID,
                s.RPT_PARAMETER__REFERENCED__OID AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT_COLUMN__SUBREPORT s
            INNER JOIN METADATA_RPT_PARAMETER__REFERENCED p ON p.RPT_PARAMETER_OID = s.RPT_PARAMETER__REFERENCED__OID
            INNER JOIN METADATA_TABLE_COLUMN c ON c.OID = p.COLUMN_OID
        ) WHERE RPT_OID = ?1",
        params![rpt_oid],
        |row| {
            Ok((
                row.get::<_, i64>("BASE_TABLE_OID")?,
                row.get::<_, Option<i64>>("SUBREPORT_BASE_PARAMETER_OID")?
            ))
        }
    )?;

    let mut select_cols_cmd: String = String::from("t.OID");
    let mut select_tbls_cmd: String = format!("FROM TABLE{base_table_oid} t");
    let mut columns = LinkedList::<Column>::new();
    let mut tbl_count: usize = 1;
    let mut param_ref_set: HashSet<i64> = HashSet::new();

    match subreport_base_parameter_oid {
        Some(param_oid) => {

        },
        None => {}
    }

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

     */
