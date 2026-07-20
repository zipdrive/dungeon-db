use crate::data::{column, column_type, datasource, query, schema, table};
use crate::data::{datasource::Datasource, file, row};
use crate::util::channel::Sender;
use crate::util::db;
use crate::util::error::Error;
use base64::{prelude::BASE64_STANDARD as base64standard, Engine};
use regex::Regex;
use rusqlite::vtab::array::Array;
use rusqlite::{ffi::FTS5_TOKENIZE_QUERY, types::Value, AndThenRows, OptionalExtension};
use rusqlite::{params, Connection, Params, Transaction};
use serde::{de::value, Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::str::FromStr;
use std::{cell, collections::HashSet};
use tauri::{AppHandle, Emitter};

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FailedValidation {
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RetrievalLimit {
    Page { num: i64, size: i64 },
    SingleRow,
    None,
}

impl RetrievalLimit {
    /// Retrieves the LIMIT of the query.
    pub fn get_size(&self) -> i64 {
        match self {
            Self::Page { size, .. } => size.clone(),
            Self::SingleRow => 1,
            Self::None => i64::MAX,
        }
    }
}

/// A dependency that may affect the value of a cell.
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CellDependency {
    table_oid: i64,
    column_oid: i64,
    row_oid: i64,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum CellIdentifier {
    /// A reference to a cell in a table.
    /// Updates from the backend can be pushed directly to the frontend, and vice versa.
    /// The cell only needs to be updated when the data in the table is updated.
    DataCell {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
    },

    /// A virtual cell.
    VirtualCell {
        /// The OID used to identify the cell's column.
        column_oid: i64,

        /// The query filter used to identify the cell's row.
        query_filter: String,

        /// The list of dependencies that always have a 1-to-1 relationship with this cell.
        /// Whenever one of these dependencies is updated, only this cell needs to be updated.
        isolated_cell_dependencies: Vec<CellDependency>,

        /// The list of dependencies that have a *-to-1 relationship with this cell.
        /// Whenever one of these dependencies is updated, the entire schema needs to be reloaded.
        full_reload_cell_dependencies: Vec<CellDependency>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum CellTextFormat {
    Plain,
    Markdown,
    Json,
    Xml
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Cell {
    /// Virtual cell. Represented by a readonly label that cannot be edited.
    Readonly {
        cell_identifier: CellIdentifier,
        label: Option<String>,
        format: CellTextFormat,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a text entry field.
    TextEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        format: CellTextFormat,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a text entry field, where the entered value is restricted to an integer.
    IntegerEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        value: Option<i64>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a text entry field, where the entered value is restricted to a number.
    NumberEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        value: Option<f64>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a text entry field, where the entered value is restricted to a date.
    DateEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a text entry field, where the entered value is restricted to a datetime.
    DatetimeEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a checkbox.
    CheckboxEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        is_checked: bool,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a filename, with buttons to upload or download.
    FileEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        file_oid: Option<i64>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by an image, where clicking on the image brings up a dialog to open a new image to replace it.
    ImageEntry {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        file: Option<file::File>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Virtual cell. Represented by a link to open a schema window that filters a report based on the current row.
    SchemaLink {
        cell_identifier: CellIdentifier,
        label: Option<String>,
        link_schema_oid: i64,
        link_query_filter: String,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a link to open an object window.
    ObjectLink {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        link_schema_oid: i64,
        link_row_oid: Option<i64>,
        link_query_filter: Option<String>,
        clipboard_data: Option<(i64, Vec<DataCellEntry>)>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a dropdown, from which a single value can be selected.
    SingleSelectDropdown {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        dropdown_table_oid: i64,
        dropdown_row_oid: Option<i64>,
        validation_failures: Vec<FailedValidation>,
    },

    /// Data cell. Represented by a dropdown, from which multiple values can be selected.
    MultiSelectDropdown {
        cell_identifier: CellIdentifier,
        data_table_oid: i64,
        data_column_oid: i64,
        data_row_oid: i64,
        label: Option<String>,
        dropdown_table_oid: i64,
        dropdown_row_oid: Vec<i64>,
        validation_failures: Vec<FailedValidation>,
    },
}

impl Cell {
    /// Retrieve a particular cell.
    pub fn get(cell_identifier: CellIdentifier) -> Self {
        let conn = match db::open() {
            Ok(conn) => conn,
            Err(e) => {
                return Self::Readonly {
                    cell_identifier,
                    label: None,
                    format: CellTextFormat::Plain,
                    validation_failures: vec![FailedValidation {
                        message: format!(
                            "SQLite error occurred when connecting to database file: {}",
                            <Error as Into<String>>::into(e)
                        ),
                    }],
                };
            }
        };
        Self::get_transact(&conn, cell_identifier)
    }

    /// Retrieve a particular cell.
    pub fn get_transact(conn: &Connection, cell_identifier: CellIdentifier) -> Self {
        match cell_identifier {
            CellIdentifier::DataCell {
                table_oid,
                column_oid,
                row_oid,
            } => {
                // Get the column metadata
                let column_metadata: column::FullMetadata =
                    match column::FullMetadata::get_transact(&conn, column_oid.clone()) {
                        Ok(column_metadata) => column_metadata,
                        Err(e) => {
                            return Self::Readonly {
                                cell_identifier,
                                label: None,
                                format: CellTextFormat::Plain,
                                validation_failures: vec![FailedValidation {
                                    message: format!(
                                        "Error while retrieving column metadata: {}",
                                        <Error as Into<String>>::into(e)
                                    ),
                                }],
                            };
                        }
                    };

                match column_metadata.column_type {
                    column_type::ColumnType::Primitive(prim) => match prim {
                        column_type::Primitive::Boolean => {
                            let is_checked_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS IS_CHECKED FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (is_checked, is_checked_e) =
                                match conn.query_one(&is_checked_sql, params![row_oid], |row| {
                                    row.get::<_, Option<bool>>("IS_CHECKED")
                                }) {
                                    Ok(is_checked) => (is_checked, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::CheckboxEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                is_checked: if let Some(is_checked) = is_checked {
                                    is_checked
                                } else {
                                    false
                                },
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(is_checked_e) = is_checked_e {
                                        vec![FailedValidation {
                                            message: format!("{is_checked_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::Integer => {
                            let value_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS VALUE FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (value, value_e) =
                                match conn.query_one(&value_sql, params![row_oid], |row| {
                                    row.get::<_, Option<i64>>("VALUE")
                                }) {
                                    Ok(value) => (value, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::IntegerEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                value,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(value_e) = value_e {
                                        vec![FailedValidation {
                                            message: format!("{value_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::Number => {
                            let value_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS VALUE FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (value, value_e) =
                                match conn.query_one(&value_sql, params![row_oid], |row| {
                                    row.get::<_, Option<f64>>("VALUE")
                                }) {
                                    Ok(value) => (value, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::NumberEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                value,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(value_e) = value_e {
                                        vec![FailedValidation {
                                            message: format!("{value_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::PlainText 
                        | column_type::Primitive::MarkdownText
                        | column_type::Primitive::JsonText
                        | column_type::Primitive::XmlText => {
                            let label_sql: String = format!("SELECT COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (label, label_e) =
                                match conn.query_one(&label_sql, params![row_oid], |row| {
                                    row.get::<_, Option<String>>("LABEL")
                                }) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::TextEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                label,
                                format: match prim {
                                    column_type::Primitive::JsonText => CellTextFormat::Json,
                                    _ => CellTextFormat::Plain,
                                },
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(label_e) = label_e {
                                        vec![FailedValidation {
                                            message: format!("{label_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::Date => {
                            let label_sql: String = format!("SELECT COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (label, label_e) =
                                match conn.query_one(&label_sql, params![row_oid], |row| {
                                    row.get::<_, Option<String>>("LABEL")
                                }) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::DateEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                label,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(label_e) = label_e {
                                        vec![FailedValidation {
                                            message: format!("{label_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::Datetime => {
                            let label_sql: String = format!("SELECT COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (label, label_e) =
                                match conn.query_one(&label_sql, params![row_oid], |row| {
                                    row.get::<_, Option<String>>("LABEL")
                                }) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e)),
                                };

                            Self::DatetimeEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                label,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(label_e) = label_e {
                                        vec![FailedValidation {
                                            message: format!("{label_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::File => {
                            let label_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS FILE_OID, COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (file_oid, label, label_e) =
                                match conn.query_one(&label_sql, params![row_oid], |row| {
                                    Ok((
                                        row.get::<_, Option<i64>>("FILE_OID")?,
                                        row.get::<_, Option<String>>("LABEL")?,
                                    ))
                                }) {
                                    Ok((file_oid, label)) => (file_oid, label, None),
                                    Err(e) => (None, None, Some(e)),
                                };

                            Self::FileEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                file_oid,
                                label,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    if let Some(label_e) = label_e {
                                        vec![FailedValidation {
                                            message: format!("{label_e}"),
                                        }]
                                    } else {
                                        Vec::new()
                                    }
                                },
                            }
                        }
                        column_type::Primitive::Image => {
                            let file_oid_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS FILE_OID, COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                            let (file_oid, label, file_oid_e) =
                                match conn.query_one(&file_oid_sql, params![row_oid], |row| {
                                    Ok((
                                        row.get::<_, Option<i64>>("FILE_OID")?,
                                        row.get::<_, Option<String>>("LABEL")?,
                                    ))
                                }) {
                                    Ok((file_oid, label)) => (file_oid, label, None),
                                    Err(e) => (None::<i64>, None::<String>, Some(e)),
                                };
                            let (file, file_e) = if let Some(file_oid) = file_oid {
                                match file::File::get_transact(conn, file_oid) {
                                    Ok(file) => (Some(file), None),
                                    Err(e) => (None, Some(e)),
                                }
                            } else {
                                (None, None)
                            };

                            Self::ImageEntry {
                                data_table_oid: table_oid.clone(),
                                data_column_oid: column_oid.clone(),
                                data_row_oid: row_oid.clone(),
                                file,
                                label,
                                cell_identifier: CellIdentifier::DataCell {
                                    table_oid,
                                    column_oid,
                                    row_oid,
                                },
                                validation_failures: {
                                    let mut failures: Vec<FailedValidation> =
                                        if let Some(file_oid_e) = file_oid_e {
                                            vec![FailedValidation {
                                                message: format!("{file_oid_e}"),
                                            }]
                                        } else {
                                            Vec::new()
                                        };
                                    if let Some(file_src_e) = file_e {
                                        failures.push(FailedValidation {
                                            message: format!(
                                                "Error while getting image src: {}",
                                                <Error as Into<String>>::into(file_src_e)
                                            ),
                                        });
                                    }
                                    failures
                                },
                            }
                        }
                    },
                    column_type::ColumnType::Object {
                        table_oid: link_schema_oid,
                        ..
                    } => {
                        let label_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS VALUE, COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                        let (link_row_oid, label, label_e) =
                            match conn.query_one(&label_sql, params![row_oid], |row| {
                                Ok((
                                    row.get::<_, Option<i64>>("VALUE")?,
                                    row.get::<_, Option<String>>("LABEL")?,
                                ))
                            }) {
                                Ok((link_row_oid, label)) => (link_row_oid, label, None),
                                Err(e) => (None, None, Some(e)),
                            };

                        let (clipboard_data, clipboard_data_e) =
                            if let Some(link_row_oid) = link_row_oid {
                                match DataCellEntry::get_object_data(
                                    link_schema_oid.clone(),
                                    link_row_oid.clone(),
                                ) {
                                    Ok(clipboard_data) => (Some(clipboard_data), None),
                                    Err(e) => (None, Some(e)),
                                }
                            } else {
                                (None, None)
                            };

                        Self::ObjectLink {
                            data_table_oid: table_oid.clone(),
                            data_column_oid: column_oid.clone(),
                            data_row_oid: row_oid.clone(),
                            label,
                            link_schema_oid,
                            link_query_filter: match link_row_oid {
                                Some(link_row_oid) => Some(format!("OID={link_row_oid}")),
                                None => None,
                            },
                            link_row_oid,
                            clipboard_data,
                            cell_identifier: CellIdentifier::DataCell {
                                table_oid,
                                column_oid,
                                row_oid,
                            },
                            validation_failures: {
                                if let Some(label_e) = label_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}"),
                                    }]
                                } else {
                                    Vec::new()
                                }
                            },
                        }
                    }
                    column_type::ColumnType::Select {
                        table_oid: dropdown_table_oid,
                        ..
                    } => {
                        let label_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS VALUE, COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                        let (label, dropdown_row_oid, dropdown_row_oid_e) =
                            match conn.query_one(&label_sql, params![row_oid], |row| {
                                Ok((
                                    row.get::<_, Option<String>>("LABEL")?,
                                    row.get::<_, Option<i64>>("VALUE")?,
                                ))
                            }) {
                                Ok((label, dropdown_row_oid)) => (label, dropdown_row_oid, None),
                                Err(e) => (None, None, Some(e)),
                            };

                        Self::SingleSelectDropdown {
                            data_table_oid: table_oid.clone(),
                            data_column_oid: column_oid.clone(),
                            data_row_oid: row_oid.clone(),
                            label,
                            dropdown_table_oid,
                            dropdown_row_oid,
                            cell_identifier: CellIdentifier::DataCell {
                                table_oid,
                                column_oid,
                                row_oid,
                            },
                            validation_failures: {
                                if let Some(label_e) = dropdown_row_oid_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}"),
                                    }]
                                } else {
                                    Vec::new()
                                }
                            },
                        }
                    }
                    column_type::ColumnType::Multiselect {
                        table_oid: dropdown_table_oid,
                        ..
                    } => {
                        let value_sql: String = format!("SELECT COLUMN{column_oid}_VALUE AS VALUE, COLUMN{column_oid}_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
                        let (value, label, label_e) =
                            match conn.query_one(&value_sql, params![row_oid], |row| {
                                Ok((
                                    row.get::<_, Option<String>>("VALUE")?,
                                    row.get::<_, Option<String>>("LABEL")?,
                                ))
                            }) {
                                Ok((value, label)) => (value, label, None),
                                Err(e) => (None, None, Some(e)),
                            };
                        let dropdown_row_oid: Vec<i64> = if let Some(value) = value {
                            value
                                .split(',')
                                .filter_map(|s| match i64::from_str_radix(s, 10) {
                                    Ok(i) => Some(i),
                                    Err(_) => None,
                                })
                                .collect()
                        } else {
                            Vec::new()
                        };

                        Self::MultiSelectDropdown {
                            data_table_oid: table_oid.clone(),
                            data_column_oid: column_oid.clone(),
                            data_row_oid: row_oid.clone(),
                            label,
                            dropdown_table_oid,
                            dropdown_row_oid,
                            cell_identifier: CellIdentifier::DataCell {
                                table_oid,
                                column_oid,
                                row_oid,
                            },
                            validation_failures: {
                                if let Some(label_e) = label_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}"),
                                    }]
                                } else {
                                    Vec::new()
                                }
                            },
                        }
                    }
                    _ => {
                        return Self::Readonly {
                            cell_identifier: CellIdentifier::DataCell {
                                table_oid,
                                column_oid,
                                row_oid,
                            },
                            label: None,
                            format: CellTextFormat::Plain,
                            validation_failures: vec![FailedValidation {
                                message: format!(
                                    "A data cell is not expected to belong to a {} column!",
                                    column_metadata.column_type.to_str()
                                ),
                            }],
                        };
                    }
                }
            }
            CellIdentifier::VirtualCell {
                column_oid,
                query_filter,
                isolated_cell_dependencies,
                full_reload_cell_dependencies,
            } => {
                // Get the column metadata
                let column_metadata: column::FullMetadata =
                    match column::FullMetadata::get_transact(&conn, column_oid.clone()) {
                        Ok(column_metadata) => column_metadata,
                        Err(e) => {
                            return Self::Readonly {
                                label: None,
                                format: CellTextFormat::Plain,
                                validation_failures: vec![FailedValidation {
                                    message: format!(
                                        "Error while retrieving column metadata: {}",
                                        <Error as Into<String>>::into(e)
                                    ),
                                }],
                                cell_identifier: CellIdentifier::VirtualCell {
                                    column_oid,
                                    query_filter,
                                    isolated_cell_dependencies,
                                    full_reload_cell_dependencies,
                                },
                            };
                        }
                    };

                match column_metadata.column_type {
                    column_type::ColumnType::Formula { .. } => {
                        let label_sql: String = format!(
                            "
                            SELECT 
                                COLUMN{column_oid}_PARAM AS PARAM, 
                                COLUMN{column_oid}_VALUE AS VALUE, 
                                COLUMN{column_oid}_LABEL AS LABEL 
                            FROM SCHEMA{}_VIEW 
                            {}
                            ",
                            column_metadata.schema.oid,
                            {
                                let schema_view_def: String = match conn.query_one(
                                    "SELECT sql FROM sqlite_schema WHERE tbl_name = ?1",
                                    params![format!("SCHEMA{}_VIEW", column_metadata.schema.oid)],
                                    |row| row.get("sql"),
                                ) {
                                    Ok(schema_view_def) => schema_view_def,
                                    Err(_) => String::from(""),
                                };
                                let filters: Vec<String> = query_filter
                                    .split('&')
                                    .filter_map(|s| {
                                        if let Some((filter_column_name, filter_column_value)) =
                                            s.split_once('=')
                                        {
                                            let pattern: String =
                                                format!(" AS {filter_column_name}");
                                            if schema_view_def.contains(&pattern) {
                                                Some(format!(
                                                    "{filter_column_name} = {filter_column_value}"
                                                ))
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                if filters.len() == 0 {
                                    String::from("")
                                } else {
                                    format!(
                                        "WHERE {}",
                                        filters
                                            .into_iter()
                                            .reduce(|acc, e| format!("{acc} AND {e}"))
                                            .unwrap()
                                    )
                                }
                            }
                        );
                        let (param, value, label, label_e) =
                            match conn.query_one(&label_sql, params![], |row| {
                                Ok((
                                    row.get::<_, Option<String>>("PARAM")?,
                                    row.get::<_, Option<String>>("VALUE")?,
                                    row.get::<_, Option<String>>("LABEL")?,
                                ))
                            }) {
                                Ok((param, value, label)) => (param, value, label, None),
                                Err(e) => (None, None, None, Some(e)),
                            };

                        // Check if the parameter points to a data cell
                        if let Some(param) = param {
                            let param_regex = Regex::new(r"[^:]*:(\d+):(\d+):(\d+)").unwrap();
                            if let Some(param_captures) = param_regex.captures(&param) {
                                // Extract the table, column, and row of the data cell
                                let data_table_oid: i64 =
                                    param_captures
                                        .get(0)
                                        .map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                            Ok(i) => i,
                                            Err(_) => 0,
                                        });
                                let data_column_oid: i64 =
                                    param_captures
                                        .get(1)
                                        .map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                            Ok(i) => i,
                                            Err(_) => 0,
                                        });
                                let data_row_oid: i64 =
                                    param_captures
                                        .get(2)
                                        .map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                            Ok(i) => i,
                                            Err(_) => 0,
                                        });

                                // Retrieve the metadata of the data cell's column
                                let data_column_metadata: column::FullMetadata =
                                    match column::FullMetadata::get_transact(conn, data_column_oid)
                                    {
                                        Ok(data_column_metadata) => data_column_metadata,
                                        Err(e) => {
                                            return Self::Readonly {  
                                            label: None, 
                                            format: CellTextFormat::Plain,
                                            validation_failures: vec![FailedValidation {
                                                message: format!("Error while retrieving metadata of referenced column: {}", <Error as Into<String>>::into(e))
                                            }],
                                            cell_identifier: CellIdentifier::VirtualCell { column_oid, query_filter, isolated_cell_dependencies, full_reload_cell_dependencies },
                                        };
                                        }
                                    };

                                // Return the data cell referenced by the formula
                                return match data_column_metadata.column_type {
                                    column_type::ColumnType::Primitive(prim) => match prim {
                                        column_type::Primitive::Boolean => {
                                            let (is_checked, is_checked_e) =
                                                if let Some(value) = value {
                                                    match i64::from_str_radix(&value, 10) {
                                                        Ok(i) => (i != 0, None),
                                                        Err(e) => (false, Some(e)),
                                                    }
                                                } else {
                                                    (false, None)
                                                };

                                            Self::CheckboxEntry {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                is_checked,
                                                cell_identifier: CellIdentifier::VirtualCell {
                                                    column_oid,
                                                    query_filter,
                                                    isolated_cell_dependencies,
                                                    full_reload_cell_dependencies,
                                                },
                                                validation_failures: {
                                                    if let Some(is_checked_e) = is_checked_e {
                                                        vec![FailedValidation {
                                                            message: format!("{is_checked_e}"),
                                                        }]
                                                    } else {
                                                        Vec::new()
                                                    }
                                                },
                                            }
                                        }
                                        column_type::Primitive::Integer => Self::IntegerEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            value: if let Some(value) = value {
                                                if let Ok(value) = i64::from_str_radix(&value, 10) {
                                                    Some(value)
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            },
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::Number => Self::NumberEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            value: if let Some(value) = value {
                                                if let Ok(value) = f64::from_str(&value) {
                                                    Some(value)
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            },
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::PlainText => Self::TextEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            format: CellTextFormat::Plain,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::MarkdownText => Self::TextEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            format: CellTextFormat::Markdown,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::JsonText => Self::TextEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            format: CellTextFormat::Json,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::XmlText => Self::TextEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            format: CellTextFormat::Xml,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::Date => Self::DateEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::Datetime => Self::DatetimeEntry {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        },
                                        column_type::Primitive::File => {
                                            let (file_oid, file_oid_e) = if let Some(value) = value
                                            {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            Self::FileEntry {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                file_oid,
                                                label,
                                                cell_identifier: CellIdentifier::VirtualCell {
                                                    column_oid,
                                                    query_filter,
                                                    isolated_cell_dependencies,
                                                    full_reload_cell_dependencies,
                                                },
                                                validation_failures: {
                                                    if let Some(file_oid_e) = file_oid_e {
                                                        vec![FailedValidation {
                                                            message: format!("{file_oid_e}"),
                                                        }]
                                                    } else {
                                                        Vec::new()
                                                    }
                                                },
                                            }
                                        }
                                        column_type::Primitive::Image => {
                                            let (file_oid, file_oid_e) = if let Some(value) = value
                                            {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            let (file, file_e) = if let Some(file_oid) = file_oid {
                                                match file::File::get_transact(conn, file_oid) {
                                                    Ok(file) => (Some(file), None),
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            Self::ImageEntry {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                label,
                                                file,
                                                cell_identifier: CellIdentifier::VirtualCell {
                                                    column_oid,
                                                    query_filter,
                                                    isolated_cell_dependencies,
                                                    full_reload_cell_dependencies,
                                                },
                                                validation_failures: {
                                                    let mut failures: Vec<FailedValidation> =
                                                        if let Some(file_oid_e) = file_oid_e {
                                                            vec![FailedValidation {
                                                                message: format!("{file_oid_e}"),
                                                            }]
                                                        } else {
                                                            Vec::new()
                                                        };
                                                    if let Some(file_src_e) = file_e {
                                                        failures.push(FailedValidation {
                                                            message: format!(
                                                                "Error while getting image src: {}",
                                                                <Error as Into<String>>::into(
                                                                    file_src_e
                                                                )
                                                            ),
                                                        });
                                                    }
                                                    failures
                                                },
                                            }
                                        }
                                    },
                                    column_type::ColumnType::Object {
                                        table_oid: link_schema_oid,
                                        ..
                                    } => {
                                        let (link_row_oid, link_row_oid_e) =
                                            if let Some(value) = value {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                        let (clipboard_data, clipboard_data_e) =
                                            if let Some(link_row_oid) = link_row_oid {
                                                match DataCellEntry::get_object_data(
                                                    link_schema_oid.clone(),
                                                    link_row_oid.clone(),
                                                ) {
                                                    Ok(clipboard_data) => {
                                                        (Some(clipboard_data), None)
                                                    }
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                        Self::ObjectLink {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            link_schema_oid,
                                            link_query_filter: match link_row_oid {
                                                Some(link_row_oid) => {
                                                    Some(format!("OID={link_row_oid}"))
                                                }
                                                None => None,
                                            },
                                            link_row_oid,
                                            clipboard_data,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                let mut failures: Vec<FailedValidation> =
                                                    if let Some(label_e) = label_e {
                                                        vec![FailedValidation {
                                                            message: format!("{label_e}"),
                                                        }]
                                                    } else {
                                                        Vec::new()
                                                    };
                                                if let Some(link_row_oid_e) = link_row_oid_e {
                                                    failures.push(FailedValidation {
                                                        message: format!("{link_row_oid_e}"),
                                                    });
                                                }
                                                failures
                                            },
                                        }
                                    }
                                    column_type::ColumnType::Select {
                                        table_oid: dropdown_table_oid,
                                        ..
                                    } => {
                                        let (dropdown_row_oid, dropdown_row_oid_e) =
                                            if let Some(value) = value {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e)),
                                                }
                                            } else {
                                                (None, None)
                                            };

                                        Self::SingleSelectDropdown {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            dropdown_table_oid,
                                            dropdown_row_oid,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = dropdown_row_oid_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        }
                                    }
                                    column_type::ColumnType::Multiselect {
                                        table_oid: dropdown_table_oid,
                                        ..
                                    } => {
                                        let dropdown_row_oid: Vec<i64> = if let Some(value) = value
                                        {
                                            value
                                                .split(',')
                                                .filter_map(|s| match i64::from_str_radix(s, 10) {
                                                    Ok(i) => Some(i),
                                                    Err(_) => None,
                                                })
                                                .collect()
                                        } else {
                                            Vec::new()
                                        };

                                        Self::MultiSelectDropdown {
                                            data_table_oid,
                                            data_column_oid,
                                            data_row_oid,
                                            label,
                                            dropdown_table_oid,
                                            dropdown_row_oid,
                                            cell_identifier: CellIdentifier::VirtualCell {
                                                column_oid,
                                                query_filter,
                                                isolated_cell_dependencies,
                                                full_reload_cell_dependencies,
                                            },
                                            validation_failures: {
                                                if let Some(label_e) = label_e {
                                                    vec![FailedValidation {
                                                        message: format!("{label_e}"),
                                                    }]
                                                } else {
                                                    Vec::new()
                                                }
                                            },
                                        }
                                    }
                                    _ => {
                                        return Self::Readonly { 
                                            cell_identifier: CellIdentifier::VirtualCell { column_oid, query_filter, isolated_cell_dependencies, full_reload_cell_dependencies },
                                            label: None, 
                                            format: CellTextFormat::Plain,
                                            validation_failures: vec![FailedValidation {
                                                message: format!("A data cell is not expected to belong to a {} column!", data_column_metadata.column_type.to_str())
                                            }]
                                        };
                                    }
                                };
                            } else {
                                // If the parameter does not point to a data cell but has a known type, return a readonly value
                                return Self::Readonly {
                                    label,
                                    format: if param.starts_with("json") {
                                        CellTextFormat::Json
                                    } else {
                                        CellTextFormat::Plain
                                    },
                                    validation_failures: Vec::new(),
                                    cell_identifier: CellIdentifier::VirtualCell {
                                        column_oid,
                                        query_filter,
                                        isolated_cell_dependencies,
                                        full_reload_cell_dependencies,
                                    },
                                };
                            }
                        }

                        // If the parameter does not point to a data cell and has an unknown type, return a readonly value
                        return Self::Readonly {
                            label,
                            format: CellTextFormat::Plain,
                            validation_failures: Vec::new(),
                            cell_identifier: CellIdentifier::VirtualCell {
                                column_oid,
                                query_filter,
                                isolated_cell_dependencies,
                                full_reload_cell_dependencies,
                            },
                        };
                    }
                    column_type::ColumnType::Subreport {
                        report_oid: link_schema_oid,
                        ..
                    } => {
                        return Self::SchemaLink {
                            label: Some(String::from("Subreport")),
                            link_schema_oid,
                            link_query_filter: query_filter.clone(),
                            validation_failures: Vec::new(),
                            cell_identifier: CellIdentifier::VirtualCell {
                                column_oid,
                                query_filter,
                                isolated_cell_dependencies,
                                full_reload_cell_dependencies,
                            },
                        }
                    }
                    _ => {
                        return Self::Readonly {
                            label: None,
                            format: CellTextFormat::Plain,
                            validation_failures: vec![FailedValidation {
                                message: format!(
                                    "{} column cannot be on a report!",
                                    column_metadata.column_type.to_str()
                                ),
                            }],
                            cell_identifier: CellIdentifier::VirtualCell {
                                column_oid,
                                query_filter,
                                isolated_cell_dependencies,
                                full_reload_cell_dependencies,
                            },
                        };
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum SchemaCellStream {
    /// Indicates the total number of rows in the schema, for purposes of pagination.
    MaxIndex(i64),

    /// Indicates the start of a new row in the schema.
    Row {
        index: i64,
        row_identifier: Option<(i64, i64)>,
        query_filter: String,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
        validation_failures: Vec<FailedValidation>,
    },

    /// A button to navigate to the previous page.
    PrevButton,

    /// A button to navigate to the next page.
    NextButton,

    /// A button to add a new row to the schema.
    AddNewRowButton {
        table_oid: i64,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
    },

    /// A cell in the schema.
    Cell(Cell),
}

impl SchemaCellStream {
    /// Sends all cells on a page in a schema.
    pub fn query_by_schema(
        mut column_sender: Sender<column::FullMetadata>,
        mut cell_sender: Sender<Self>,
        schema_oid: i64,
        filters: Vec<(String, i64)>,
        limit: RetrievalLimit,
    ) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Query the columns of the schema
        let root_datasource_alias: Option<String> = match conn
            .query_one(
                "SELECT OID FROM METADATA_DATASOURCE WHERE TABLE_OID = ?1 LIMIT 1",
                params![schema_oid],
                |row| row.get::<_, i64>("OID"),
            )
            .optional()?
        {
            Some(root_datasource_oid) => Some(format!("ROOT{root_datasource_oid}")),
            None => None,
        };
        let mut cols: Vec<(column::FullMetadata, String)> = Vec::new();
        for row_result in conn.prepare("SELECT COLUMN_OID, DATASOURCE_PATH FROM METADATA_SCHEMA_COLUMN_VIEW WHERE IS_REQUIRED AND SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, String>("DATASOURCE_PATH")?)))? {
            let (column_oid, datasource_path) = row_result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;

            // Send the column
            column_sender.send(column_metadata.clone())?;

            // Add to the list of columns
            cols.push((column_metadata, datasource_path));
        }

        // Page-level filter
        let where_expr: String = {
            let schema_view_name: String = format!("SCHEMA{schema_oid}_VIEW");
            let mut where_clauses: Vec<String> = Vec::new();
            for (filter_oid_column, filter_oid_value) in filters {
                if conn.query_one("SELECT (sql LIKE ?1 || ',') AS CONTAINS_COLUMN FROM sqlite_schema WHERE tbl_name = ?2", params![filter_oid_column, &schema_view_name], |row| row.get::<_, bool>("CONTAINS_COLUMN"))? {
                    where_clauses.push(format!("{filter_oid_column} = {filter_oid_value}"));
                }
            }
            if where_clauses.len() > 0 {
                format!(
                    "WHERE {}",
                    where_clauses
                        .into_iter()
                        .reduce(|acc, e| format!("{acc} AND {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            }
        };

        // Row limits
        let limit_expr: String = match limit {
            RetrievalLimit::SingleRow => String::from("LIMIT 1"),
            RetrievalLimit::Page { num, size } => {
                format!("LIMIT {size} OFFSET {}", size * (num - 1))
            }
            RetrievalLimit::None => String::from(""),
        };

        // Send over the MAX index, for purposes of determining page count
        let max_index: i64 = {
            let max_sql: String =
                format!("SELECT COUNT(*) AS MAX_INDEX FROM SCHEMA{schema_oid}_VIEW {where_expr}");
            conn.query_one(&max_sql, [], |row| row.get::<_, Option<i64>>("MAX_INDEX"))
                .optional()?
                .unwrap_or(Some(0))
                .unwrap_or(0)
        };
        cell_sender.send(Self::MaxIndex(max_index.clone()))?;

        // Query the cells of the schema
        let cell_sql: String = format!(
            "SELECT ROW_NUMBER() OVER (ORDER BY ROW_INDEX) AS QUERY_ROW_INDEX, * FROM SCHEMA{schema_oid}_VIEW {where_expr} ORDER BY ROW_INDEX {limit_expr}"            
        );
        let mut stmt_query = conn.prepare(&cell_sql)?;
        let mut rows_query = stmt_query.query([])?;
        loop {
            // Get the next row of the query
            let Some(row) = rows_query.next()? else {
                break;
            };
            let index: i64 = row.get("QUERY_ROW_INDEX")?;

            // Get the query filter for the row
            let query_filter: String = row.get("QUERY_FILTER")?;

            // Send indicator that a new row has started
            cell_sender.send(Self::Row {
                index: index.clone(),
                row_identifier: {
                    if let Some(row_oid) = row.get::<_, Option<i64>>("OID")? {
                        Some((row.get("TABLE_OID")?, row_oid))
                    } else {
                        None
                    }
                },
                query_filter: query_filter.clone(),
                fixed_parent_datasource: None, // TODO get fixed parent datasources
                validation_failures: Vec::new(),
            })?;

            // Iterate over columns of schema
            for (c, datasource_path) in cols.iter() {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                let label_ord: String = format!("COLUMN{}_LABEL", c.oid);

                cell_sender.send(Self::Cell(match &c.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        let data_table_oid: i64 = c.schema.oid.clone();
                        let data_column_oid: i64 = c.oid.clone();
                        let data_row_oid: i64 = if let Some(root_datasource_alias) = &root_datasource_alias {
                            let row_ord: String = format!("{root_datasource_alias}{datasource_path}_OID");
                            match row.get::<&str, i64>(&row_ord) {
                                Ok(row_oid) => row_oid,
                                Err(_) => {
                                    println!("OID with ordinal {row_ord} does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                                    continue;
                                }
                            }
                        } else {
                            println!("Root datasource does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                            continue;
                        };
                        let cell_identifier: CellIdentifier = CellIdentifier::DataCell { 
                            table_oid: c.schema.oid.clone(), 
                            column_oid: data_column_oid.clone(), 
                            row_oid: data_row_oid.clone()
                        };

                        match prim {
                            column_type::Primitive::Boolean => {
                                let (is_checked, is_checked_e) = match row.get::<&str, Option<bool>>(&value_ord) {
                                    Ok(is_checked) => (is_checked, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::CheckboxEntry {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    is_checked: if let Some(is_checked) = is_checked { is_checked } else { false },
                                    cell_identifier,
                                    validation_failures: {
                                        if let Some(is_checked_e) = is_checked_e {
                                            vec![FailedValidation {
                                                message: format!("{is_checked_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        }
                                    }
                                }
                            }
                            column_type::Primitive::Integer => {
                                let (value, value_e) = match row.get::<&str, Option<i64>>(&value_ord) {
                                    Ok(value) => (value, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::IntegerEntry {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    value,
                                    cell_identifier,
                                    validation_failures: {
                                        let mut failures: Vec<FailedValidation> = if let Some(value_e) = value_e {
                                            vec![FailedValidation {
                                                message: format!("{value_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        };
                                        failures
                                    }
                                }
                            }
                            column_type::Primitive::Number => {
                                let (value, value_e) = match row.get::<&str, Option<f64>>(&value_ord) {
                                    Ok(value) => (value, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::NumberEntry {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    value,
                                    cell_identifier,
                                    validation_failures: {
                                        let mut failures: Vec<FailedValidation> = if let Some(value_e) = value_e {
                                            vec![FailedValidation {
                                                message: format!("{value_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        };
                                        failures
                                    }
                                }
                            }
                            column_type::Primitive::PlainText
                            | column_type::Primitive::MarkdownText
                            | column_type::Primitive::JsonText
                            | column_type::Primitive::XmlText => {
                                let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::TextEntry  {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    label,
                                    format: match prim {
                                        column_type::Primitive::JsonText => CellTextFormat::Json,
                                        _ => CellTextFormat::Plain
                                    },
                                    cell_identifier,
                                    validation_failures: {
                                        if let Some(label_e) = label_e {
                                            vec![FailedValidation {
                                                message: format!("{label_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        }
                                    }
                                }
                            }
                            column_type::Primitive::Date => {
                                let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::DateEntry  {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    label,
                                    cell_identifier,
                                    validation_failures: {
                                        if let Some(label_e) = label_e {
                                            vec![FailedValidation {
                                                message: format!("{label_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        }
                                    }
                                }
                            }
                            column_type::Primitive::Datetime => {
                                let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::DatetimeEntry  {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    label,
                                    cell_identifier,
                                    validation_failures: {
                                        if let Some(label_e) = label_e {
                                            vec![FailedValidation {
                                                message: format!("{label_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        }
                                    }
                                }
                            }
                            column_type::Primitive::File => {
                                let (file_oid, file_oid_e) = match row.get::<&str, Option<i64>>(&value_ord) {
                                    Ok(file_oid) => (file_oid, None),
                                    Err(e) => (None, Some(e))
                                };
                                let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e))
                                };

                                Cell::FileEntry {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    file_oid,
                                    label,
                                    cell_identifier,
                                    validation_failures: {
                                        let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                            vec![FailedValidation {
                                                message: format!("{label_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        };
                                        if let Some(file_oid_e) = file_oid_e {
                                            failures.push(FailedValidation {
                                                message: format!("{file_oid_e}")
                                            })
                                        }
                                        failures
                                    }
                                }
                            }
                            column_type::Primitive::Image => {
                                let (file_oid, file_oid_e) = match row.get::<&str, Option<i64>>(&value_ord) {
                                    Ok(file_oid) => (file_oid, None),
                                    Err(e) => (None, Some(e))
                                };
                                let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                                    Ok(label) => (label, None),
                                    Err(e) => (None, Some(e))
                                };
                                let (file, file_e) = if let Some(file_oid) = file_oid {
                                    match file::File::get(file_oid) {
                                        Ok(file) => (Some(file), None),
                                        Err(e) => (None, Some(e))
                                    }
                                } else {
                                    (None, None) 
                                };

                                Cell::ImageEntry {
                                    data_table_oid,
                                    data_column_oid,
                                    data_row_oid,
                                    label,
                                    file,
                                    cell_identifier,
                                    validation_failures: {
                                        let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                            vec![FailedValidation {
                                                message: format!("{label_e}")
                                            }]
                                        } else {
                                            Vec::new()
                                        };
                                        if let Some(file_oid_e) = file_oid_e {
                                            failures.push(FailedValidation {
                                                message: format!("{file_oid_e}")
                                            })
                                        }
                                        if let Some(file_src_e) = file_e {
                                            failures.push(FailedValidation { 
                                                message: format!("Error while getting image src: {}", <Error as Into<String>>::into(file_src_e))
                                            });
                                        }
                                        failures
                                    }
                                }
                            }
                        }
                    }
                    column_type::ColumnType::Object { table_oid: link_schema_oid, .. } => {
                        let data_table_oid: i64 = c.schema.oid.clone();
                        let data_column_oid: i64 = c.oid.clone();
                        let data_row_oid: i64 = if let Some(root_datasource_alias) = &root_datasource_alias {
                            let row_ord: String = format!("{root_datasource_alias}{datasource_path}_OID");
                            match row.get::<&str, i64>(&row_ord) {
                                Ok(row_oid) => row_oid,
                                Err(_) => {
                                    println!("OID with ordinal {row_ord} does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                                    continue;
                                }
                            }
                        } else {
                            println!("Root datasource does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                            continue;
                        };
                        let cell_identifier: CellIdentifier = CellIdentifier::DataCell { 
                            table_oid: c.schema.oid.clone(), 
                            column_oid: data_column_oid.clone(), 
                            row_oid: data_row_oid.clone()
                        };

                        let (link_row_oid, link_row_oid_e) = match row.get::<&str, Option<i64>>(&value_ord) {
                            Ok(link_row_oid) => (link_row_oid, None),
                            Err(e) => (None, Some(e))
                        };
                        let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                            Ok(label) => (label, None),
                            Err(e) => (None, Some(e))
                        };

                        let (clipboard_data, clipboard_data_e) = if let Some(link_row_oid) = link_row_oid {
                            match DataCellEntry::get_object_data(link_schema_oid.clone(), link_row_oid.clone()) {
                                Ok(clipboard_data) => (Some(clipboard_data), None),
                                Err(e) => (None, Some(e))
                            }
                        } else {
                            (None, None)
                        };

                        Cell::ObjectLink {
                            data_table_oid,
                            data_column_oid,
                            data_row_oid,
                            label,
                            link_schema_oid: link_schema_oid.clone(),
                            link_query_filter: match link_row_oid {
                                Some(link_row_oid) => Some(format!("OID={link_row_oid}")),
                                None => None
                            },
                            link_row_oid,
                            clipboard_data,
                            cell_identifier,
                            validation_failures: {
                                let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}")
                                    }]
                                } else {
                                    Vec::new()
                                };
                                if let Some(link_row_oid_e) = link_row_oid_e {
                                    failures.push(FailedValidation {
                                        message: format!("{link_row_oid_e}")
                                    })
                                }
                                failures
                            }
                        }
                    }
                    column_type::ColumnType::Select { table_oid: dropdown_table_oid, .. } => {
                        let data_table_oid: i64 = c.schema.oid.clone();
                        let data_column_oid: i64 = c.oid.clone();
                        let data_row_oid: i64 = if let Some(root_datasource_alias) = &root_datasource_alias {
                            let row_ord: String = format!("{root_datasource_alias}{datasource_path}_OID");
                            match row.get::<&str, i64>(&row_ord) {
                                Ok(row_oid) => row_oid,
                                Err(_) => {
                                    println!("OID with ordinal {row_ord} does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                                    continue;
                                }
                            }
                        } else {
                            println!("Root datasource does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                            continue;
                        };
                        let cell_identifier: CellIdentifier = CellIdentifier::DataCell { 
                            table_oid: c.schema.oid.clone(), 
                            column_oid: data_column_oid.clone(), 
                            row_oid: data_row_oid.clone()
                        };

                        let (dropdown_row_oid, dropdown_row_oid_e) = match row.get::<&str, Option<i64>>(&value_ord) {
                            Ok(dropdown_row_oid) => (dropdown_row_oid, None),
                            Err(e) => (None, Some(e))
                        };
                        let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                            Ok(label) => (label, None),
                            Err(e) => (None, Some(e))
                        };

                        Cell::SingleSelectDropdown {
                            data_table_oid,
                            data_column_oid,
                            data_row_oid,
                            label,
                            dropdown_table_oid: dropdown_table_oid.clone(),
                            dropdown_row_oid,
                            cell_identifier,
                            validation_failures: {
                                let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}")
                                    }]
                                } else {
                                    Vec::new()
                                };
                                if let Some(value_e) = dropdown_row_oid_e {
                                    failures.push(FailedValidation {
                                        message: format!("{value_e}")
                                    })
                                }
                                failures
                            }
                        }
                    }
                    column_type::ColumnType::Multiselect { table_oid: dropdown_table_oid, .. } => {
                        let data_table_oid: i64 = c.schema.oid.clone();
                        let data_column_oid: i64 = c.oid.clone();
                        let data_row_oid: i64 = if let Some(root_datasource_alias) = &root_datasource_alias {
                            let row_ord: String = format!("{root_datasource_alias}{datasource_path}_OID");
                            match row.get::<&str, i64>(&row_ord) {
                                Ok(row_oid) => row_oid,
                                Err(_) => {
                                    println!("OID with ordinal {row_ord} does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                                    continue;
                                }
                            }
                        } else {
                            println!("Root datasource does not exist for row {index}. Skipping column {} ({})...", c.oid, c.column_type.to_str());
                            continue;
                        };
                        let cell_identifier: CellIdentifier = CellIdentifier::DataCell { 
                            table_oid: c.schema.oid.clone(), 
                            column_oid: data_column_oid.clone(), 
                            row_oid: data_row_oid.clone()
                        };

                        let (value, value_e) = match row.get::<&str, Option<String>>(&value_ord) {
                            Ok(value) => (value, None),
                            Err(e) => (None, Some(e))
                        };
                        let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                            Ok(label) => (label, None),
                            Err(e) => (None, Some(e))
                        };
                        let dropdown_row_oid: Vec<i64> = if let Some(value) = value {
                            value.split(',').filter_map(|s| match i64::from_str_radix(s, 10) {
                                Ok(i) => Some(i),
                                Err(_) => None
                            }).collect()
                        } else {
                            Vec::new()
                        };

                        Cell::MultiSelectDropdown {
                            data_table_oid,
                            data_column_oid,
                            data_row_oid,
                            label,
                            dropdown_table_oid: dropdown_table_oid.clone(),
                            dropdown_row_oid,
                            cell_identifier,
                            validation_failures: {
                                let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                    vec![FailedValidation {
                                        message: format!("{label_e}")
                                    }]
                                } else {
                                    Vec::new()
                                };
                                if let Some(value_e) = value_e {
                                    failures.push(FailedValidation {
                                        message: format!("{value_e}")
                                    })
                                }
                                failures
                            }
                        }
                    }
                    column_type::ColumnType::Formula { .. } => {
                        let cell_identifier: CellIdentifier = CellIdentifier::VirtualCell { 
                            column_oid: c.oid.clone(), 
                            query_filter: query_filter.clone(), 
                            isolated_cell_dependencies: Vec::new(), 
                            full_reload_cell_dependencies: Vec::new() 
                        };

                        let param_ord: String = format!("COLUMN{}_PARAM", c.oid);
                        let (param, param_e) = match row.get::<&str, Option<String>>(&param_ord) {
                            Ok(param) => (param, None),
                            Err(e) => (None, Some(e))
                        };
                        let (value, value_e) = match row.get::<&str, Option<String>>(&value_ord) {
                            Ok(value) => (value, None),
                            Err(e) => (None, Some(e))
                        };
                        let (label, label_e) = match row.get::<&str, Option<String>>(&label_ord) {
                            Ok(label) => (label, None),
                            Err(e) => (None, Some(e))
                        };

                        let mut validation_failures: Vec<FailedValidation> = {
                            let mut failures: Vec<FailedValidation> = if let Some(label_e) = label_e {
                                vec![FailedValidation {
                                    message: format!("{label_e}")
                                }]
                            } else {
                                Vec::new()
                            };
                            if let Some(value_e) = value_e {
                                failures.push(FailedValidation {
                                    message: format!("{value_e}")
                                })
                            }
                            if let Some(param_e) = param_e {
                                failures.push(FailedValidation {
                                    message: format!("{param_e}")
                                })
                            }
                            failures
                        };

                        // Check if the parameter points to a data cell
                        if let Some(param) = param {
                            let param_regex = Regex::new(r"[^:]*:(\d+):(\d+):(\d+)").unwrap();
                            if let Some(param_captures) = param_regex.captures(&param) {
                                // Extract the column and row of the data cell
                                let data_table_oid: i64 = param_captures.get(0).map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                    Ok(i) => i,
                                    Err(_) => 0
                                });
                                let data_column_oid: i64 = param_captures.get(1).map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                    Ok(i) => i,
                                    Err(_) => 0
                                });
                                let data_row_oid: i64 = param_captures.get(2).map_or(0, |s| match i64::from_str_radix(s.as_str(), 10) {
                                    Ok(i) => i,
                                    Err(_) => 0
                                });

                                // Retrieve the metadata of the data cell's column
                                match column::FullMetadata::get(data_column_oid) {
                                    Ok(data_column_metadata) => match data_column_metadata.column_type {
                                        column_type::ColumnType::Primitive(prim) => {
                                            match prim {
                                                column_type::Primitive::Boolean => {
                                                    let (is_checked, is_checked_e) = if let Some(value) = value {
                                                        match i64::from_str_radix(&value, 10) {
                                                            Ok(i) => (i != 0, None),
                                                            Err(e) => (false, Some(e))
                                                        }
                                                    } else {
                                                        (false, None)
                                                    };

                                                    Cell::CheckboxEntry {
                                                        data_table_oid,
                                                        data_column_oid,
                                                        data_row_oid,
                                                        is_checked,
                                                        cell_identifier,
                                                        validation_failures: {
                                                            if let Some(is_checked_e) = is_checked_e {
                                                                validation_failures.push(FailedValidation {
                                                                    message: format!("{is_checked_e}")
                                                                })
                                                            }
                                                            validation_failures
                                                        }
                                                    }
                                                }
                                                column_type::Primitive::Integer
                                                | column_type::Primitive::Number
                                                | column_type::Primitive::PlainText
                                                | column_type::Primitive::MarkdownText
                                                | column_type::Primitive::JsonText
                                                | column_type::Primitive::XmlText
                                                | column_type::Primitive::Date
                                                | column_type::Primitive::Datetime => {
                                                    Cell::TextEntry  {
                                                        data_table_oid,
                                                        data_column_oid,
                                                        data_row_oid,
                                                        label,
                                                        format: match prim {
                                                            column_type::Primitive::JsonText => CellTextFormat::Json,
                                                            _ => CellTextFormat::Plain
                                                        },
                                                        cell_identifier,
                                                        validation_failures
                                                    }
                                                }
                                                column_type::Primitive::File => {
                                                    let (file_oid, file_oid_e) = if let Some(value) = value {
                                                        match i64::from_str_radix(&value, 10) {
                                                            Ok(i) => (Some(i), None),
                                                            Err(e) => (None, Some(e))
                                                        }
                                                    } else {
                                                        (None, None)
                                                    };

                                                    Cell::FileEntry {
                                                        data_table_oid,
                                                        data_column_oid,
                                                        data_row_oid,
                                                        file_oid,
                                                        label,
                                                        cell_identifier,
                                                        validation_failures: {
                                                            if let Some(file_oid_e) = file_oid_e {
                                                                validation_failures.push(FailedValidation {
                                                                    message: format!("{file_oid_e}")
                                                                })
                                                            }
                                                            validation_failures
                                                        }
                                                    }
                                                }
                                                column_type::Primitive::Image => {
                                                    let (file_oid, file_oid_e) = if let Some(value) = value {
                                                        match i64::from_str_radix(&value, 10) {
                                                            Ok(i) => (Some(i), None),
                                                            Err(e) => (None, Some(e))
                                                        }
                                                    } else {
                                                        (None, None)
                                                    };

                                                    let (file, file_e) = if let Some(file_oid) = file_oid {
                                                        match file::File::get(file_oid) {
                                                            Ok(file) => (Some(file), None),
                                                            Err(e) => (None, Some(e))
                                                        }
                                                    } else {
                                                        (None, None) 
                                                    };

                                                    Cell::ImageEntry {
                                                        data_table_oid,
                                                        data_column_oid,
                                                        data_row_oid,
                                                        label,
                                                        file,
                                                        cell_identifier,
                                                        validation_failures: {
                                                            if let Some(file_oid_e) = file_oid_e {
                                                                validation_failures.push(FailedValidation {
                                                                    message: format!("{file_oid_e}")
                                                                });
                                                            }
                                                            if let Some(file_src_e) = file_e {
                                                                validation_failures.push(FailedValidation { 
                                                                    message: format!("Error while getting image src: {}", <Error as Into<String>>::into(file_src_e))
                                                                });
                                                            }
                                                            validation_failures
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        column_type::ColumnType::Object { table_oid: link_schema_oid, .. } => {
                                            let (link_row_oid, link_row_oid_e) = if let Some(value) = value {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e))
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            let (clipboard_data, clipboard_data_e) = if let Some(link_row_oid) = link_row_oid {
                                                match DataCellEntry::get_object_data(link_schema_oid.clone(), link_row_oid.clone()) {
                                                    Ok(clipboard_data) => (Some(clipboard_data), None),
                                                    Err(e) => (None, Some(e))
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            Cell::ObjectLink {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                label,
                                                link_schema_oid,
                                                link_query_filter: match link_row_oid {
                                                    Some(link_row_oid) => Some(format!("OID={link_row_oid}")),
                                                    None => None
                                                },
                                                clipboard_data,
                                                link_row_oid,
                                                cell_identifier,
                                                validation_failures: {
                                                    if let Some(link_row_oid_e) = link_row_oid_e {
                                                        validation_failures.push(FailedValidation {
                                                            message: format!("{link_row_oid_e}")
                                                        })
                                                    }
                                                    validation_failures
                                                }
                                            }
                                        }
                                        column_type::ColumnType::Select { table_oid: dropdown_table_oid, .. } => {
                                            let (dropdown_row_oid, dropdown_row_oid_e) = if let Some(value) = value {
                                                match i64::from_str_radix(&value, 10) {
                                                    Ok(i) => (Some(i), None),
                                                    Err(e) => (None, Some(e))
                                                }
                                            } else {
                                                (None, None)
                                            };

                                            Cell::SingleSelectDropdown {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                label,
                                                dropdown_table_oid,
                                                dropdown_row_oid,
                                                cell_identifier,
                                                validation_failures: {
                                                    if let Some(dropdown_row_oid_e) = dropdown_row_oid_e {
                                                        validation_failures.push(FailedValidation {
                                                            message: format!("{dropdown_row_oid_e}")
                                                        });
                                                    }
                                                    validation_failures
                                                }
                                            }
                                        }
                                        column_type::ColumnType::Multiselect { table_oid: dropdown_table_oid, .. } => {
                                            let dropdown_row_oid: Vec<i64> = if let Some(value) = value {
                                                value.split(',').filter_map(|s| match i64::from_str_radix(s, 10) {
                                                    Ok(i) => Some(i),
                                                    Err(_) => None
                                                }).collect()
                                            } else {
                                                Vec::new()
                                            };

                                            Cell::MultiSelectDropdown {
                                                data_table_oid,
                                                data_column_oid,
                                                data_row_oid,
                                                label,
                                                dropdown_table_oid,
                                                dropdown_row_oid,
                                                cell_identifier,
                                                validation_failures
                                            }
                                        }
                                        _ => {
                                            Cell::Readonly { 
                                                cell_identifier, 
                                                label: None, 
                                                format: CellTextFormat::Plain,
                                                validation_failures: {
                                                    validation_failures.push(FailedValidation {
                                                        message: format!("A data cell is not expected to belong to a {} column!", data_column_metadata.column_type.to_str())
                                                    });
                                                    validation_failures
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        Cell::Readonly {  
                                            label: None, 
                                            format: CellTextFormat::Plain,
                                            validation_failures: {
                                                validation_failures.push(FailedValidation {
                                                    message: format!("Error while retrieving metadata of referenced column: {}", <Error as Into<String>>::into(e))
                                                });
                                                validation_failures
                                            },
                                            cell_identifier
                                        }
                                    }
                                }
                            } else {
                                // If formula returns a value with known type but not associated with a data cell, send as a readonly value
                                Cell::Readonly {  
                                    label, 
                                    format: if param.starts_with("json") {
                                        CellTextFormat::Json
                                    } else {
                                        CellTextFormat::Plain
                                    },
                                    validation_failures: Vec::new(),
                                    cell_identifier
                                }
                            }
                        } else {
                            // If formula returns a value with unknown type, send as a readonly value
                            Cell::Readonly {  
                                label, 
                                format: CellTextFormat::Plain,
                                validation_failures: Vec::new(),
                                cell_identifier
                            }
                        }
                    }
                    column_type::ColumnType::Subreport { report_oid: link_schema_oid, .. } => {
                        let cell_identifier: CellIdentifier = CellIdentifier::VirtualCell { 
                            column_oid: c.oid.clone(), 
                            query_filter: query_filter.clone(), 
                            isolated_cell_dependencies: Vec::new(), 
                            full_reload_cell_dependencies: Vec::new() 
                        };

                        Cell::SchemaLink { 
                            label: Some(String::from("Subreport")), 
                            link_schema_oid: link_schema_oid.clone(), 
                            link_query_filter: query_filter.clone(), 
                            validation_failures: Vec::new(),
                            cell_identifier
                        }
                    }
                }))?;
            }
        }

        // If it is appropriate to add a new row to some table via this schema, send an "Add New Row" button for it
        let table_name: String = format!("TABLE{schema_oid}");
        if conn.table_exists(Some("main"), &table_name)? {
            // Is a table, so always send Add New Row over at the end if there is room
            cell_sender.send(Self::AddNewRowButton {
                table_oid: schema_oid,
                fixed_parent_datasource: None,
            })?;
        } else {
            // Is a report, so only send Add New Row over at the end if there is a single unfixed datasource
            // TODO read the datasources from the SQL definition
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DataCellObjectBehavior {
    New,
    SetExisting(i64),
    CopyExisting {
        table_oid: i64,
        cells: Vec<DataCellEntry>,
    },
    Delete,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum DataCellValue {
    Text(Option<String>),
    Integer(Option<i64>),
    Number(Option<f64>),
    Date {
        label: Option<String>,
    },
    Datetime {
        label: Option<String>,
    },
    Boolean(Option<bool>),
    File {
        file_oid: Option<i64>,
    },
    Object {
        linked_row_oid: DataCellObjectBehavior,
    },
    Select {
        linked_row_oid: Option<i64>,
    },
    Multiselect {
        linked_row_oid: Vec<i64>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataCellEntry {
    pub table_oid: i64,
    column_oid: i64,
    pub row_oid: i64,
    value: DataCellValue,
}

impl DataCellEntry {
    /// Retrieves all clipboard data from a given Object.
    pub fn get_object_data(table_oid: i64, row_oid: i64) -> Result<(i64, Vec<Self>), Error> {
        let conn = db::open()?;
        Self::get_object_data_transact(&conn, table_oid, row_oid)
    }

    /// Retrieves all clipboard data from a given Object.
    pub fn get_object_data_transact(
        conn: &Connection,
        table_oid: i64,
        row_oid: i64,
    ) -> Result<(i64, Vec<Self>), Error> {
        let select_sql: String =
            format!("SELECT TABLE_OID, ROW_OID FROM TABLE{table_oid}_POLYMORPHISM WHERE OID = ?1");
        let (table_oid, row_oid) = conn.query_one(&select_sql, params![row_oid], |row| {
            Ok((
                row.get::<_, i64>("TABLE_OID")?,
                row.get::<_, i64>("ROW_OID")?,
            ))
        })?;
        Ok((
            table_oid,
            Self::get_row_data_transact(conn, table_oid, row_oid)?,
        ))
    }

    /// Retrieves all clipboard data from a particular row in a table.
    fn get_row_data_transact(
        conn: &Connection,
        table_oid: i64,
        row_oid: i64,
    ) -> Result<Vec<Self>, Error> {
        // Get the columns of the table
        let mut cols: Vec<(column::FullMetadata, String)> = Vec::new();
        for row_result in conn.prepare("SELECT COLUMN_OID, DATASOURCE_PATH FROM METADATA_SCHEMA_COLUMN_VIEW WHERE IS_REQUIRED AND SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, String>("DATASOURCE_PATH")?)))? {
            let (column_oid, datasource_path) = row_result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;

            // Add to the list of columns
            cols.push((column_metadata, datasource_path));
        }

        // Query for root datasource
        let root_datasource_alias: String = if let Some(root_datasource) = Datasource::get_default_datasource_transact(conn, table_oid)? {
            root_datasource.get_alias()
        } else {
            return Err(Error::AdhocError("Table does not have a default datasource!"));
        };

        // Query row from
        let select_sql: String = format!("SELECT * FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
        let mut select_stmt = conn.prepare(&select_sql)?;
        let mut select_rows = select_stmt.query(params![row_oid])?;
        let Some(row) = select_rows.next()? else {
            return Err(Error::RusqliteError(rusqlite::Error::QueryReturnedNoRows));
        };

        // For each column, populate the data
        let mut data: Vec<Self> = Vec::new();
        for (col, datasource_path) in cols {
            let label_ord: String = format!("COLUMN{}_LABEL", col.oid);
            let value_ord: String = format!("COLUMN{}_VALUE", col.oid);
            let row_ord: String = format!("{root_datasource_alias}_{datasource_path}_OID");
            let row_oid: i64 = row.get::<&str, i64>(&row_ord)?;

            data.push(Self {
                value: match col.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::PlainText
                            | column_type::Primitive::MarkdownText
                            | column_type::Primitive::JsonText
                            | column_type::Primitive::XmlText => DataCellValue::Text( 
                                row.get::<&str, _>(&value_ord)?
                            ),
                            column_type::Primitive::Integer => DataCellValue::Integer( 
                                row.get::<&str, _>(&value_ord)?
                            ),
                            column_type::Primitive::Number => DataCellValue::Number(
                                row.get::<&str, _>(&value_ord)?
                            ),
                            column_type::Primitive::Date => DataCellValue::Date { 
                                label: row.get::<&str, _>(&label_ord)?
                            },
                            column_type::Primitive::Datetime => DataCellValue::Datetime { 
                                label: row.get::<&str, _>(&label_ord)?
                            },
                            column_type::Primitive::Boolean => DataCellValue::Boolean(
                                row.get::<&str, _>(&value_ord)?
                            ),
                            column_type::Primitive::File
                            | column_type::Primitive::Image => DataCellValue::File { 
                                file_oid: row.get::<&str, _>(&value_ord)?
                            }
                        }
                    }
                    column_type::ColumnType::Object { table_oid: object_table_oid, .. } => {
                        let (object_subtype_table_oid, object_data) = Self::get_object_data(object_table_oid, row.get::<&str, _>(&value_ord)?)?;
                        DataCellValue::Object { 
                            linked_row_oid: DataCellObjectBehavior::CopyExisting { 
                                table_oid: object_subtype_table_oid, 
                                cells: object_data
                            }
                        }
                    }
                    column_type::ColumnType::Select { .. } => DataCellValue::Select { 
                        linked_row_oid: row.get::<&str, _>(&value_ord)?
                    },
                    column_type::ColumnType::Multiselect { .. } => DataCellValue::Multiselect { 
                        linked_row_oid: {
                            match row.get::<&str, Option<String>>(&value_ord)? {
                                Some(value) => value.split(',').filter_map(|s| match i64::from_str(s) {
                                    Ok(i) => Some(i),
                                    Err(_) => None 
                                }).collect(),
                                None => Vec::new()
                            }
                        }
                    },
                    column_type::ColumnType::Formula { .. } => {
                        let param_ord: String = format!("COLUMN{}_PARAM", col.oid);
                        let object_regex: Regex = Regex::new(r"^object(\d+)").unwrap();

                        match row.get::<&str, Option<String>>(&param_ord)? {
                            Some(param) => {
                                if param.starts_with("text") {
                                    DataCellValue::Text(
                                        row.get::<&str, _>(&value_ord)?
                                    )
                                } else if param.starts_with("integer") {
                                    DataCellValue::Integer(
                                        row.get::<&str, _>(&value_ord)?
                                    )
                                } else if param.starts_with("number") {
                                    DataCellValue::Number(
                                        row.get::<&str, _>(&value_ord)?
                                    )
                                } else if param.starts_with("boolean") {
                                    DataCellValue::Boolean(
                                        row.get::<&str, _>(&value_ord)?
                                    )
                                } else if param.starts_with("datetime") {
                                    DataCellValue::Datetime { 
                                        label: row.get::<&str, _>(&label_ord)?
                                    }
                                } else if param.starts_with("dateonly") {
                                    DataCellValue::Date { 
                                        label: row.get::<&str, _>(&label_ord)?
                                    }
                                } else if param.starts_with("file") {
                                    DataCellValue::File { 
                                        file_oid: row.get::<&str, _>(&value_ord)?
                                    }
                                } else if let Some(object_cap) = object_regex.captures(&param) {
                                    let Ok(object_table_oid) = i64::from_str(object_cap.get(1).map_or("", |m| m.as_str())) else {
                                        return Err(Error::AdhocError("Unable to parse object table OID from formula return type."));
                                    };
                                    let (object_subtype_table_oid, object_data) = Self::get_object_data(object_table_oid, row.get::<&str, _>(&value_ord)?)?;
                                    DataCellValue::Object { 
                                        linked_row_oid: DataCellObjectBehavior::CopyExisting { 
                                            table_oid: object_subtype_table_oid, 
                                            cells: object_data
                                        }
                                    }
                                } else if param.starts_with("select") {
                                    DataCellValue::Select { 
                                        linked_row_oid: row.get::<&str, _>(&value_ord)?
                                    }
                                } else if param.starts_with("multiselect") {
                                    DataCellValue::Multiselect { 
                                        linked_row_oid: {
                                            match row.get::<&str, Option<String>>(&value_ord)? {
                                                Some(value) => value.split(',').filter_map(|s| match i64::from_str(s) {
                                                    Ok(i) => Some(i),
                                                    Err(_) => None 
                                                }).collect(),
                                                None => Vec::new()
                                            }
                                        }
                                    }
                                } else {
                                    // Unknown type, so default to NULL
                                    DataCellValue::Text(None)
                                }
                            }
                            // NULL type, so data is NULL
                            None => DataCellValue::Text(None)
                        }
                    }
                    _ => {
                        continue; // Skip virtual columns with no data
                    }
                },
                table_oid: col.schema.oid,
                column_oid: col.oid,
                row_oid
            });
        }

        // Return the queried data
        Ok(data)
    }

    /// Sets the value of a data cell.
    pub fn set(&self) -> Result<DataCellEntry, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Set the data for the cell transactionally
        let result = self.set_transact(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(result)
    }

    /// Sets the value of a data cell.
    pub fn set_transact(&self, trans: &Transaction) -> Result<DataCellEntry, Error> {
        let old_value: DataCellValue = match &self.value {
            DataCellValue::Text(value) => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<String> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![value, self.row_oid])?;

                // Return the old value
                DataCellValue::Text(old_value)
            }
            DataCellValue::Boolean(value) => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<bool> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![value, self.row_oid])?;

                // Return the old value
                DataCellValue::Boolean(old_value)
            }
            DataCellValue::Integer(value) => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<i64> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![value, self.row_oid])?;

                // Return the old value
                DataCellValue::Integer(old_value)
            }
            DataCellValue::Select {
                linked_row_oid: value,
            } => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<i64> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![value, self.row_oid])?;

                // Return the old value
                DataCellValue::Select {
                    linked_row_oid: old_value,
                }
            }
            DataCellValue::Number(value) => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<f64> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![value, self.row_oid])?;

                // Return the old value
                DataCellValue::Number(old_value)
            }
            DataCellValue::Date { label } => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT DATE(COLUMN{}, 'julianday') AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_label: Option<String> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = JULIANDAY(?1, 'start of day') WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![label, self.row_oid])?;

                // Return the old value
                DataCellValue::Date { label: old_label }
            }
            DataCellValue::Datetime { label } => {
                // Store the old value
                let sql_get: String = format!("SELECT STRFTIME('%FT%TZ', COLUMN{}, 'julianday') AS VALUE FROM TABLE{} WHERE OID = ?1", self.column_oid, self.table_oid);
                let old_label: Option<String> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Update with the new value
                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = JULIANDAY(?1) WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![label, self.row_oid])?;

                // Return the old value
                DataCellValue::Datetime { label: old_label }
            }
            DataCellValue::File { file_oid } => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<i64> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| {
                        row.get::<_, Option<i64>>("VALUE")
                    })?;

                let sql_update: String = format!(
                    "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                    self.table_oid, self.column_oid
                );
                trans.execute(&sql_update, params![file_oid, self.row_oid])?;

                // Return the old value
                DataCellValue::File {
                    file_oid: old_value,
                }
            }
            DataCellValue::Object {
                linked_row_oid: value,
            } => {
                // Store the old value
                let sql_get: String = format!(
                    "SELECT COLUMN{} AS VALUE FROM TABLE{} WHERE OID = ?1",
                    self.column_oid, self.table_oid
                );
                let old_value: Option<i64> =
                    trans.query_one(&sql_get, params![self.row_oid], |row| row.get("VALUE"))?;

                // Get the table OID of the Object column
                let object_table_oid: i64 = trans.query_one("SELECT typ.TABLE_OID FROM METADATA_COLUMN c INNER JOIN METADATA_COLUMN_TYPE__OBJECT typ ON c.TYPE_OID = typ.OID WHERE c.OID = ?1", params![self.column_oid], |row| row.get("TABLE_OID"))?;
                // Check if the old Object value needs to be trashed
                if let Some(old_value) = old_value {
                    // Trash the old Object value
                    let mut completed_table_oid: HashSet<i64> = HashSet::new();
                    row::trash_transact(
                        &trans,
                        object_table_oid,
                        old_value,
                        &mut completed_table_oid,
                    )?;
                }

                match value {
                    DataCellObjectBehavior::New => {
                        // Get the table OID of the Object column
                        let object_table_oid: i64 = trans.query_one("SELECT typ.TABLE_OID FROM METADATA_COLUMN c INNER JOIN METADATA_COLUMN_TYPE__OBJECT typ ON c.TYPE_OID = typ.OID WHERE c.OID = ?1", params![self.column_oid], |row| row.get("TABLE_OID"))?;

                        // Create a new Object row
                        let mut object_master_rows: HashMap<i64, i64> = HashMap::new();
                        let object_row_oid: i64 = row::insert_transact(
                            &trans,
                            object_table_oid,
                            None,
                            &mut object_master_rows,
                        )?;

                        // Overwrite old reference with the newly-created Object row
                        let sql_update: String = format!(
                            "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                            self.table_oid, self.column_oid
                        );
                        trans.execute(&sql_update, params![object_row_oid, self.row_oid])?;
                    }
                    DataCellObjectBehavior::SetExisting(row_oid) => {
                        // Update with the specific row OID indicated
                        let sql_update: String = format!(
                            "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                            self.table_oid, self.column_oid
                        );
                        trans.execute(&sql_update, params![row_oid, self.row_oid])?;

                        // Untrash the specified row
                        let mut completed_table_oid: HashSet<i64> = HashSet::new();
                        row::untrash_transact(
                            &trans,
                            object_table_oid,
                            *row_oid,
                            &mut completed_table_oid,
                        )?;
                    }
                    DataCellObjectBehavior::CopyExisting {
                        table_oid: object_subtype_table_oid,
                        cells,
                    } => {
                        // Create and copy over data to a new Object row
                        let mut object_master_rows: HashMap<i64, i64> = HashMap::new();
                        row::copy_transact(
                            &trans,
                            *object_subtype_table_oid,
                            cells.clone(),
                            &mut object_master_rows,
                        )?;

                        // Update with the OID of that Object row
                        let row_oid: i64 = object_master_rows[&object_table_oid];
                        let sql_update: String = format!(
                            "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                            self.table_oid, self.column_oid
                        );
                        trans.execute(&sql_update, params![row_oid, self.row_oid])?;
                    }
                    DataCellObjectBehavior::Delete => {
                        // Remove any reference to an Object row
                        let sql_update: String = format!(
                            "UPDATE TABLE{} SET COLUMN{} = NULL WHERE OID = ?1",
                            self.table_oid, self.column_oid
                        );
                        trans.execute(&sql_update, params![self.row_oid])?;
                    }
                }

                // Return the old value
                DataCellValue::Object {
                    linked_row_oid: match old_value {
                        Some(old_value) => DataCellObjectBehavior::SetExisting(old_value),
                        None => DataCellObjectBehavior::Delete,
                    },
                }
            }
            DataCellValue::Multiselect { linked_row_oid } => {
                // Get the table OID of the Multiselect column
                let multiselect_table_oid: i64 = trans.query_one("SELECT typ.TABLE_OID FROM METADATA_COLUMN c INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT typ ON c.TYPE_OID = typ.OID WHERE c.OID = ?1", params![self.column_oid], |row| row.get("TABLE_OID"))?;

                // Store the old value
                let sql_get: String = format!("SELECT TABLE{multiselect_table_oid}_OID AS VALUE FROM MULTISELECT{} WHERE TABLE{}_OID = ?1", self.column_oid, self.table_oid);
                let mut old_value: Vec<i64> = Vec::new();
                for row_result in trans
                    .prepare(&sql_get)?
                    .query_and_then(params![self.row_oid], |row| row.get::<_, i64>("VALUE"))?
                {
                    old_value.push(row_result?);
                }

                // Delete the rows selected in the database that were deselected
                let sql_delete: String = format!(
                    "DELETE FROM MULTISELECT{} WHERE TABLE{}_OID = ?1 AND TABLE{multiselect_table_oid}_OID NOT IN rarray(?2)",
                    self.column_oid,
                    self.table_oid
                );
                trans.execute(
                    &sql_delete,
                    params![
                        self.row_oid,
                        Array::new(
                            linked_row_oid
                                .iter()
                                .map(|i| Value::Integer(i.clone()))
                                .collect()
                        )
                    ],
                )?;

                // Insert the selected rows
                let sql_insert: String = format!(
                    "INSERT OR IGNORE INTO MULTISELECT{} (TABLE{}_OID, TABLE{multiselect_table_oid}_OID) VALUES (?1, ?2)",
                    self.column_oid,
                    self.table_oid
                );
                for selected_oid in linked_row_oid.iter() {
                    trans.execute(&sql_insert, params![self.row_oid, selected_oid])?;
                }

                // Return the old value
                DataCellValue::Multiselect {
                    linked_row_oid: old_value,
                }
            }
        };

        Ok(DataCellEntry {
            table_oid: self.table_oid.clone(),
            column_oid: self.column_oid.clone(),
            row_oid: self.row_oid.clone(),
            value: old_value,
        })
    }
}
