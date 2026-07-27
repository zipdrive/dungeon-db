use rusqlite::Error as RusqliteError;
use serde::Serialize;
use tauri::ipc::InvokeError;
use tauri::{Error as TauriError};
use backtrace::Backtrace;

pub enum Error {
    /// An arbitrary error.
    AdhocError {
        msg: String,
        backtrace: Backtrace
    },



    /// Error for when there are two columns with the same name.
    DuplicateColumnName {
        column_name: String,
    },

    /// Error for when a column with type Primitive, Object, Select, or Multiselect does not belong to a table.
    OrphanedDataColumn {
        column_oid: i64,
        column_name: String,
    },

    /// Error for when a datasource cannot be added to a view.
    InvalidDatasource {
        datasource_alias: String,
    },

    /// Error for when a datasource is identified by an improper column.
    InvalidDatasourceColumn {
        column_oid: i64,
        column_name: String,
        column_type: &'static str,
    },

    /// Error for when a virtual column is attempted to be added as a parameter to the view for a schema.
    InvalidParameter {
        column_oid: i64,
        column_name: String,
        column_type: &'static str,
    },



    /// Error for when a formula has invalid syntax.
    FormulaParseError {
        msg: String,
        full_formula: String,
        substring_with_error: String,
    },

    /// Error for when a function in a formula is passed an incorrect type.
    FormulaTypeValidationError {
        outer_name: &'static str,
        inner_name: String,
        expected_type: String,
        received_type: String,
    },

    /// Error for when a parameter in a formula has been deleted.
    FormulaOrphanedParameterError {

    },

    /*
    FormulaTypeConflictError {
        name: String,
        types: Vec<String>
    },
    FormulaTypeCardinalityError {
        name: String,
        types: Vec<String>
    },
     */
    SaveInitializationError(RusqliteError),

    /// An error with an SQL statement.
    SqlError {
        sql: String,
        backtrace: Backtrace,
        err: RusqliteError
    },

    /// An arbitrary error within Rusqlite. Might include connection errors, etc.
    RusqliteError(RusqliteError),

    /// An arbitrary error within Tauri.
    TauriError(TauriError),
}


impl Into<InvokeError> for Error {
    fn into(self) -> InvokeError {
        let as_str: String = self.into();
        return InvokeError(as_str.into());
    }
}

impl From<RusqliteError> for Error {
    fn from(e: RusqliteError) -> Error {
        Error::RusqliteError(e)
    }
}

impl From<TauriError> for Error {
    fn from(e: TauriError) -> Error {
        Error::TauriError(e)
    }
}

impl Into<String> for Error {
    fn into(self) -> String {
        match self {
            Self::AdhocError { msg, backtrace } => {
                return format!("{msg}\n\n{backtrace:#?}");
            }

            Self::DuplicateColumnName { column_name } => {
                return format!("Multiple columns in table with the name \"{}\"!", column_name.replace("\\", "\\\\").replace("\"", "\\\""))
            }

            Self::OrphanedDataColumn { column_oid, column_name } => {
                return format!("Data column \"{}\" (ID {column_oid}) does not belong to a table!", column_name.replace("\\", "\\\\").replace("\"", "\\\""));
            }

            Self::InvalidDatasource { datasource_alias } => {
                return format!("Datasource \"{datasource_alias}\" could not be queried!");
            }

            Self::InvalidDatasourceColumn { column_oid, column_name, column_type } => {
                return format!("The source for a data column cannot be identified by the {column_type} column \"{}\" (ID {column_oid})!", column_name.replace("\\", "\\\\").replace("\"", "\\\""));
            }

            Self::InvalidParameter { column_oid, column_name, column_type } => {
                return format!("{column_type} column \"{}\" (ID {column_oid}) cannot be a parameter!", column_name.replace("\\", "\\\\").replace("\"", "\\\""));
            }
            
            Self::FormulaParseError { msg, full_formula, substring_with_error } => {
                return match full_formula.find(&substring_with_error) {
                    Some(idx) => format!(
                        "{msg}\nAt char {idx} (\"{}{}\"): {full_formula}", 
                        if idx > 0 {
                            ".."
                        } else {
                            ""
                        },
                        if substring_with_error.len() < 22 {
                            substring_with_error
                        } else {
                            let substring_with_error_slice: String = substring_with_error[0..20].to_string();
                            format!("{}..", substring_with_error_slice)
                        }
                    ),
                    None => format!("{msg}\n{full_formula}")
                };
            }
            Self::FormulaTypeValidationError { outer_name, inner_name, expected_type, received_type } => {
                return format!("Formula error occurred: {outer_name} expected a value of type {expected_type}, but {inner_name} returned a value of type {received_type}.")
            }
            Self::SaveInitializationError(e) => {
                return format!("An SQLite error occurred while attempting to save the state of the database: {}", e);
            }

            Self::SqlError { sql, backtrace, err } => {
                return format!("An error occurred while executing SQL expression:\n{sql}\n\n{err}\n\n{backtrace:#?}");
            }

            Self::RusqliteError(e) => {
                return format!("SQLite error occurred: {}", e);
            }
            Self::TauriError(e) => {
                return format!("Tauri error occurred: {}", e);
            }
        }
    }
}

impl Error {
    /// Constructs a new ad-hoc error.
    pub fn adhoc<S>(msg: S) -> Self where S : AsRef<str> {
        Self::AdhocError { 
            msg: String::from(msg.as_ref()), 
            backtrace: Backtrace::new_unresolved() 
        }
    }
}



#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
/// A flag for a validation check that was not passed.
pub struct FailedValidation {
    pub description: String,
}
