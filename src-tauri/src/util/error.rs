use rusqlite::Error as RusqliteError;
use serde::Serialize;
use tauri::ipc::InvokeError;
use tauri::{ipc::Invoke, Error as TauriError};

pub enum Error {
    AdhocError(&'static str),
    SaveInitializationError(RusqliteError),
    RusqliteError(RusqliteError),
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
            Self::AdhocError(s) => {
                return s.into();
            }
            Self::SaveInitializationError(e) => {
                return format!("An SQLite error occurred while attempting to save the state of the database: {}", e);
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// A flag for a validation check that was not passed.
pub struct FailedValidation {
    pub description: String,
}
