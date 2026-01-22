use rusqlite::Error as RusqliteError;
use tauri::ipc::InvokeError;

pub enum Error {
    AdhocError(&'static str),
    RusqliteError(RusqliteError),
}

impl Into<InvokeError> for Error {
    fn into(self) -> InvokeError {
        match self {
            Self::AdhocError(s) => {
                return InvokeError(s.into());
            },
            Self::RusqliteError(e) => {
                return InvokeError(format!("SQLite error occurred: {}", e).into());
            }
        };
    }
}