use rusqlite::Error as RusqliteError;
use tauri::Error as TauriError;
use tauri::ipc::InvokeError;

pub enum Error {
    AdhocError(&'static str),
    RusqliteError(RusqliteError),
    TauriError(TauriError),
}

impl Into<InvokeError> for Error {
    fn into(self) -> InvokeError {
        match self {
            Self::AdhocError(s) => {
                return InvokeError(s.into());
            },
            Self::RusqliteError(e) => {
                return InvokeError(format!("SQLite error occurred: {}", e).into());
            },
            Self::TauriError(e) => {
                return InvokeError(format!("Tauri error occurred: {}", e).into());
            }
        };
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