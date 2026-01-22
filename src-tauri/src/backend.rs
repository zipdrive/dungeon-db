mod db;
use crate::util::error;

#[tauri::command]
pub fn init(path: String) -> Result<(), error::Error> {
    return db::init(path);
}