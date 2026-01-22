mod db;
mod table;
use tauri::{AppHandle, WebviewWindowBuilder, WebviewUrl};

use crate::util::error;

#[tauri::command]
/// Initialize a connection to a StaticDB database file.
pub fn init(path: String) -> Result<(), error::Error> {
    return db::init(path);
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub fn dialog__create_table(app: AppHandle) -> Result<(), error::Error> {
    match WebviewWindowBuilder::new(
        &app,
        String::from("createTableWindow"),
        WebviewUrl::App("/src/dialogs/createTable.html".into()),
    ).build() {
        Ok(_) => {
            return Ok(());
        },
        Err(e) => {
            return Err(error::Error::TauriError(e));
        }
    }
}

#[tauri::command]
/// Create a table.
pub fn create_table(name: String) -> Result<(), error::Error> {
    table::Table::create(name)?;
    return Ok(());
}