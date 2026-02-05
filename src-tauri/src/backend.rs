mod db;
mod table;
mod column_type;
mod column;
mod table_data;
use std::sync::Mutex;
use serde::{Serialize, Deserialize};
use tauri::menu::{ContextMenu, Menu, MenuItem, MenuBuilder};
use tauri::{AppHandle, WebviewWindowBuilder, WebviewUrl, Emitter, Size, PhysicalSize, Manager};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};
use tauri::ipc::{Channel, InvokeError};
use crate::util::error;

#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Action {
    CreateTable {
        table_name: String 
    },
    DeleteTable {
        table_oid: i64 
    },
    RestoreDeletedTable {
        table_oid: i64
    },
    CreateTableColumn {
        table_oid: i64, 
        column_name: String, 
        column_type: column_type::MetadataColumnType, 
        column_ordering: Option<i64>, 
        column_style: String, 
        is_nullable: bool, 
        is_unique: bool, 
        is_primary_key: bool
    },
    EditTableColumnMetadata {
        table_oid: i64, 
        column_oid: i64,
        column_name: String, 
        column_type: column_type::MetadataColumnType, 
        column_style: String, 
        is_nullable: bool, 
        is_unique: bool, 
        is_primary_key: bool
    },
    RestoreEditedTableColumnMetadata {
        table_oid: i64,
        column_oid: i64,
        prior_metadata_column_oid: i64
    },
    EditTableColumnDropdownValues {
        table_oid: i64,
        column_oid: i64,
        dropdown_values: Vec<column::DropdownValue>
    },
    DeleteTableColumn {
        table_oid: i64,
        column_oid: i64
    },
    RestoreDeletedTableColumn {
        table_oid: i64,
        column_oid: i64
    },
    PushTableRow {
        table_oid: i64 
    },
    InsertTableRow {
        table_oid: i64,
        row_oid: i64 
    },
    DeleteTableRow {
        table_oid: i64,
        row_oid: i64
    },
    RestoreDeletedTableRow {
        table_oid: i64,
        row_oid: i64
    },
    UpdateTableCellStoredAsPrimitiveValue {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        value: Option<String>
    }
}

static REVERSE_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());
static FORWARD_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());

impl Action {
    fn execute(&self, app: &AppHandle, is_forward: bool) -> Result<(), error::Error> {
        match self {
            Self::CreateTable { table_name } => {
                match table::create(table_name.clone()) {
                    Ok(table_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTable { 
                            table_oid: table_oid
                        });
                        msg_update_table_list(app);
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::DeleteTable { table_oid } => {
                match table::move_trash(table_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::RestoreDeletedTable { 
                            table_oid: table_oid.clone() 
                        });
                        msg_update_table_list(app);
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::RestoreDeletedTable { table_oid } => {
                match table::unmove_trash(table_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTable { 
                            table_oid: table_oid.clone() 
                        });
                        msg_update_table_list(app);
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::CreateTableColumn { 
                table_oid, 
                column_name, 
                column_type, 
                column_ordering, 
                column_style, 
                is_nullable, 
                is_unique, 
                is_primary_key } => {
                
                match column::create(
                    table_oid.clone(), 
                    column_name, 
                    column_type.clone(), 
                    column_ordering.clone(), 
                    column_style, 
                    is_nullable.clone(), 
                    is_unique.clone(), 
                    is_primary_key.clone()) {

                    Ok(column_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTableColumn { 
                            table_oid: table_oid.clone(),
                            column_oid: column_oid
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::EditTableColumnMetadata { 
                table_oid,
                column_oid, 
                column_name, 
                column_type, 
                column_style, 
                is_nullable, 
                is_unique, 
                is_primary_key } => {

                match column::edit(
                    table_oid.clone(),
                    column_oid.clone(), 
                    column_name, 
                    column_type.clone(), 
                    column_style, 
                    is_nullable.clone(), 
                    is_unique.clone(), 
                    is_primary_key.clone()) {

                    Ok(trash_column_oid_optional) => {
                        match trash_column_oid_optional {
                            Some(trash_column_oid) => {
                                let mut reverse_stack = if is_forward {
                                    REVERSE_STACK.lock().unwrap() 
                                } else { 
                                    FORWARD_STACK.lock().unwrap() 
                                };
                                (*reverse_stack).push(Self::RestoreEditedTableColumnMetadata {
                                    table_oid: table_oid.clone(), 
                                    column_oid: column_oid.clone(), 
                                    prior_metadata_column_oid: trash_column_oid 
                                });
                                msg_update_table_data(app, table_oid.clone());
                            },
                            _ => {}
                        }
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::EditTableColumnDropdownValues { table_oid, column_oid, dropdown_values } => {
                let prior_dropdown_values: Vec<column::DropdownValue> = column::get_table_column_dropdown_values(column_oid.clone())?;
                match column::set_table_column_dropdown_values(column_oid.clone(), dropdown_values.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::EditTableColumnDropdownValues {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            dropdown_values: prior_dropdown_values
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::DeleteTableColumn { table_oid, column_oid } => {
                match column::move_trash(table_oid.clone(), column_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::RestoreDeletedTableColumn {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone()
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::RestoreDeletedTableColumn { table_oid, column_oid } => {
                match column::unmove_trash(table_oid.clone(), column_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTableColumn { 
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone() 
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::PushTableRow { table_oid } => {
                match table_data::push(table_oid.clone()) {
                    Ok(row_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTableRow { 
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone() 
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::InsertTableRow { table_oid, row_oid } => {
                match table_data::insert(table_oid.clone(), row_oid.clone()) {
                    Ok(row_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTableRow { 
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone() 
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::DeleteTableRow { table_oid, row_oid } => {
                match table_data::move_trash(table_oid.clone(), row_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::RestoreDeletedTableRow { 
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone() 
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::RestoreDeletedTableRow { table_oid, row_oid } => {
                match table_data::unmove_trash(table_oid.clone(), row_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::DeleteTableRow { 
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone() 
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::UpdateTableCellStoredAsPrimitiveValue { table_oid, column_oid, row_oid, value } => {
                match table_data::try_update_primitive_value(table_oid.clone(), row_oid.clone(), column_oid.clone(), value.clone()) {
                    Ok(old_value) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap() 
                        } else { 
                            FORWARD_STACK.lock().unwrap() 
                        };
                        (*reverse_stack).push(Self::UpdateTableCellStoredAsPrimitiveValue { 
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            value: old_value
                        });
                        msg_update_table_data(app, table_oid.clone());
                    },
                    Err(e) => {
                        msg_update_table_data(app, table_oid.clone());
                        return Err(e);
                    }
                }
            }
            _ => {
                return Err(error::Error::AdhocError("Action has not been implemented."));
            }
        }
        return Ok(());
    }
}



#[tauri::command]
/// Initialize a connection to a StaticDB database file.
pub fn init(path: String) -> Result<(), error::Error> {
    return db::init(path);
}

/// Sends a message to the frontend that the list of tables needs to be updated.
fn msg_update_table_list(app: &AppHandle) {
    app.emit("update-table-list", ()).unwrap();
}

/// Sends a message to the frontend that the currently-displayed table needs to be refreshed.
fn msg_update_table_data(app: &AppHandle, table_oid: i64) {
    app.emit("update-table-data", table_oid).unwrap();
}

/// Sends a message to the frontend that a row in the currently-displayed table needs to be refreshed.
fn msg_update_table_row(app: &AppHandle, table_oid: i64, row_oid: i64) {
    app.emit("update-table-row", (table_oid, row_oid)).unwrap();
}


#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_create_table(app: AppHandle) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableMetadataWindow-{window_idx}"),
        WebviewUrl::App("/src/frontend/dialogTableMetadata.html".into()),
    )
    .title("Create New Table")
    .inner_size(400.0, 150.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_create_table_column(app: AppHandle, table_oid: i64, column_ordering: Option<i64>) -> Result<(), error::Error> {    
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableColumnMetadataWindow-{window_idx}"),
        WebviewUrl::App(
            format!(
                "/src/frontend/dialogTableColumnMetadata.html?table_oid={table_oid}{}", 
                match column_ordering {
                    Some(o) => format!("column_ordering={o}"),
                    None => String::from("")
                }
            ).into()
        ),
    )
    .title("Add New Column")
    .inner_size(400.0, 200.0)
    .resizable(false)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for editing a table column.
pub async fn dialog_edit_table_column(app: AppHandle, table_oid: i64, column_oid: i64) -> Result<(), error::Error> {    
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableColumnMetadataWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/dialogTableColumnMetadata.html?table_oid={table_oid}&column_oid={column_oid}").into()),
    )
    .title("Edit Column")
    .inner_size(400.0, 200.0)
    .resizable(false)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Open a separate window for the contents of a table.
pub async fn dialog_table_data(app: AppHandle, table_oid: i64, table_name: String) -> Result<(), error::Error> {
    // Create the window
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/table.html?table_oid={table_oid}").into()),
    )
    .title(&table_name)
    .inner_size(800.0, 600.0)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Closes the current dialog window.
pub fn dialog_close(window: tauri::Window) -> Result<(), error::Error> {
    match window.close() {
        Ok(_) => { return Ok(()); },
        Err(e) => { return Err(error::Error::TauriError(e)); }
    }
}


#[tauri::command]
pub fn get_table_list(table_channel: Channel<table::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    table::send_metadata_list(table_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_report_list(report_channel: Channel<table::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    return Ok(());
}

#[tauri::command]
pub fn get_object_type_list(object_type_channel: Channel<obj_type::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    obj_type::send_metadata_list(object_type_channel)?;
    return Ok(());
}

#[tauri::command]
/// Get the metadata for a particular column in a table.
pub fn get_table_column(column_oid: i64) -> Result<Option<column::Metadata>, error::Error> {
    return column::get_metadata(column_oid);
}

#[tauri::command]
/// Send possible dropdown values for a column.
pub fn get_table_column_dropdown_values(column_oid: i64, dropdown_value_channel: Channel<column::DropdownValue>) -> Result<(), error::Error> {
    // Use channel to send DropdownValue objects
    column::send_table_column_dropdown_values(column_oid, dropdown_value_channel)?;
    return Ok(());
}

#[tauri::command] 
/// Send possible tables to be referenced.
pub fn get_table_column_reference_values(reference_type_channel: Channel<column::BasicTypeMetadata>) -> Result<(), error::Error> {
    column::send_type_metadata_list(column_type::MetadataColumnType::Reference(0), reference_type_channel)?;
    return Ok(());
}

#[tauri::command] 
/// Send possible global data types for an object.
pub fn get_table_column_object_values(object_type_channel: Channel<column::BasicTypeMetadata>) -> Result<(), error::Error> {
    column::send_type_metadata_list(column_type::MetadataColumnType::ChildObject(0), object_type_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_column_list(table_oid: i64, column_channel: Channel<column::Metadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    column::send_metadata_list(table_oid, column_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_data(table_oid: i64, page_num: i64, page_size: i64, cell_channel: Channel<table_data::Cell>) -> Result<(), error::Error> {
    table_data::send_table_data(table_oid, page_num, page_size, cell_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_row(table_oid: i64, row_oid: i64, cell_channel: Channel<table_data::RowCell>) -> Result<(), error::Error> {
    table_data::send_table_row(table_oid, row_oid, cell_channel)?;
    return Ok(());
}


#[tauri::command]
/// Executes an action that affects the state of the database.
pub fn execute(app: AppHandle, action: Action) -> Result<(), error::Error> {
    // Do something that affects the database
    action.execute(&app, true)?;

    // Clear the stack of undone actions
    let mut forward_stack = FORWARD_STACK.lock().unwrap();
    *forward_stack = Vec::new();
    return Ok(());
}

#[tauri::command]
/// Undoes the last action by popping the top of the reverse stack.
pub fn undo(app: AppHandle) -> Result<(), error::Error> {
    // Get the action from the top of the stack
    match {
        let mut reverse_stack = REVERSE_STACK.lock().unwrap();
        (*reverse_stack).pop()
    } {
        Some(reverse_action) => {
            reverse_action.execute(&app, false)?;
        },
        None => {}
    }
    return Ok(());
}

#[tauri::command]
/// Redoes the last undone action by popping the top of the forward stack.
pub fn redo(app: AppHandle) -> Result<(), error::Error> {
    // Get the action from the top of the stack
    match {
        let mut forward_stack = FORWARD_STACK.lock().unwrap();
        (*forward_stack).pop()
    } {
        Some(forward_action) => {
            forward_action.execute(&app, true)?;
        },
        None => {}
    }
    return Ok(());
}
