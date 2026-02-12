mod data_type;
mod db;
mod obj_type;
mod report;
mod report_column;
mod report_data;
mod table;
mod table_column;
mod table_data;
use crate::util::error;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::ipc::{Channel, InvokeError};
use tauri::menu::{ContextMenu, Menu, MenuBuilder, MenuItem};
use tauri::{AppHandle, Emitter, Manager, PhysicalSize, Size, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_dialog::{DialogExt, MessageDialogKind};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Action {
    CreateTable {
        table_name: String,
        master_table_oid_list: Vec<i64>,
    },
    EditTableMetadata {
        table_oid: i64,
        table_name: String,
        master_table_oid_list: Vec<i64>
    },
    DeleteTable {
        table_oid: i64,
    },
    RestoreDeletedTable {
        table_oid: i64,
    },
    CreateReport {
        report_name: String,
        base_table_oid: i64,
    },
    DeleteReport {
        report_oid: i64,
    },
    RestoreDeletedReport {
        report_oid: i64,
    },
    CreateObjectType {
        obj_type_name: String,
        master_table_oid_list: Vec<i64>,
    },
    EditObjectTypeMetadata {
        obj_type_oid: i64,
        obj_type_name: String,
        master_table_oid_list: Vec<i64>
    },
    DeleteObjectType {
        obj_type_oid: i64,
    },
    RestoreDeletedObjectType {
        obj_type_oid: i64,
    },
    CreateTableColumn {
        table_oid: i64,
        column_name: String,
        column_type: data_type::MetadataColumnType,
        column_ordering: Option<i64>,
        column_style: String,
        is_nullable: bool,
        is_unique: bool,
        is_primary_key: bool,
    },
    EditTableColumnMetadata {
        table_oid: i64,
        column_oid: i64,
        column_name: String,
        column_type: data_type::MetadataColumnType,
        column_style: String,
        is_nullable: bool,
        is_unique: bool,
        is_primary_key: bool,
    },
    RestoreEditedTableColumnMetadata {
        table_oid: i64,
        column_oid: i64,
        prior_metadata_column_oid: i64,
    },
    EditTableColumnWidth {
        table_oid: i64,
        column_oid: i64,
        column_width: i64,
    },
    ReorderTableColumn {
        table_oid: i64,
        column_oid: i64,
        old_column_ordering: i64,
        new_column_ordering: Option<i64>,
    },
    EditTableColumnDropdownValues {
        table_oid: i64,
        column_oid: i64,
        dropdown_values: Vec<table_column::DropdownValue>,
    },
    DeleteTableColumn {
        table_oid: i64,
        column_oid: i64,
    },
    RestoreDeletedTableColumn {
        table_oid: i64,
        column_oid: i64,
    },
    PushTableRow {
        table_oid: i64,
        parent_row_oid: Option<i64>
    },
    InsertTableRow {
        table_oid: i64,
        parent_row_oid: Option<i64>,
        row_oid: i64,
    },
    RetypeTableRow {
        base_type_oid: i64,
        base_row_oid: i64,
        new_subtype_oid: i64,
    },
    DeleteTableRow {
        table_oid: i64,
        row_oid: i64,
    },
    RestoreDeletedTableRow {
        table_oid: i64,
        row_oid: i64,
    },
    UpdateTableCellStoredAsPrimitiveValue {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        value: Option<String>,
    },
    UpdateTableCellStoredAsBlob {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        file_path: String
    },
    SetTableObjectCell {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        obj_type_oid: Option<i64>,
        obj_row_oid: Option<i64>,
    },
    UnsetTableObjectCell {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        obj_type_oid: i64,
        obj_row_oid: i64,
    },
}

static REVERSE_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());
static FORWARD_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());

impl Action {
    fn execute(&self, app: &AppHandle, is_forward: bool) -> Result<(), error::Error> {
        match self {
            Self::CreateTable {
                table_name,
                master_table_oid_list,
            } => {
                match table::create(table_name.clone(), master_table_oid_list, data_type::MetadataColumnType::Reference(0)) {
                    Ok(table_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteTable {
                            table_oid: table_oid,
                        });
                        msg_update_table_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::EditTableMetadata { table_oid, table_name, master_table_oid_list } => {
                match table::edit(table_oid.clone(), table_name.clone(), master_table_oid_list) {
                    Ok((old_table_name, old_master_table_oid_list)) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::EditTableMetadata {
                            table_oid: table_oid.clone(),
                            table_name: old_table_name,
                            master_table_oid_list: old_master_table_oid_list
                        });
                        msg_update_table_list(app);
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
                            table_oid: table_oid.clone(),
                        });
                        msg_update_table_list(app);
                    }
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
                            table_oid: table_oid.clone(),
                        });
                        msg_update_table_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::CreateReport {
                report_name,
                base_table_oid,
            } => match report::create(&report_name, base_table_oid.clone()) {
                Ok(report_oid) => {
                    let mut reverse_stack = if is_forward {
                        REVERSE_STACK.lock().unwrap()
                    } else {
                        FORWARD_STACK.lock().unwrap()
                    };
                    (*reverse_stack).push(Self::DeleteReport {
                        report_oid: report_oid,
                    });
                    msg_update_report_list(app);
                }
                Err(e) => {
                    return Err(e);
                }
            },
            Self::DeleteReport { report_oid } => match report::move_trash(report_oid.clone()) {
                Ok(_) => {
                    let mut reverse_stack = if is_forward {
                        REVERSE_STACK.lock().unwrap()
                    } else {
                        FORWARD_STACK.lock().unwrap()
                    };
                    (*reverse_stack).push(Self::RestoreDeletedReport {
                        report_oid: report_oid.clone(),
                    });
                    msg_update_report_list(app);
                }
                Err(e) => {
                    return Err(e);
                }
            },
            Self::RestoreDeletedReport { report_oid } => {
                match report::unmove_trash(report_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteReport {
                            report_oid: report_oid.clone(),
                        });
                        msg_update_report_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::CreateObjectType {
                obj_type_name,
                master_table_oid_list,
            } => {
                match table::create(obj_type_name.clone(), master_table_oid_list, data_type::MetadataColumnType::ChildObject(0)) {
                    Ok(obj_type_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteObjectType {
                            obj_type_oid: obj_type_oid,
                        });
                        msg_update_obj_type_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::EditObjectTypeMetadata { obj_type_oid, obj_type_name, master_table_oid_list } => {
                match table::edit(obj_type_oid.clone(), obj_type_name.clone(), master_table_oid_list) {
                    Ok((old_obj_type_name, old_master_table_oid_list)) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::EditObjectTypeMetadata {
                            obj_type_oid: obj_type_oid.clone(),
                            obj_type_name: old_obj_type_name,
                            master_table_oid_list: old_master_table_oid_list
                        });
                        msg_update_obj_type_list(app);
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::DeleteObjectType { obj_type_oid } => {
                match table::move_trash(obj_type_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::RestoreDeletedObjectType {
                            obj_type_oid: obj_type_oid.clone(),
                        });
                        msg_update_obj_type_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::RestoreDeletedObjectType { obj_type_oid } => {
                match table::unmove_trash(obj_type_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteObjectType {
                            obj_type_oid: obj_type_oid.clone(),
                        });
                        msg_update_obj_type_list(app);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::CreateTableColumn {
                table_oid,
                column_name,
                column_type,
                column_ordering,
                column_style,
                is_nullable,
                is_unique,
                is_primary_key,
            } => {
                match table_column::create(
                    table_oid.clone(),
                    column_name,
                    column_type.clone(),
                    column_ordering.clone(),
                    column_style,
                    is_nullable.clone(),
                    is_unique.clone(),
                    is_primary_key.clone(),
                ) {
                    Ok(column_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteTableColumn {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid,
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
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
                is_primary_key,
            } => {
                match table_column::edit(
                    table_oid.clone(),
                    column_oid.clone(),
                    column_name,
                    column_type.clone(),
                    column_style,
                    is_nullable.clone(),
                    is_unique.clone(),
                    is_primary_key.clone(),
                ) {
                    Ok(trash_column_oid_optional) => match trash_column_oid_optional {
                        Some(trash_column_oid) => {
                            let mut reverse_stack = if is_forward {
                                REVERSE_STACK.lock().unwrap()
                            } else {
                                FORWARD_STACK.lock().unwrap()
                            };
                            (*reverse_stack).push(Self::RestoreEditedTableColumnMetadata {
                                table_oid: table_oid.clone(),
                                column_oid: column_oid.clone(),
                                prior_metadata_column_oid: trash_column_oid,
                            });
                            msg_update_table_data_deep(app, table_oid.clone());
                        }
                        _ => {}
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::EditTableColumnWidth { table_oid, column_oid, column_width } => {
                match table_column::edit_width(table_oid.clone(), column_oid.clone(), column_width.clone()) {
                    Ok(trash_column_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::RestoreEditedTableColumnMetadata {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            prior_metadata_column_oid: trash_column_oid,
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    },
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::EditTableColumnDropdownValues {
                table_oid,
                column_oid,
                dropdown_values,
            } => {
                let prior_dropdown_values: Vec<table_column::DropdownValue> =
                    table_column::get_table_column_dropdown_values(column_oid.clone())?;
                match table_column::set_table_column_dropdown_values(
                    column_oid.clone(),
                    dropdown_values.clone(),
                ) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::EditTableColumnDropdownValues {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            dropdown_values: prior_dropdown_values,
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::ReorderTableColumn {
                table_oid,
                column_oid,
                old_column_ordering,
                new_column_ordering,
            } => {
                match table_column::reorder(
                    table_oid.clone(),
                    column_oid.clone(),
                    new_column_ordering.clone(),
                ) {
                    Ok(new_column_ordering) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::ReorderTableColumn {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            old_column_ordering: new_column_ordering,
                            new_column_ordering: Some(old_column_ordering.clone()),
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        msg_update_table_data_deep(app, table_oid.clone());
                        return Err(e);
                    }
                }
            }
            Self::DeleteTableColumn {
                table_oid,
                column_oid,
            } => match table_column::move_trash(table_oid.clone(), column_oid.clone()) {
                Ok(_) => {
                    let mut reverse_stack = if is_forward {
                        REVERSE_STACK.lock().unwrap()
                    } else {
                        FORWARD_STACK.lock().unwrap()
                    };
                    (*reverse_stack).push(Self::RestoreDeletedTableColumn {
                        table_oid: table_oid.clone(),
                        column_oid: column_oid.clone(),
                    });
                    msg_update_table_data_deep(app, table_oid.clone());
                }
                Err(e) => {
                    return Err(e);
                }
            },
            Self::RestoreDeletedTableColumn {
                table_oid,
                column_oid,
            } => match table_column::unmove_trash(table_oid.clone(), column_oid.clone()) {
                Ok(_) => {
                    let mut reverse_stack = if is_forward {
                        REVERSE_STACK.lock().unwrap()
                    } else {
                        FORWARD_STACK.lock().unwrap()
                    };
                    (*reverse_stack).push(Self::DeleteTableColumn {
                        table_oid: table_oid.clone(),
                        column_oid: column_oid.clone(),
                    });
                    msg_update_table_data_deep(app, table_oid.clone());
                }
                Err(e) => {
                    return Err(e);
                }
            },
            Self::PushTableRow { table_oid, parent_row_oid } => {
                match table_data::push(table_oid.clone(), parent_row_oid.clone()) {
                    Ok(row_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteTableRow {
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone(),
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            },
            Self::InsertTableRow { table_oid, parent_row_oid, row_oid } => {
                match table_data::insert(table_oid.clone(), parent_row_oid.clone(), row_oid.clone()) {
                    Ok(row_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteTableRow {
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone(),
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::RetypeTableRow {
                base_type_oid,
                base_row_oid,
                new_subtype_oid,
            } => {
                match table_data::retype(
                    base_type_oid.clone(),
                    base_row_oid.clone(),
                    new_subtype_oid.clone(),
                ) {
                    Ok(old_subtype_oid) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::RetypeTableRow {
                            base_type_oid: base_type_oid.clone(),
                            base_row_oid: base_row_oid.clone(),
                            new_subtype_oid: old_subtype_oid.clone(),
                        });
                        msg_update_table_data_deep(app, base_type_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::DeleteTableRow { table_oid, row_oid } => {
                match table_data::trash(table_oid.clone(), row_oid.clone()) {
                    Ok((table_oid, row_oid)) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::RestoreDeletedTableRow {
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone(),
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::RestoreDeletedTableRow { table_oid, row_oid } => {
                match table_data::untrash(table_oid.clone(), row_oid.clone()) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::DeleteTableRow {
                            table_oid: table_oid.clone(),
                            row_oid: row_oid.clone(),
                        });
                        msg_update_table_data_deep(app, table_oid.clone());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Self::UpdateTableCellStoredAsPrimitiveValue {
                table_oid,
                column_oid,
                row_oid,
                value,
            } => {
                match table_data::try_update_primitive_value(
                    table_oid.clone(),
                    row_oid.clone(),
                    column_oid.clone(),
                    value.clone(),
                ) {
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
                            value: old_value,
                        });
                        msg_update_table_data_shallow(app, table_oid.clone());
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone());
                        return Err(e);
                    }
                }
            },
            Self::UpdateTableCellStoredAsBlob { table_oid, column_oid, row_oid, file_path } => {
                match table_data::try_update_blob_value(table_oid.clone(), row_oid.clone(), column_oid.clone(), file_path.clone()) {
                    Ok(_) => {
                        // This action cannot be undone

                        // Send message to update table display
                        msg_update_table_data_shallow(app, table_oid.clone());
                    },
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone());
                        return Err(e);
                    }
                }
            },
            Self::SetTableObjectCell {
                table_oid,
                column_oid,
                row_oid,
                obj_type_oid,
                obj_row_oid,
            } => {
                match table_data::set_table_object_value(
                    table_oid.clone(),
                    row_oid.clone(),
                    column_oid.clone(),
                    obj_type_oid.clone(),
                    obj_row_oid.clone(),
                ) {
                    Ok((obj_type_oid, obj_row_oid)) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::UnsetTableObjectCell {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            obj_type_oid,
                            obj_row_oid,
                        });
                        msg_update_table_data_shallow(app, table_oid.clone());
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone());
                        return Err(e);
                    }
                }
            }
            Self::UnsetTableObjectCell {
                table_oid,
                column_oid,
                row_oid,
                obj_type_oid,
                obj_row_oid,
            } => {
                match table_data::unset_table_object_value(
                    table_oid.clone(),
                    row_oid.clone(),
                    column_oid.clone(),
                    obj_type_oid.clone(),
                    obj_row_oid.clone(),
                ) {
                    Ok(_) => {
                        let mut reverse_stack = if is_forward {
                            REVERSE_STACK.lock().unwrap()
                        } else {
                            FORWARD_STACK.lock().unwrap()
                        };
                        (*reverse_stack).push(Self::SetTableObjectCell {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            obj_type_oid: Some(obj_type_oid.clone()),
                            obj_row_oid: Some(obj_row_oid.clone()),
                        });
                        msg_update_table_data_shallow(app, table_oid.clone());
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone());
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

/// Sends a message to the frontend that the list of reports needs to be updated.
fn msg_update_report_list(app: &AppHandle) {
    app.emit("update-report-list", ()).unwrap();
}

/// Sends a message to the frontend that the list of object types needs to be updated.
fn msg_update_obj_type_list(app: &AppHandle) {
    app.emit("update-object-type-list", ()).unwrap();
}

/// Sends a message to the frontend that the currently-displayed table needs to be deep refreshed.
fn msg_update_table_data_deep(app: &AppHandle, table_oid: i64) {
    app.emit("update-table-data-deep", table_oid).unwrap();
}

/// Sends a message to the frontend that the currently-displayed table needs to be shallow refreshed.
fn msg_update_table_data_shallow(app: &AppHandle, table_oid: i64) {
    app.emit("update-table-data-shallow", table_oid).unwrap();
}

/// Sends a message to the frontend that the values for a specific row need to be shallow refreshed.
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
        WebviewUrl::App("/src/frontend/dialogTableMetadata.html?mode=3".into()),
    )
    .title("Create New Table")
    .inner_size(400.0, 250.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_edit_table(app: AppHandle, table_oid: i64) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableMetadataWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/dialogTableMetadata.html?table_oid={table_oid}&mode=3").into()),
    )
    .title("Edit Table")
    .inner_size(400.0, 250.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new object type.
pub async fn dialog_create_object_type(app: AppHandle) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableMetadataWindow-{window_idx}"),
        WebviewUrl::App("/src/frontend/dialogTableMetadata.html?mode=4".into()),
    )
    .title("Create New Object Type")
    .inner_size(400.0, 250.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new object type.
pub async fn dialog_edit_object_type(app: AppHandle, obj_type_oid: i64) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableMetadataWindow-{window_idx}"),
        WebviewUrl::App(format!("/src/frontend/dialogTableMetadata.html?table_oid={obj_type_oid}&mode=4").into()),
    )
    .title("Edit Object Type")
    .inner_size(400.0, 250.0)
    .maximizable(false)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Pull up a dialog window for creating a new table.
pub async fn dialog_create_table_column(
    app: AppHandle,
    table_oid: i64,
    column_ordering: Option<i64>,
) -> Result<(), error::Error> {
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableColumnMetadataWindow-{window_idx}"),
        WebviewUrl::App(
            format!(
                "/src/frontend/dialogTableColumnMetadata.html?table_oid={table_oid}{}",
                match column_ordering {
                    Some(o) => format!("&column_ordering={o}"),
                    None => String::from(""),
                }
            )
            .into(),
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
pub async fn dialog_edit_table_column(
    app: AppHandle,
    table_oid: i64,
    column_oid: i64,
) -> Result<(), error::Error> {
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
pub async fn dialog_table_data(
    app: AppHandle,
    table_oid: i64,
    table_name: String,
) -> Result<(), error::Error> {
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
/// Open a separate window for the contents of a table.
pub async fn dialog_child_table_data(
    app: AppHandle,
    table_oid: i64,
    parent_row_oid: i64,
    table_name: String,
) -> Result<(), error::Error> {
    // Create the window
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("childTableWindow-{window_idx}"),
        WebviewUrl::App(
            format!(
                "/src/frontend/table.html?table_oid={table_oid}&parent_row_oid={parent_row_oid}"
            )
            .into(),
        ),
    )
    .title(&table_name)
    .inner_size(800.0, 600.0)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Open a separate window for the contents of an object.
pub async fn dialog_object_data(
    app: AppHandle,
    table_oid: i64,
    row_oid: i64,
    title: String,
) -> Result<(), error::Error> {
    // Create the window
    let window_idx = app.webview_windows().len();
    WebviewWindowBuilder::new(
        &app,
        format!("tableObjectWindow-{window_idx}"),
        WebviewUrl::App(
            format!("/src/frontend/table_object.html?table_oid={table_oid}&obj_oid={row_oid}")
                .into(),
        ),
    )
    .title(&title)
    .inner_size(800.0, 600.0)
    .build()?;
    return Ok(());
}

#[tauri::command]
/// Closes the current dialog window.
pub fn dialog_close(window: tauri::Window) -> Result<(), error::Error> {
    match window.close() {
        Ok(_) => {
            return Ok(());
        }
        Err(e) => {
            return Err(error::Error::TauriError(e));
        }
    }
}

#[tauri::command]
pub fn get_table_list(table_channel: Channel<table::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    table::send_metadata_list(table_channel)?;
    return Ok(());
}

#[tauri::command]
/// Gets the metadata for a table.
pub fn get_table_metadata(table_oid: i64) -> Result<table::Metadata, error::Error> {
    return table::get_metadata(&table_oid);
}

#[tauri::command]
pub fn get_report_list(report_channel: Channel<table::BasicMetadata>) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    return Ok(());
}

#[tauri::command]
pub fn get_object_type_list(
    object_type_channel: Channel<obj_type::BasicMetadata>,
) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    obj_type::send_metadata_list(None, object_type_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_master_list_option_dropdown_values(
    table_oid: Option<i64>,
    allow_inheritance_from_tables: bool,
    option_channel: Channel<table::MasterListOption>,
) -> Result<(), error::Error> {
    table::send_master_list_options(table_oid, allow_inheritance_from_tables, option_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_subtype_list(
    table_oid: i64,
    object_type_channel: Channel<obj_type::BasicMetadata>,
) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    obj_type::send_metadata_list(Some(table_oid), object_type_channel)?;
    return Ok(());
}

#[tauri::command]
/// Get the metadata for a particular column in a table.
pub fn get_table_column(column_oid: i64) -> Result<Option<table_column::Metadata>, error::Error> {
    return table_column::get_metadata(column_oid);
}

#[tauri::command]
/// Send possible dropdown values for a column.
pub fn get_table_column_dropdown_values(
    column_oid: i64,
    dropdown_value_channel: Channel<table_column::DropdownValue>,
) -> Result<(), error::Error> {
    // Use channel to send DropdownValue objects
    table_column::send_table_column_dropdown_values(column_oid, dropdown_value_channel)?;
    return Ok(());
}

#[tauri::command]
/// Send possible tables to be referenced.
pub fn get_table_column_reference_values(
    reference_type_channel: Channel<table_column::BasicTypeMetadata>,
) -> Result<(), error::Error> {
    table_column::send_type_metadata_list(
        data_type::MetadataColumnType::Reference(0),
        reference_type_channel,
    )?;
    return Ok(());
}

#[tauri::command]
/// Send possible global data types for an object.
pub fn get_table_column_object_values(
    object_type_channel: Channel<table_column::BasicTypeMetadata>,
) -> Result<(), error::Error> {
    table_column::send_type_metadata_list(
        data_type::MetadataColumnType::ChildObject(0),
        object_type_channel,
    )?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_column_list(
    table_oid: i64,
    column_channel: Channel<table_column::Metadata>,
) -> Result<(), error::Error> {
    // Use channel to send BasicMetadata objects
    table_column::send_metadata_list(table_oid, column_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_data(
    table_oid: i64,
    parent_row_oid: Option<i64>,
    page_num: i64,
    page_size: i64,
    cell_channel: Channel<table_data::Cell>,
) -> Result<(), error::Error> {
    table_data::send_table_data(table_oid, parent_row_oid, page_num, page_size, cell_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_table_row(
    table_oid: i64,
    row_oid: i64,
    cell_channel: Channel<table_data::RowCell>,
) -> Result<(), error::Error> {
    table_data::send_table_row(table_oid, row_oid, cell_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_object_data(
    obj_type_oid: i64,
    obj_row_oid: i64,
    obj_data_channel: Channel<table_data::RowCell>,
) -> Result<(), error::Error> {
    obj_type::send_obj_data(obj_type_oid, obj_row_oid, obj_data_channel)?;
    return Ok(());
}

#[tauri::command]
pub fn get_blob_value(table_oid: i64, row_oid: i64, column_oid: i64) -> Result<String, error::Error> {
    return table_data::get_blob_value(table_oid, row_oid, column_oid);
}

#[tauri::command]
pub fn download_blob_value(table_oid: i64, row_oid: i64, column_oid: i64, file_path: String) -> Result<(), error::Error> {
    return table_data::download_blob_value(table_oid, row_oid, column_oid, file_path);
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

/// Undoes the last action by popping the top of the reverse stack.
pub fn undo(app: &AppHandle) -> Result<(), error::Error> {
    // Get the action from the top of the stack
    match {
        let mut reverse_stack = REVERSE_STACK.lock().unwrap();
        (*reverse_stack).pop()
    } {
        Some(reverse_action) => {
            reverse_action.execute(app, false)?;
        }
        None => {}
    }
    return Ok(());
}

/// Redoes the last undone action by popping the top of the forward stack.
pub fn redo(app: &AppHandle) -> Result<(), error::Error> {
    // Get the action from the top of the stack
    match {
        let mut forward_stack = FORWARD_STACK.lock().unwrap();
        (*forward_stack).pop()
    } {
        Some(forward_action) => {
            forward_action.execute(app, true)?;
        }
        None => {}
    }
    return Ok(());
}
