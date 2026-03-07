mod data_type;
mod db;
mod obj_type;
mod report;
mod report_column;
mod report_parameter;
//mod report_data;
mod table;
mod table_column;
mod table_data;
use crate::util::error;
use crate::util::channel::Sender;
use serde::{Deserialize};
use std::sync::Mutex;
use tauri::ipc::{Channel as TauriChannel, JavaScriptChannelId};
use tauri::{AppHandle, Emitter, Manager, Webview, WebviewUrl, WebviewWindowBuilder};

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
    EditReportMetadata {
        report_oid: i64,
        report_name: String
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
        dropdown_values: Option<Vec<table_column::DropdownValue>>
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
        dropdown_values: Option<Vec<table_column::DropdownValue>>
    },
    EditTableColumnWidth {
        table_oid: i64,
        column_oid: i64,
        column_width: i64,
    },
    RestoreEditedTableColumnMetadata {
        table_oid: i64,
        column_oid: i64,
        prior_metadata_column_oid: i64,
    },
    EditTableColumnDropdownValues {
        table_oid: i64,
        column_oid: i64,
        dropdown_values: Vec<table_column::DropdownValue>,
    },
    ReorderTableColumn {
        table_oid: i64,
        column_oid: i64,
        old_column_ordering: i64,
        new_column_ordering: Option<i64>,
    },
    DeleteTableColumn {
        table_oid: i64,
        column_oid: i64,
    },
    RestoreDeletedTableColumn {
        table_oid: i64,
        column_oid: i64,
    },
    CreateReportFormulaColumn {
        report_oid: i64,
        column_name: String,
        column_ordering: Option<i64>,
        column_style: String,
        formula: String
    },
    EditReportFormulaColumnMetadata {
        report_oid: i64,
        column_oid: i64,
        column_name: String,
        column_style: String,
        formula: String
    },
    CreateReportSubreportColumn {
        report_oid: i64,
        column_name: String,
        column_ordering: Option<i64>,
        column_style: String,
        base_parameter_oid: i64
    },
    EditReportSubreportColumnMetadata {
        report_oid: i64,
        column_oid: i64,
        column_name: String,
        column_style: String
    },
    EditReportColumnWidth {
        report_oid: i64,
        column_oid: i64,
        column_width: i64,
    },
    RestoreEditedReportColumnMetadata {
        report_oid: i64,
        column_oid: i64,
        prior_metadata_column_oid: i64,
    },
    ReorderReportColumn {
        report_oid: i64,
        column_oid: i64,
        old_column_ordering: i64,
        new_column_ordering: Option<i64>,
    },
    DeleteReportColumn {
        report_oid: i64,
        column_oid: i64,
    },
    RestoreDeletedReportColumn {
        report_oid: i64,
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
    UpdateTableCellStoredAsMultiselectValue {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        column_type_oid: i64,
        value_oid_list: Vec<i64>
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

/// Records the opposite action to the one that was just performed, for undo/redo purposes.
fn record_action(action: Action, is_forward: bool) {
    let mut reverse_stack = if is_forward {
        REVERSE_STACK.lock().unwrap()
    } else {
        FORWARD_STACK.lock().unwrap()
    };
    (*reverse_stack).push(action);
}

impl Action {
    fn execute(&self, app: &AppHandle, is_forward: bool) -> Result<(), error::Error> {
        match self {
            Self::CreateTable {
                table_name,
                master_table_oid_list,
            } => {
                let table_oid = table::create(table_name.clone(), master_table_oid_list, data_type::MetadataColumnType::Reference(0))?;
                record_action(Self::DeleteTable {
                    table_oid: table_oid,
                }, is_forward);
                msg_update_table_list(app);
            },
            Self::EditTableMetadata { table_oid, table_name, master_table_oid_list } => {
                let (old_table_name, old_master_table_oid_list) = table::edit(table_oid.clone(), table_name.clone(), master_table_oid_list)?;
                record_action(Self::EditTableMetadata {
                    table_oid: table_oid.clone(),
                    table_name: old_table_name,
                    master_table_oid_list: old_master_table_oid_list
                }, is_forward);
                msg_update_table_list(app);
            },
            Self::DeleteTable { table_oid } => {
                table::trash(table_oid.clone())?;
                record_action(Self::RestoreDeletedTable {
                    table_oid: table_oid.clone(),
                }, is_forward);
                msg_update_table_list(app);
            },
            Self::RestoreDeletedTable { table_oid } => {
                table::untrash(table_oid.clone())?;
                record_action(Self::DeleteTable {
                    table_oid: table_oid.clone(),
                }, is_forward);
                msg_update_table_list(app);
            }
            Self::CreateReport {
                report_name,
                base_table_oid,
            } => {
                let report_oid = report::create(&report_name, base_table_oid.clone())?;
                record_action(Self::DeleteReport {
                    report_oid,
                }, is_forward);
                msg_update_report_list(app);
            },
            Self::EditReportMetadata { report_oid, report_name } => {
                let old_report_name = report::edit(report_oid.clone(), &report_name)?;
                record_action(Self::EditReportMetadata { 
                    report_oid: report_oid.clone(), 
                    report_name: old_report_name 
                }, is_forward);
                msg_update_report_list(app);
            },
            Self::DeleteReport { report_oid } => {
                report::trash(report_oid.clone())?;
                record_action(Self::RestoreDeletedReport {
                    report_oid: report_oid.clone(),
                }, is_forward);
                msg_update_report_list(app);
            },
            Self::RestoreDeletedReport { report_oid } => {
                report::untrash(report_oid.clone())?;
                record_action(Self::DeleteReport {
                    report_oid: report_oid.clone(),
                }, is_forward);
                msg_update_report_list(app);
            }
            Self::CreateObjectType {
                obj_type_name,
                master_table_oid_list,
            } => {
                let obj_type_oid = table::create(obj_type_name.clone(), master_table_oid_list, data_type::MetadataColumnType::ChildObject(0))?;
                record_action(Self::DeleteObjectType {
                    obj_type_oid: obj_type_oid,
                }, is_forward);
                msg_update_obj_type_list(app);
            },
            Self::EditObjectTypeMetadata { obj_type_oid, obj_type_name, master_table_oid_list } => {
                let (old_obj_type_name, old_master_table_oid_list) = table::edit(obj_type_oid.clone(), obj_type_name.clone(), master_table_oid_list)?;
                record_action(Self::EditObjectTypeMetadata {
                    obj_type_oid: obj_type_oid.clone(),
                    obj_type_name: old_obj_type_name,
                    master_table_oid_list: old_master_table_oid_list
                }, is_forward);
                msg_update_obj_type_list(app);
            },
            Self::DeleteObjectType { obj_type_oid } => {
                table::trash(obj_type_oid.clone())?;
                record_action(Self::RestoreDeletedObjectType {
                    obj_type_oid: obj_type_oid.clone(),
                }, is_forward);
                msg_update_obj_type_list(app);
            }
            Self::RestoreDeletedObjectType { obj_type_oid } => {
                table::untrash(obj_type_oid.clone())?;
                record_action(Self::DeleteObjectType {
                    obj_type_oid: obj_type_oid.clone(),
                }, is_forward);
                msg_update_obj_type_list(app);
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
                dropdown_values
            } => {
                let column_oid = table_column::create(
                    table_oid.clone(),
                    column_name,
                    column_type.clone(),
                    column_ordering.clone(),
                    column_style,
                    is_nullable.clone(),
                    is_unique.clone(),
                    is_primary_key.clone(),
                    dropdown_values.clone()
                )?;
                record_action(Self::DeleteTableColumn {
                    table_oid: table_oid.clone(),
                    column_oid: column_oid,
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
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
                dropdown_values
            } => {
                if let Some(trash_column_oid) = table_column::edit(
                    table_oid.clone(),
                    column_oid.clone(),
                    column_name,
                    column_type.clone(),
                    column_style,
                    is_nullable.clone(),
                    is_unique.clone(),
                    is_primary_key.clone(),
                    dropdown_values.clone()
                )? {
                    record_action(Self::RestoreEditedTableColumnMetadata {
                        table_oid: table_oid.clone(),
                        column_oid: column_oid.clone(),
                        prior_metadata_column_oid: trash_column_oid,
                    }, is_forward);
                    msg_update_table_data_deep(app, table_oid.clone());
                }
            },
            Self::EditTableColumnWidth { table_oid, column_oid, column_width } => {
                let trash_column_oid = table_column::edit_width(table_oid.clone(), column_oid.clone(), column_width.clone())?;
                record_action(Self::RestoreEditedTableColumnMetadata {
                    table_oid: table_oid.clone(),
                    column_oid: column_oid.clone(),
                    prior_metadata_column_oid: trash_column_oid,
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            }
            Self::EditTableColumnDropdownValues {
                table_oid,
                column_oid,
                dropdown_values,
            } => {
                let prior_dropdown_values: Vec<table_column::DropdownValue> =
                    table_column::get_table_column_dropdown_values(column_oid.clone())?;
                table_column::set_table_column_dropdown_values(
                    column_oid.clone(),
                    dropdown_values.clone(),
                )?;
                record_action(Self::EditTableColumnDropdownValues {
                    table_oid: table_oid.clone(),
                    column_oid: column_oid.clone(),
                    dropdown_values: prior_dropdown_values,
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
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
                        record_action(Self::ReorderTableColumn {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            old_column_ordering: new_column_ordering,
                            new_column_ordering: Some(old_column_ordering.clone()),
                        }, is_forward);
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
            } => {
                table_column::trash(table_oid.clone(), column_oid.clone())?;
                record_action(Self::RestoreDeletedTableColumn {
                    table_oid: table_oid.clone(),
                    column_oid: column_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            },
            Self::RestoreDeletedTableColumn {
                table_oid,
                column_oid,
            } => {
                table_column::untrash(table_oid.clone(), column_oid.clone())?;
                record_action(Self::DeleteTableColumn {
                    table_oid: table_oid.clone(),
                    column_oid: column_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            },
            Self::CreateReportFormulaColumn { report_oid, column_name, column_ordering, column_style, formula } => {
                let column_oid = report_column::create_formula(
                    report_oid.clone(),
                    &column_name, 
                    column_ordering.clone(), 
                    &column_style, 
                    &formula
                )?;
                record_action(Self::DeleteReportColumn { 
                    report_oid: report_oid.clone(), 
                    column_oid: column_oid.clone()
                }, is_forward);
                msg_update_report_data_deep(app, report_oid.clone());
            },
            Self::CreateReportSubreportColumn { report_oid, column_name, column_ordering, column_style, base_parameter_oid } => {
                let column_oid = report_column::create_subreport(
                    report_oid.clone(),
                    &column_name, 
                    column_ordering.clone(), 
                    &column_style, 
                    base_parameter_oid.clone()
                )?;
                record_action(Self::DeleteReportColumn { 
                    report_oid: report_oid.clone(), 
                    column_oid: column_oid.clone()
                }, is_forward);
                msg_update_report_data_deep(app, report_oid.clone());
            },
            Self::PushTableRow { table_oid, parent_row_oid } => {
                let row_oid = table_data::push(table_oid.clone(), parent_row_oid.clone())?;
                record_action(Self::DeleteTableRow {
                    table_oid: table_oid.clone(),
                    row_oid: row_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            },
            Self::InsertTableRow { table_oid, parent_row_oid, row_oid } => {
                let row_oid = table_data::insert(table_oid.clone(), parent_row_oid.clone(), row_oid.clone())?;
                record_action(Self::DeleteTableRow {
                    table_oid: table_oid.clone(),
                    row_oid: row_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            }
            Self::RetypeTableRow {
                base_type_oid,
                base_row_oid,
                new_subtype_oid,
            } => {
                let old_subtype_oid = table_data::retype(
                    base_type_oid.clone(),
                    base_row_oid.clone(),
                    new_subtype_oid.clone(),
                )?;
                record_action(Self::RetypeTableRow {
                    base_type_oid: base_type_oid.clone(),
                    base_row_oid: base_row_oid.clone(),
                    new_subtype_oid: old_subtype_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, base_type_oid.clone());
            }
            Self::DeleteTableRow { table_oid, row_oid } => {
                let (table_oid, row_oid) = table_data::trash(table_oid.clone(), row_oid.clone())?;
                record_action(Self::RestoreDeletedTableRow {
                    table_oid: table_oid.clone(),
                    row_oid: row_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
            }
            Self::RestoreDeletedTableRow { table_oid, row_oid } => {
                table_data::untrash(table_oid.clone(), row_oid.clone())?;
                record_action(Self::DeleteTableRow {
                    table_oid: table_oid.clone(),
                    row_oid: row_oid.clone(),
                }, is_forward);
                msg_update_table_data_deep(app, table_oid.clone());
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
                        record_action(Self::UpdateTableCellStoredAsPrimitiveValue {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            value: old_value,
                        }, is_forward);
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                        return Err(e);
                    }
                }
            },
            Self::UpdateTableCellStoredAsMultiselectValue { table_oid, column_oid, row_oid, column_type_oid, value_oid_list } => {
                match table_data::try_update_multiselect_value(
                    table_oid.clone(), 
                    row_oid.clone(), 
                    column_oid.clone(), 
                    column_type_oid.clone(), 
                    value_oid_list.clone()) {

                    Ok(prior_value_oid_list) => {
                        record_action(Self::UpdateTableCellStoredAsMultiselectValue { 
                            table_oid: table_oid.clone(), 
                            column_oid: column_oid.clone(), 
                            row_oid: row_oid.clone(), 
                            column_type_oid: column_type_oid.clone(), 
                            value_oid_list: prior_value_oid_list 
                        }, is_forward);

                        // Send message to update table display
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                    },
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                        return Err(e);
                    }
                }
            },
            Self::UpdateTableCellStoredAsBlob { table_oid, column_oid, row_oid, file_path } => {
                match table_data::try_update_blob_value(table_oid.clone(), row_oid.clone(), column_oid.clone(), file_path.clone()) {
                    Ok(_) => {
                        // This action cannot be undone
                        // (for now)

                        // Send message to update table display
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                    },
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
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
                        record_action(Self::UnsetTableObjectCell {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            obj_type_oid,
                            obj_row_oid,
                        }, is_forward);
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
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
                        record_action(Self::SetTableObjectCell {
                            table_oid: table_oid.clone(),
                            column_oid: column_oid.clone(),
                            row_oid: row_oid.clone(),
                            obj_type_oid: Some(obj_type_oid.clone()),
                            obj_row_oid: Some(obj_row_oid.clone()),
                        }, is_forward);
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
                    }
                    Err(e) => {
                        msg_update_table_data_shallow(app, table_oid.clone(), None);
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


#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Dialog {
    CreateTable,
    EditTableMetadata {
        table_oid: i64 
    },
    CreateReport,
    EditReportMetadata {
        report_oid: i64 
    },
    CreateObjectType,
    EditObjectTypeMetadata {
        obj_type_oid: i64 
    },
    CreateTableColumn {
        table_oid: i64,
        column_ordering: Option<i64>
    },
    EditTableColumnMetadata {
        table_oid: i64,
        column_oid: i64 
    },
    CreateReportColumn {
        report_oid: i64,
        column_ordering: Option<i64>
    },
    EditReportColumnMetadata {
        report_oid: i64,
        column_oid: i64 
    },
    Table {
        table_oid: i64,
        table_name: String
    },
    ChildTable {
        table_oid: i64,
        parent_row_oid: i64,
        table_name: String
    },
    TableObject {
        table_oid: i64,
        row_oid: i64,
        object_name: String
    },
    Report {
        report_oid: i64,
        report_name: String
    }
}