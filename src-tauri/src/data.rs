use serde::Deserialize;
use tauri::{AppHandle, Emitter, Webview};
use tauri::ipc::JavaScriptChannelId;
use std::sync::Mutex;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::{db, dialog};
mod query;
mod datasource;
mod schema;
mod table;
mod report;
mod surrogate;
mod column_type;
mod column;
mod row;
mod cell;
mod file;

#[tauri::command]
/// Initialize a connection to a StaticDB database file.
pub fn init(path: String) -> Result<(), Error> {
    return db::init(path);
}



#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum QueryStream {
    Tables {
        channel: JavaScriptChannelId
    },
    Reports {
        channel: JavaScriptChannelId
    },
    InheritorTables {
        table_oid: i64,
        row_oid: i64,
        channel: JavaScriptChannelId
    },
    MasterSchemas {
        schema_oid: Option<i64>,
        is_table: bool,
        channel: JavaScriptChannelId
    },
    Columns {
        schema_oid: i64,
        channel: JavaScriptChannelId
    },
    RootDatasources {
        channel: JavaScriptChannelId
    },
    LinkedDatasources {
        parent_datasource: datasource::Datasource,
        channel: JavaScriptChannelId
    },
    ColumnAssociatedTables {
        channel: JavaScriptChannelId
    },
    ColumnValues {
        schema_oid: i64,
        channel: JavaScriptChannelId
    },
    Cells {
        schema_oid: i64,
        filters: Vec<(String, i64)>,
        limit: cell::RetrievalLimit,
        column_channel: JavaScriptChannelId,
        cell_channel: JavaScriptChannelId
    }
}

impl QueryStream {
    /// Sends data through a channel from the database to the frontend.
    pub fn send(self, webview: Webview) -> Result<(), Error> {
        match self {
            Self::Tables { channel} => 
                schema::HierarchicalListItemMetadata::query_tables(Sender::Channel(channel.channel_on(webview))),
            Self::Reports { channel} => 
                schema::HierarchicalListItemMetadata::query_reports(Sender::Channel(channel.channel_on(webview))),

            Self::InheritorTables { channel, table_oid, row_oid } => 
                schema::SelectedHierarchicalListItemMetadata::query_inheritor_tables(Sender::Channel(channel.channel_on(webview)), table_oid, row_oid),

            Self::MasterSchemas { schema_oid, is_table, channel } => 
                schema::ToggledHierarchicalListItemMetadata::query_master_schemas(Sender::Channel(channel.channel_on(webview)), schema_oid, is_table),

            Self::Columns { schema_oid, channel } => 
                column::FullMetadata::query_by_schema(Sender::Channel(channel.channel_on(webview)), schema_oid),

            Self::RootDatasources { channel } =>
                datasource::Datasource::query_roots(Sender::Channel(channel.channel_on(webview))),
            Self::LinkedDatasources { parent_datasource, channel } => 
                parent_datasource.query_links(Sender::Channel(channel.channel_on(webview))),
            Self::ColumnAssociatedTables { channel } => 
                column::FullMetadata::query_associated_tables(Sender::Channel(channel.channel_on(webview))),
            Self::ColumnValues { schema_oid, channel } => 
                column::FullMetadata::query_values(Sender::Channel(channel.channel_on(webview)), schema_oid),
                
            Self::Cells { schema_oid, filters, limit, column_channel, cell_channel } =>
                cell::Cell::query_by_schema(
                    Sender::Channel(column_channel.channel_on(webview.clone())), 
                    Sender::Channel(cell_channel.channel_on(webview)), 
                    schema_oid, 
                    filters,
                    limit
                )
        }
    }
}

#[tauri::command]
/// Sends data through a channel from the backend to the frontend.
pub fn query(webview: Webview, query: QueryStream) -> Result<(), Error> {
    query.send(webview)
}



#[tauri::command]
/// Gets the metadata for a table.
pub fn get_table_metadata(table_oid: i64) -> Result<table::FullMetadata, Error> {
    table::FullMetadata::get(table_oid)
}

#[tauri::command]
/// Gets the metadata for a report.
pub fn get_report_metadata(report_oid: i64) -> Result<report::FullMetadata, Error> {
    report::FullMetadata::get(report_oid)
}

#[tauri::command]
/// Get the metadata for a particular column in a table.
pub fn get_column(column_oid: i64) -> Result<column::FullMetadata, Error> {
    column::FullMetadata::get(column_oid)
}

#[tauri::command]
pub fn get_cell(cell_oid: cell::CellOid) -> Result<cell::Cell, Error> {
    cell::Cell::get(cell_oid)
}

#[tauri::command]
pub fn get_file_base64(file_oid: i64) -> Result<String, Error> {
    let file: file::File = file::File::get(file_oid)?;
    file.into_base64()
}

#[tauri::command]
pub fn download_file(file_oid: i64, download_to_path: String) -> Result<(), Error> {
    let file: file::File = file::File::get(file_oid)?;
    file.download(download_to_path)
}

#[tauri::command]
pub fn upload_file(mut file: file::File, upload_from_path: String) -> Result<i64, Error> {
    file.upload(upload_from_path)?;
    Ok(match file { file::File::Path { oid, .. } | file::File::Blob { oid } => oid})
}



#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Action {
    CreateTable(table::FullMetadata),
    EditTable(table::FullMetadata),
    CreateReport(report::FullMetadata),
    EditReport(report::FullMetadata),
    TrashSchema(i64),
    UntrashSchema(i64),

    CreateColumn(column::FullMetadata),
    EditColumn(column::FullMetadata),
    EditColumnStyle {
        metadata: column::FullMetadata,
        new_column_style: String
    },
    EditColumnOrdering {
        metadata: column::FullMetadata,
        new_column_ordering: Option<i64>
    },
    TrashColumn(i64),
    UntrashColumn(i64),
    RestoreColumn {
        trash_column_oid: i64,
        untrash_column_oid: i64
    },

    CreateRow {
        table_oid: i64,
        row_oid: Option<i64>,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>
    },
    TrashRow {
        table_oid: i64,
        row_oid: i64 
    },
    UntrashRow {
        table_oid: i64,
        row_oid: i64
    },
    EditRowSubtype {
        table_oid: i64,
        row_oid: i64,
        inheritor_table_oid: i64
    },

    EditCellContents(cell::Cell)
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

const UPDATE_SCHEMA_SIGNAL: &'static str = "schema";
const UPDATE_TABLE_SIGNAL: &'static str = "table";
const UPDATE_REPORT_SIGNAL: &'static str = "report";

impl Action {
    async fn execute(self, app: &AppHandle, is_forward: bool) -> Result<(), Error> {
        match self {
            Self::CreateTable(mut metadata) => {
                // Create the table
                metadata.create()?;
                record_action(Self::TrashSchema(metadata.schema.oid), is_forward);

                // Send signal to update table
                app.emit(UPDATE_TABLE_SIGNAL, metadata.schema.oid)?;

                // Open new window to view the table
                dialog::dialog_open(app.clone(), dialog::Dialog::Schema { 
                    title: metadata.schema.name, 
                    query_string: format!("schema_oid={}", metadata.schema.oid)
                }).await?;
            }
            Self::EditTable(metadata) => {
                // Update the table
                let old_metadata: table::FullMetadata = table::FullMetadata::get(metadata.schema.oid.clone())?;
                metadata.set()?;
                record_action(Self::EditTable(old_metadata), is_forward);

                // Send signal to update table
                app.emit(UPDATE_TABLE_SIGNAL, metadata.schema.oid)?;
            }
            Self::CreateReport(mut metadata) => {
                // Create the report
                metadata.create()?;
                record_action(Self::TrashSchema(metadata.schema.oid), is_forward);

                // Send signal to update report
                app.emit(UPDATE_REPORT_SIGNAL, metadata.schema.oid)?;

                // Open new window to view the report
                dialog::dialog_open(app.clone(), dialog::Dialog::Schema { 
                    title: metadata.schema.name, 
                    query_string: format!("schema_oid={}", metadata.schema.oid)
                }).await?;
            }
            Self::EditReport(metadata) => {
                // Update the report
                let old_metadata: report::FullMetadata = report::FullMetadata::get(metadata.schema.oid.clone())?;
                metadata.set()?;
                record_action(Self::EditReport(old_metadata), is_forward);

                // Send signal to update report
                app.emit(UPDATE_REPORT_SIGNAL, metadata.schema.oid)?;
            }
            Self::TrashSchema(schema_oid) => {
                // Flag the schema for garbage collection
                schema::FullMetadata::trash(schema_oid.clone())?;
                record_action(Self::UntrashSchema(schema_oid), is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, schema_oid)?;
            }
            Self::UntrashSchema(schema_oid) => {
                // Unflag the schema for garbage collection
                schema::FullMetadata::untrash(schema_oid.clone())?;
                record_action(Self::TrashSchema(schema_oid), is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, schema_oid)?;
            }



            Self::CreateColumn(mut metadata) => {
                // Create the column
                metadata.create()?;
                record_action(Self::TrashColumn(metadata.oid), is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, metadata.schema.oid)?;
            }
            Self::EditColumn(mut metadata) => {
                // Update the column
                let old_column_oid: i64 = metadata.oid.clone();
                metadata.set()?;
                record_action(Self::RestoreColumn { 
                    trash_column_oid: metadata.oid, 
                    untrash_column_oid: old_column_oid
                }, is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, metadata.schema.oid)?;
            }
            Self::EditColumnStyle { mut metadata, new_column_style } => {
                // Update the column style
                let old_column_style: String = metadata.style.clone();
                metadata.set_style(new_column_style)?;
                record_action(Self::EditColumnStyle { 
                    metadata: metadata.clone(), 
                    new_column_style: old_column_style
                }, is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, metadata.schema.oid)?;
            }
            Self::EditColumnOrdering { mut metadata, new_column_ordering } => {
                // Update the column style
                let old_column_ordering: i64 = metadata.ordering.clone();
                metadata.set_ordering(new_column_ordering)?;
                record_action(Self::EditColumnOrdering { 
                    metadata: metadata.clone(), 
                    new_column_ordering: Some(old_column_ordering)
                }, is_forward);

                // Send signal to update schema
                app.emit(UPDATE_SCHEMA_SIGNAL, metadata.schema.oid)?;
            }
            Self::TrashColumn(column_oid) => {
                // Flag the column for garbage collection
                column::FullMetadata::trash(column_oid.clone())?;
                record_action(Self::UntrashColumn(column_oid), is_forward);

                // Send signal to update schema
                // TODO
            }
            Self::UntrashColumn(column_oid) => {
                // Unflag the column for garbage collection
                column::FullMetadata::untrash(column_oid.clone())?;
                record_action(Self::TrashColumn(column_oid), is_forward);

                // Send signal to update schema
                // TODO
            }
            Self::RestoreColumn { trash_column_oid, untrash_column_oid } => {
                // Unflag the old column for garbage collection, and flag the new column in its place
                column::FullMetadata::trash_and_untrash(untrash_column_oid.clone(), trash_column_oid.clone())?;
                record_action(Self::RestoreColumn { 
                    trash_column_oid: untrash_column_oid, 
                    untrash_column_oid: trash_column_oid 
                }, is_forward);

                // Send signal to update schema
                // TODO
            }



            Self::CreateRow { table_oid, row_oid, fixed_parent_datasource } => {
                // Create the row
                let row_oid: i64 = row::insert(table_oid, row_oid, fixed_parent_datasource)?;
                record_action(Self::TrashRow {
                    table_oid,
                    row_oid
                }, is_forward);

                // Send signal to update table
                app.emit(UPDATE_TABLE_SIGNAL, table_oid)?;
            }
            Self::TrashRow { table_oid, row_oid } => {
                if let Some((table_oid, row_oid)) = row::trash(table_oid, row_oid)? {
                    record_action(Self::UntrashRow {
                        table_oid,
                        row_oid
                    }, is_forward);

                    // Send signal to update table
                    app.emit(UPDATE_TABLE_SIGNAL, table_oid)?;
                }
            }
            Self::UntrashRow { table_oid, row_oid } => {
                row::untrash(table_oid, row_oid)?;
                record_action(Self::TrashRow {
                    table_oid,
                    row_oid
                }, is_forward);

                // Send signal to update table
                app.emit(UPDATE_TABLE_SIGNAL, table_oid)?;
            }
            Self::EditRowSubtype { table_oid, row_oid, inheritor_table_oid } => {
                let old_inheritor_table_oid: i64 = row::change_object_type(table_oid, row_oid, inheritor_table_oid)?;
                record_action(Self::EditRowSubtype { 
                    table_oid, 
                    row_oid, 
                    inheritor_table_oid: old_inheritor_table_oid 
                }, is_forward);

                // Send signal to update table
                app.emit(UPDATE_TABLE_SIGNAL, table_oid)?;
            }



            Self::EditCellContents(cell) => {
                let execution_result: Result<(), Error> = {
                    // Update the contents of the cell
                    match cell.get_cell_oid() {
                        Ok(cell_oid) => {
                            match cell::Cell::get(cell_oid) {
                                Ok(old_cell) => {
                                    match cell.set() {
                                        Ok(_) => {
                                            record_action(Self::EditCellContents(old_cell), is_forward);
                                            Ok(())
                                        }
                                        Err(e) => Err(e)
                                    }
                                }
                                Err(e) => Err(e)
                            }
                        }
                        Err(e) => Err(e)
                    }
                };

                // Send signal to update that cell + any dependent cells
                // Do this regardless of whether previous execution succeeded or failed
                cell.get_value_oid()?.query_affected_cells(app)?;
                
                // Throw error if execution failed
                if let Err(e) = execution_result {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

#[tauri::command]
/// Executes an action that affects the state of the database.
pub async fn execute(app: AppHandle, action: Action) -> Result<(), Error> {
    // Do something that affects the database
    action.execute(&app, true).await?;

    // Clear the stack of undone actions
    let mut forward_stack = FORWARD_STACK.lock().unwrap();
    *forward_stack = Vec::new();
    return Ok(());
}

/// Undoes the last action by popping the top of the reverse stack.
pub async fn undo(app: &AppHandle) -> Result<(), Error> {
    // Get the action from the top of the stack
    match {
        let mut reverse_stack = REVERSE_STACK.lock().unwrap();
        (*reverse_stack).pop()
    } {
        Some(reverse_action) => {
            reverse_action.execute(app, false).await?;
        }
        None => {}
    }
    return Ok(());
}

/// Redoes the last undone action by popping the top of the forward stack.
pub async fn redo(app: &AppHandle) -> Result<(), Error> {
    // Get the action from the top of the stack
    match {
        let mut forward_stack = FORWARD_STACK.lock().unwrap();
        (*forward_stack).pop()
    } {
        Some(forward_action) => {
            forward_action.execute(app, true).await?;
        }
        None => {}
    }
    return Ok(());
}
