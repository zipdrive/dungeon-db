use crate::data::schema::UPDATE_SCHEMA_SIGNAL;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::{db, dialog, process};
use rocket::serde::json::Json;
use serde::Deserialize;
use std::sync::Mutex;
use tauri::ipc::JavaScriptChannelId;
use tauri::{AppHandle, Emitter, Manager, Webview};
use tauri_plugin_dialog::DialogExt;
mod cell;
mod column;
mod column_type;
mod datasource;
mod export;
mod file;
mod report;
mod row;
mod schema;
mod table;
mod view;

fn reset(app: &AppHandle) -> Result<(), Error> {
    // Close all dialogs
    for (_, subwindow) in app.webview_windows().iter() {
        if subwindow.label() != "main" {
            subwindow.close()?;
        }
    }

    // Emit that schemas have changed
    app.emit(UPDATE_SCHEMA_SIGNAL, Vec::<i64>::new())?;
    Ok(())
}

#[tauri::command]
/// Create a new DungeonDB database file.
pub fn init_new(app: AppHandle) -> Result<(), Error> {
    // Create a new DungeonDB database file
    db::init_new()?;

    // Reset the window
    reset(&app)?;
    Ok(())
}

#[tauri::command]
/// Initialize a connection to an existing DungeonDB database file.
pub fn init_existing(app: AppHandle, path: String) -> Result<(), Error> {
    // Initialize a connection to an existing DungeonDB database file.
    db::init_existing(path)?;

    // Reset the app
    reset(&app)?;
    Ok(())
}

#[tauri::command]
/// Save to the main file being worked on.
pub fn save(app: AppHandle) -> Result<(), Error> {
    save_shortcut(&app)
}

/// Save to the main file being worked on.
pub fn save_shortcut(app: &AppHandle) -> Result<(), Error> {
    println!("Command: SAVE");
     // Save to main file, then clean database
    if db::save_to_current_file(app)? {
        // Record that there are no changes since the last save
        let mut has_unsaved_changes = HAS_UNSAVED_CHANGES.lock().unwrap();
        *has_unsaved_changes = false;
    }
    Ok(())
}

#[tauri::command]
/// Save to a prompted file.
pub fn save_as(app: AppHandle) -> Result<(), Error> {
    println!("Command: SAVE AS");
    // Save to prompted main file, then clean database
    if db::save_to_prompted_file(&app)? {
        // Record that there are no changes since the last save
        let mut has_unsaved_changes = HAS_UNSAVED_CHANGES.lock().unwrap();
        *has_unsaved_changes = false;
    }
    Ok(())
}

#[tauri::command]
/// Prompt for a DungeonDB file to load, then load it.
pub fn load(app: AppHandle) -> Result<(), Error> {
    app
        .dialog()
        .file()
        .add_filter("DungeonDB File (*.dndb)", &["dndb"])
        .pick_file(|path| {
            if let Some(path) = path {
                match init_existing(app, path.to_string()) {
                    Ok(_) => {},
                    Err(e) => {
                        todo!("Do something with the error.")
                    }
                }
            }
        });
    Ok(())
}

/// Check if the autosave has changes that have not been saved.
pub fn has_unsaved_changes() -> bool {
    let has_unsaved_changes = HAS_UNSAVED_CHANGES.lock().unwrap();
    (*has_unsaved_changes).clone()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum QueryStream {
    Tables {
        channel: JavaScriptChannelId,
    },
    Reports {
        channel: JavaScriptChannelId,
    },
    InheritorTables {
        table_oid: i64,
        row_oid: i64,
        channel: JavaScriptChannelId,
    },
    MasterSchemas {
        schema_oid: Option<i64>,
        is_table: bool,
        channel: JavaScriptChannelId,
    },
    Columns {
        schema_oid: i64,
        channel: JavaScriptChannelId,
    },
    RootDatasources {
        channel: JavaScriptChannelId,
    },
    LinkedDatasources {
        parent_datasource: datasource::Datasource,
        channel: JavaScriptChannelId,
    },
    Parameters {
        parent_datasource: datasource::Datasource,
        channel: JavaScriptChannelId,
    },
    ColumnAssociatedTables {
        channel: JavaScriptChannelId,
    },
    ColumnAssociatedReports {
        channel: JavaScriptChannelId,
    },
    Cells {
        schema_oid: i64,
        filters: Vec<(String, i64)>,
        limit: cell::RetrievalLimit,
        column_channel: JavaScriptChannelId,
        cell_channel: JavaScriptChannelId,
    },

    TableRowLabels {
        table_oid: i64,
        processid: i64
    }
}

impl QueryStream {
    /// Sends data through a channel from the database to the frontend.
    pub fn send(self, app: AppHandle, webview: Webview) -> Result<(), Error> {
        match self {
            Self::Tables { channel } => schema::HierarchicalListItemMetadata::query_tables(
                Sender::Channel(channel.channel_on(webview)),
            ),
            Self::Reports { channel } => schema::HierarchicalListItemMetadata::query_reports(
                Sender::Channel(channel.channel_on(webview)),
            ),

            Self::InheritorTables {
                channel,
                table_oid,
                row_oid,
            } => schema::SelectedHierarchicalListItemMetadata::query_inheritor_tables(
                Sender::Channel(channel.channel_on(webview)),
                table_oid,
                row_oid,
            ),

            Self::MasterSchemas {
                schema_oid,
                is_table,
                channel,
            } => schema::ToggledHierarchicalListItemMetadata::query_master_schemas(
                Sender::Channel(channel.channel_on(webview)),
                schema_oid,
                is_table,
            ),

            Self::Columns {
                schema_oid,
                channel,
            } => column::FullMetadata::query_by_schema(
                Sender::Channel(channel.channel_on(webview)),
                schema_oid,
            ),

            Self::RootDatasources { channel } => {
                datasource::Datasource::query_roots(Sender::Channel(channel.channel_on(webview)))
            }
            Self::LinkedDatasources {
                parent_datasource,
                channel,
            } => parent_datasource.query_links(Sender::Channel(channel.channel_on(webview))),
            Self::Parameters {
                parent_datasource,
                channel,
            } => parent_datasource.query_parameters(Sender::Channel(channel.channel_on(webview))),

            Self::ColumnAssociatedTables { channel } => {
                column::FullMetadata::query_associated_tables(Sender::Channel(
                    channel.channel_on(webview),
                ))
            }
            Self::ColumnAssociatedReports { channel } => {
                column::FullMetadata::query_associated_reports(Sender::Channel(
                    channel.channel_on(webview),
                ))
            },

            Self::Cells {
                schema_oid,
                filters,
                limit,
                column_channel,
                cell_channel,
            } => cell::SchemaCellStream::query_by_schema(
                Sender::Channel(column_channel.channel_on(webview.clone())),
                Sender::Channel(cell_channel.channel_on(webview)),
                schema_oid,
                filters,
                limit,
            ),

            Self::TableRowLabels { 
                table_oid, 
                processid 
            } => {
                tauri::async_runtime::spawn_blocking(move || {
                    table::DropdownValue::emit_table_row_labels(
                        app, 
                        processid, 
                        table_oid
                    )
                });
                Ok(())
            }
        }
    }
}

#[tauri::command]
/// Sends data through a channel from the backend to the frontend.
pub fn query(app: AppHandle, webview: Webview, query: QueryStream) -> Result<(), Error> {
    query.send(app, webview)
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
/// Gets the metadata for a schema.
pub fn get_schema_metadata(schema_oid: i64) -> Result<schema::Schema, Error> {
    schema::Schema::get(schema_oid)
}

#[tauri::command]
/// Get the metadata for a particular column in a table.
pub fn get_column(column_oid: i64) -> Result<column::FullMetadata, Error> {
    column::FullMetadata::get(column_oid)
}

#[tauri::command]
pub fn get_cell(cell_identifier: cell::CellIdentifier) -> cell::Cell {
    cell::Cell::get(cell_identifier)
}

#[tauri::command]
pub fn get_image_src(file: file::File) -> Result<String, Error> {
    file.get_image_src()
}

#[tauri::command]
pub fn download_file(file_oid: i64, download_to_path: String) -> Result<(), Error> {
    let file: file::File = file::File::get(file_oid)?;
    file.download(download_to_path)
}

#[tauri::command]
pub fn upload_file(mut file: file::File, upload_from_path: String) -> Result<i64, Error> {
    file.upload(upload_from_path)?;
    Ok(match file {
        file::File::Path { oid, .. } | file::File::Blob { oid } => oid,
    })
}

#[tauri::command] 
pub fn get_processid() -> i64 {
    process::get_processid()
}

#[tauri::command]
pub fn get_table_row_labels(app: AppHandle, processid: i64, table_oid: i64) {
    
}



#[derive(Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
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
        new_column_style: String,
    },
    EditColumnOrdering {
        metadata: column::FullMetadata,
        new_column_ordering: Option<i64>,
    },
    TrashColumn {
        schema_oid: i64,
        column_oid: i64,
    },
    UntrashColumn {
        schema_oid: i64,
        column_oid: i64,
    },
    RestoreColumn {
        schema_oid: i64,
        trash_column_oid: i64,
        untrash_column_oid: i64,
    },

    CreateRow {
        table_oid: i64,
        row_oid: Option<i64>,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
    },
    EditRowOid {
        table_oid: i64,
        row_oid: i64,
        new_row_oid: Option<i64>,
    },
    TrashRow {
        table_oid: i64,
        row_oid: i64,
    },
    UntrashRow {
        table_oid: i64,
        row_oid: i64,
    },
    EditRowSubtype {
        table_oid: i64,
        row_oid: i64,
        inheritor_table_oid: i64,
    },

    EditCellContents(cell::DataCellEntry),
}

static REVERSE_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());
static FORWARD_STACK: Mutex<Vec<Action>> = Mutex::new(Vec::new());
static HAS_UNSAVED_CHANGES: Mutex<bool> = Mutex::new(false);

/// Records the opposite action to the one that was just performed, for undo/redo purposes.
fn record_action(action: Action, is_forward: bool) {
    {
        let mut reverse_stack = if is_forward {
            REVERSE_STACK.lock().unwrap()
        } else {
            FORWARD_STACK.lock().unwrap()
        };
        (*reverse_stack).push(action);
    }
    {
        let mut has_unsaved_changes = HAS_UNSAVED_CHANGES.lock().unwrap();
        *has_unsaved_changes = true;
    }
}

impl Action {
    async fn execute(self, app: &AppHandle, is_forward: bool) -> Result<(), Error> {
        match self {
            Self::CreateTable(mut metadata) => {
                // Create the table
                metadata.create()?;
                record_action(Self::TrashSchema(metadata.schema.oid), is_forward);

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;

                // Open new window to view the table
                dialog::dialog_open(
                    app.clone(),
                    dialog::Dialog::Schema {
                        title: metadata.schema.name,
                        query_string: format!("schema_oid={}", metadata.schema.oid),
                    },
                )
                .await?;
            }
            Self::EditTable(metadata) => {
                // Update the table
                let old_metadata: table::FullMetadata =
                    table::FullMetadata::get(metadata.schema.oid.clone())?;
                metadata.set()?;
                record_action(Self::EditTable(old_metadata), is_forward);

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::CreateReport(mut metadata) => {
                // Create the report
                metadata.create()?;
                record_action(Self::TrashSchema(metadata.schema.oid), is_forward);

                // Send signal to update report
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;

                // Open new window to view the report
                dialog::dialog_open(
                    app.clone(),
                    dialog::Dialog::Schema {
                        title: metadata.schema.name,
                        query_string: format!("schema_oid={}", metadata.schema.oid),
                    },
                )
                .await?;
            }
            Self::EditReport(metadata) => {
                // Update the report
                let old_metadata: report::FullMetadata =
                    report::FullMetadata::get(metadata.schema.oid.clone())?;
                metadata.set()?;
                record_action(Self::EditReport(old_metadata), is_forward);

                // Send signal to update report
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::TrashSchema(schema_oid) => {
                // Flag the schema for garbage collection
                schema::FullMetadata::trash(schema_oid.clone())?;
                record_action(Self::UntrashSchema(schema_oid), is_forward);

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }
            Self::UntrashSchema(schema_oid) => {
                // Unflag the schema for garbage collection
                schema::FullMetadata::untrash(schema_oid.clone())?;
                record_action(Self::TrashSchema(schema_oid), is_forward);

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }

            Self::CreateColumn(mut metadata) => {
                // Create the column
                metadata.create()?;
                record_action(
                    Self::TrashColumn {
                        schema_oid: metadata.schema.oid.clone(),
                        column_oid: metadata.oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::EditColumn(mut metadata) => {
                // Update the column
                let old_column_oid: i64 = metadata.oid.clone();
                metadata.set()?;
                record_action(
                    Self::RestoreColumn {
                        schema_oid: metadata.schema.oid.clone(),
                        trash_column_oid: metadata.oid,
                        untrash_column_oid: old_column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                app.emit("column", (old_column_oid, metadata))?;
            }
            Self::EditColumnStyle {
                mut metadata,
                new_column_style,
            } => {
                // Update the column style
                let old_column_style: String = metadata.style.clone();
                metadata.set_style(new_column_style)?;
                record_action(
                    Self::EditColumnStyle {
                        metadata: metadata.clone(),
                        new_column_style: old_column_style,
                    },
                    is_forward,
                );

                // Send signal to update schema
                app.emit("column", (metadata.oid, metadata))?;
            }
            Self::EditColumnOrdering {
                mut metadata,
                new_column_ordering,
            } => {
                // Update the column style
                let old_column_ordering: i64 = metadata.ordering.clone();
                metadata.set_ordering(new_column_ordering)?;
                record_action(
                    Self::EditColumnOrdering {
                        metadata: metadata.clone(),
                        new_column_ordering: Some(old_column_ordering),
                    },
                    is_forward,
                );

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::TrashColumn {
                schema_oid,
                column_oid,
            } => {
                // Flag the column for garbage collection
                column::FullMetadata::trash(column_oid.clone())?;
                record_action(
                    Self::UntrashColumn {
                        schema_oid: schema_oid.clone(),
                        column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }
            Self::UntrashColumn {
                schema_oid,
                column_oid,
            } => {
                // Unflag the column for garbage collection
                column::FullMetadata::untrash(column_oid.clone())?;
                record_action(
                    Self::TrashColumn {
                        schema_oid: schema_oid.clone(),
                        column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }
            Self::RestoreColumn {
                schema_oid,
                trash_column_oid,
                untrash_column_oid,
            } => {
                // Unflag the old column for garbage collection, and flag the new column in its place
                column::FullMetadata::trash_and_untrash(
                    untrash_column_oid.clone(),
                    trash_column_oid.clone(),
                )?;
                record_action(
                    Self::RestoreColumn {
                        schema_oid: schema_oid.clone(),
                        trash_column_oid: untrash_column_oid,
                        untrash_column_oid: trash_column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }

            Self::CreateRow {
                table_oid,
                row_oid,
                fixed_parent_datasource,
            } => {
                // Create the row
                let row_oid: i64 = row::insert(table_oid, row_oid, fixed_parent_datasource)?;
                record_action(Self::TrashRow { table_oid, row_oid }, is_forward);

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::EditRowOid {
                table_oid,
                row_oid,
                new_row_oid,
            } => {
                let new_row_oid: i64 = row::reorder(table_oid, row_oid, new_row_oid)?;
                record_action(
                    Self::EditRowOid {
                        table_oid,
                        row_oid: new_row_oid,
                        new_row_oid: Some(row_oid),
                    },
                    is_forward,
                );

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::TrashRow { table_oid, row_oid } => {
                if let Some((table_oid, row_oid)) = row::trash(table_oid, row_oid)? {
                    record_action(Self::UntrashRow { table_oid, row_oid }, is_forward);

                    // Send signal to update table
                    schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
                }
            }
            Self::UntrashRow { table_oid, row_oid } => {
                row::untrash(table_oid, row_oid)?;
                record_action(Self::TrashRow { table_oid, row_oid }, is_forward);

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::EditRowSubtype {
                table_oid,
                row_oid,
                inheritor_table_oid,
            } => {
                let old_inheritor_table_oid: i64 =
                    row::change_object_type(table_oid, row_oid, inheritor_table_oid)?;
                record_action(
                    Self::EditRowSubtype {
                        table_oid,
                        row_oid,
                        inheritor_table_oid: old_inheritor_table_oid,
                    },
                    is_forward,
                );

                // Send signal to update table
                schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }

            Self::EditCellContents(cell) => {
                let execution_result: Result<(), Error> = {
                    // Update the contents of the cell
                    match cell.set() {
                        Ok(old_cell) => {
                            record_action(Self::EditCellContents(old_cell), is_forward);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };

                // Send signal to update that cell + any dependent cells
                // Do this regardless of whether previous execution succeeded or failed
                app.emit("cell", cell)?;

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

#[tauri::command]
/// Undoes the last action by popping the top of the reverse stack.
pub async fn undo(app: AppHandle) -> Result<(), Error> {
    // Get the action from the top of the stack
    match {
        let mut reverse_stack = REVERSE_STACK.lock().unwrap();
        (*reverse_stack).pop()
    } {
        Some(reverse_action) => {
            reverse_action.execute(&app, false).await?;
        }
        None => {}
    }
    return Ok(());
}

#[tauri::command]
/// Redoes the last undone action by popping the top of the forward stack.
pub async fn redo(app: AppHandle) -> Result<(), Error> {
    // Get the action from the top of the stack
    match {
        let mut forward_stack = FORWARD_STACK.lock().unwrap();
        (*forward_stack).pop()
    } {
        Some(forward_action) => {
            forward_action.execute(&app, true).await?;
        }
        None => {}
    }
    return Ok(());
}
