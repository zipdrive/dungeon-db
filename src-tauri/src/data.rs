use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use serde::Deserialize;
use std::sync::Mutex;
use tauri::ipc::JavaScriptChannelId;
use tauri::{AppHandle, Emitter, Manager, Webview};
use tauri_plugin_dialog::DialogExt;
mod file;
mod table;
mod report;
mod formula;

fn reset(app: &AppHandle) -> Result<(), Error> {
    // Close all dialogs
    for (_, subwindow) in app.webview_windows().iter() {
        if subwindow.label() != "main" {
            subwindow.close()?;
        }
    }

    // Emit that schemas have changed
    //app.emit(UPDATE_SCHEMA_SIGNAL, Vec::<i64>::new())?;
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
    /// Emits a basic stream of all tables.
    Tables {
        channel: JavaScriptChannelId,
    },
    /// Emits a basic stream of all reports.
    Reports {
        channel: JavaScriptChannelId,
    },

    /// Emits a basic stream of all options for masters to a table.
    TableMasters {
        table_oid: Option<i64>,
        channel: JavaScriptChannelId,
    },


    /// Emits a basic stream of all columns directly owned by or inherited by a table.
    TableColumns {
        table_oid: i64,
        channel: JavaScriptChannelId
    },
    /// Emits a basic stream of all columns belonging to a report.
    ReportColumns {
        report_oid: i64,
        channel: JavaScriptChannelId
    },

    /// Emits a stream of all columns directly owned by or inherited by a table, and the rows of the table.
    TableCells {
        table_oid: i64,
        column_channel: JavaScriptChannelId,
        row_channel: JavaScriptChannelId,
    },
    /// Emits a stream of all columns belonging to a report, and the rows of the report.
    ReportCells {
        report_oid: i64,
        column_channel: JavaScriptChannelId,
        row_channel: JavaScriptChannelId,
    },

    /// Emits a basic stream of labels for each row in a table.
    TableRowLabels {
        table_oid: i64,
        channel: JavaScriptChannelId
    }
}

impl QueryStream {
    /// Sends data through a channel from the database to the frontend.
    pub fn send(self, app: AppHandle, webview: Webview) -> Result<(), Error> {
        match self {
            Self::Tables { channel } => table::TableListItem::send_all(
                Sender::Channel(channel.channel_on(webview)),
            ),

            Self::Reports { channel } => report::ReportListItem::send_all(
                Sender::Channel(channel.channel_on(webview)),
            ),

            Self::TableMasters {
                table_oid,
                channel,
            } => table::TableListItem::send_masters(
                Sender::Channel(channel.channel_on(webview)),
                table_oid,
            ),

            Self::TableColumns {
                table_oid,
                channel,
            } => table::column::TableColumnListItem::send_all(
                Sender::Channel(channel.channel_on(webview)),
                table_oid,
            ),

            Self::ReportColumns { 
                report_oid, 
                channel
            } => report::column::ReportColumnListItem::send_all(
                Sender::Channel(channel.channel_on(webview)), 
                report_oid
            ),

            Self::TableCells {
                table_oid,
                column_channel,
                row_channel,
            } => table::row::TableRow::send(
                Sender::Channel(column_channel.channel_on(webview.clone())),
                Sender::Channel(row_channel.channel_on(webview)),
                table_oid,
            ),

            Self::ReportCells { 
                report_oid, 
                column_channel, 
                row_channel
            } => report::row::ReportRow::send(
                Sender::Channel(column_channel.channel_on(webview.clone())),
                Sender::Channel(row_channel.channel_on(webview)), 
                report_oid
            ),

            Self::TableRowLabels { 
                table_oid, 
                channel 
            } => table::row::TableRowLabel::send_all(
                Sender::Channel(channel.channel_on(webview)), 
                table_oid
            )
        }
    }
}

#[tauri::command]
/// Sends data through a channel from the backend to the frontend.
pub async fn query(app: AppHandle, webview: Webview, query: QueryStream) -> Result<(), Error> {
    query.send(app, webview)
}

#[tauri::command]
/// Gets the metadata for a table.
pub fn get_table_metadata(table_oid: i64) -> Result<table::TableMetadata, Error> {
    table::TableMetadata::get(table_oid)
}

#[tauri::command]
/// Gets the metadata for a table column.
pub fn get_table_column_metadata(column_oid: i64) -> Result<table::column::TableColumnMetadata, Error> {
    let (_, column_metadata) = table::column::TableColumnMetadata::get(column_oid)?;
    Ok(column_metadata)
}

#[tauri::command]
/// Gets the label for an Object.
pub async fn get_object_label(table_oid: i64, row_oid: i64) -> Result<String, Error> {
    table::label::get_object_label(table_oid, row_oid)
}

#[tauri::command]
/// Gets the metadata for a report.
pub fn get_report_metadata(report_oid: i64) -> Result<report::ReportMetadata, Error> {
    report::ReportMetadata::get(report_oid)
}

#[tauri::command]
/// Gets the columns and cells of a row in a table.
pub fn get_row(table_oid: i64, row_oid: i64) -> Result<(Vec<(i64, table::column::TableColumnMetadata)>, table::row::TableRow), Error> {
    table::row::TableRow::get(table_oid, row_oid)
}

#[tauri::command]
/// Gets the table, columns, and cells of an Object.
pub fn get_object_row(table_oid: i64, row_oid: i64) -> Result<(i64, Vec<(i64, table::column::TableColumnMetadata)>, table::row::TableRow), Error> {
    table::row::TableRow::get_object(table_oid, row_oid)
}



#[tauri::command]
/// Gets the content of a file as a base64 string.
pub async fn get_src(file: file::File) -> Result<String, Error> {
    file.get_src()
}

#[tauri::command]
/// Downloads a file.
pub async fn download_file(file_oid: i64, download_to_path: String) -> Result<(), Error> {
    let file: file::File = file::File::get(file_oid)?;
    file.download(download_to_path)
}

#[tauri::command]
/// Uploads a file.
pub async fn upload_file(mut file: file::File, upload_from_path: String) -> Result<i64, Error> {
    file.upload(upload_from_path)?;
    Ok(*file.oid())
}



#[derive(Deserialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Action {
    CreateTable(table::TableMetadata),
    EditTable(table::TableMetadata),
    TrashTable(i64),
    UntrashTable(i64),

    CreateReport(report::ReportMetadata),
    EditReport(report::ReportMetadata),
    TrashReport(i64),
    UntrashReport(i64),

    CreateTableColumn {
        table_oid: i64,
        metadata: table::column::TableColumnMetadata,
        ordering: Option<i64>
    },
    ReplaceTableColumn {
        old_metadata: table::column::TableColumnMetadata,
        new_metadata: table::column::TableColumnMetadata
    },
    EditTableColumnMetadata {
        metadata: table::column::TableColumnMetadata,
    },
    EditTableColumnOrdering {
        metadata: table::column::TableColumnMetadata,
        ordering: Option<i64>
    },
    TrashTableColumn {
        column_oid: i64,
    },
    UntrashTableColumn {
        column_oid: i64,
    },
    RestoreTableColumn {
        table_oid: i64,
        trash_column_oid: i64,
        untrash_column_oid: i64,
    },

    CreateReportColumn {
        report_oid: i64,
        metadata: report::column::ReportColumnMetadata,
        ordering: Option<i64>
    },
    ReplaceReportColumn {
        old_metadata: report::column::ReportColumnMetadata,
        new_metadata: report::column::ReportColumnMetadata
    },
    EditReportColumnMetadata {
        metadata: report::column::ReportColumnMetadata,
    },
    EditReportColumnOrdering {
        metadata: report::column::ReportColumnMetadata,
        ordering: Option<i64>
    },
    TrashReportColumn {
        report_oid: i64,
        column_oid: i64,
    },
    UntrashReportColumn {
        report_oid: i64,
        column_oid: i64,
    },
    RestoreReportColumn {
        report_oid: i64,
        trash_column_oid: i64,
        untrash_column_oid: i64,
    },

    CreateTableRow {
        table_oid: i64,
        row_oid: Option<i64>,
        fixed_parent_datasource: Option<(i64, i64, table::column::TableColumnMetadata)>,
    },
    EditTableRowOid {
        table_oid: i64,
        row_oid: i64,
        new_row_oid: Option<i64>,
    },
    TrashTableRow {
        table_oid: i64,
        row_oid: i64,
    },
    UntrashTableRow {
        table_oid: i64,
        row_oid: i64,
    },
    EditTableRowSubtype {
        table_oid: i64,
        row_oid: i64,
        inheritor_table_oid: i64,
    },

    EditTableCellContents(table::row::TableCell),
    CreateObject {
        table_oid: i64,
        column_oid: i64,
        row_oid: i64,
        object_table_oid: i64
    }
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
                record_action(Self::TrashTable(metadata.oid), is_forward);

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::EditTable(metadata) => {
                // Update the table
                let old_metadata: table::TableMetadata = table::TableMetadata::get(metadata.oid.clone())?;
                metadata.set_metadata()?;
                record_action(Self::EditTable(old_metadata), is_forward);

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::TrashTable(table_oid) => {
                // Trash the table
                table::TableMetadata::trash(table_oid)?;
                record_action(Self::UntrashTable(table_oid), is_forward);

                // Send signal to update table
            }
            Self::UntrashTable(table_oid) => {
                // Trash the table
                table::TableMetadata::untrash(table_oid)?;
                record_action(Self::TrashTable(table_oid), is_forward);

                // Send signal to update table
            }


            Self::CreateReport(mut metadata) => {
                // Create the report
                metadata.create()?;
                record_action(Self::TrashReport(metadata.oid), is_forward);

                // Send signal to update report
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::EditReport(metadata) => {
                // Update the report
                let old_metadata: report::ReportMetadata = report::ReportMetadata::get(metadata.oid.clone())?;
                metadata.set_metadata()?;
                record_action(Self::EditReport(old_metadata), is_forward);

                // Send signal to update report
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::TrashReport(report_oid) => {
                // Trash the table
                report::ReportMetadata::trash(report_oid)?;
                record_action(Self::UntrashReport(report_oid), is_forward);

                // Send signal to update report
            }
            Self::UntrashReport(table_oid) => {
                // Trash the table
                report::ReportMetadata::untrash(table_oid)?;
                record_action(Self::TrashReport(table_oid), is_forward);

                // Send signal to update report
            }
            


            Self::CreateTableColumn { 
                table_oid,
                mut metadata,
                ordering
            } => {
                // Create the column
                metadata.create(table_oid.clone())?;
                record_action(
                    Self::TrashTableColumn {
                        column_oid: metadata.oid,
                    },
                    is_forward,
                );

                // Adjust the ordering of the column
                if let Some(ordering) = ordering {
                    let old_ordering = metadata.set_ordering(Some(ordering))?;
                    record_action(
                        Self::EditTableColumnOrdering { 
                            metadata, 
                            ordering: Some(old_ordering)
                        }, 
                        is_forward
                    );
                }

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::ReplaceTableColumn { 
                old_metadata,
                mut new_metadata
            } => {
                // Update the column
                let table_oid: i64 = new_metadata.replace(&old_metadata)?;
                record_action(
                    Self::RestoreTableColumn {
                        table_oid: table_oid.clone(),
                        trash_column_oid: new_metadata.oid,
                        untrash_column_oid: old_metadata.oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::EditTableColumnMetadata { 
                metadata 
            } => {
                // Overwrite the old metadata
                let (_, old_metadata) = table::column::TableColumnMetadata::get(metadata.oid)?;
                let table_oid: i64 = metadata.set_metadata()?;
                record_action(
                    Self::EditTableColumnMetadata {
                        metadata: old_metadata
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::EditTableColumnOrdering {
                mut metadata,
                ordering,
            } => {
                // Update the column style
                let old_column_ordering = metadata.set_ordering(ordering)?;
                record_action(
                    Self::EditTableColumnOrdering {
                        metadata: metadata.clone(),
                        ordering: Some(old_column_ordering),
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![metadata.schema.oid])?;
            }
            Self::TrashTableColumn {
                column_oid,
            } => {
                // Flag the column for garbage collection
                table::column::TableColumnMetadata::trash(column_oid.clone())?;
                record_action(
                    Self::UntrashTableColumn {
                        column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }
            Self::UntrashTableColumn {
                column_oid,
            } => {
                // Unflag the column for garbage collection
                table::column::TableColumnMetadata::untrash(column_oid.clone())?;
                record_action(
                    Self::TrashTableColumn {
                        column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }
            Self::RestoreTableColumn {
                table_oid,
                trash_column_oid,
                untrash_column_oid,
            } => {
                // Unflag the old column for garbage collection, and flag the new column in its place
                table::column::TableColumnMetadata::swap(
                    table_oid.clone(), 
                    trash_column_oid.clone(), 
                    untrash_column_oid.clone()
                )?;
                record_action(
                    Self::RestoreTableColumn {
                        table_oid: table_oid.clone(),
                        trash_column_oid: untrash_column_oid,
                        untrash_column_oid: trash_column_oid,
                    },
                    is_forward,
                );

                // Send signal to update schema
                //schema::FullMetadata::emit_affected_schema(app, vec![schema_oid])?;
            }


            Self::CreateReportColumn { 
                report_oid, 
                mut metadata,
                ordering
            } => {
                // Create the column
                metadata.create(report_oid.clone())?;
                record_action(
                    Self::TrashReportColumn {
                        report_oid: report_oid.clone(),
                        column_oid: metadata.oid,
                    },
                    is_forward,
                );

                // Adjust the ordering of the column
                if let Some(ordering) = ordering {
                    let old_ordering = metadata.set_ordering(Some(ordering))?;
                    record_action(
                        Self::EditReportColumnOrdering { 
                            metadata, 
                            ordering: Some(old_ordering)
                        }, 
                        is_forward
                    );
                }

                // Emit signal to update report
            }
            Self::ReplaceReportColumn { old_metadata, mut new_metadata } => {
                // Update the column
                let report_oid: i64 = new_metadata.replace(&old_metadata)?;
                record_action(
                    Self::RestoreReportColumn {
                        report_oid: report_oid.clone(),
                        trash_column_oid: new_metadata.oid,
                        untrash_column_oid: old_metadata.oid,
                    },
                    is_forward,
                );

                // Emit signal to update report
            }
            Self::EditReportColumnMetadata { metadata } => {
                // Overwrite the old metadata
                let (_, old_metadata) = report::column::ReportColumnMetadata::get(metadata.oid)?;
                metadata.set_metadata()?;
                record_action(
                    Self::EditReportColumnMetadata {
                        metadata: old_metadata
                    },
                    is_forward,
                );

                // Emit signal to update report
            }
            Self::EditReportColumnOrdering { metadata, ordering } => {
                // Update the column style
                let old_column_ordering = metadata.set_ordering(ordering)?;
                record_action(
                    Self::EditReportColumnOrdering {
                        metadata: metadata.clone(),
                        ordering: Some(old_column_ordering),
                    },
                    is_forward,
                );

                // Emit signal to update report
            }
            Self::TrashReportColumn { report_oid, column_oid } => {
                // Flag the column for garbage collection
                report::column::ReportColumnMetadata::trash(report_oid.clone(), column_oid.clone())?;
                record_action(
                    Self::UntrashReportColumn {
                        report_oid: report_oid.clone(),
                        column_oid,
                    },
                    is_forward,
                );

                // Emit signal to update report
            }
            Self::UntrashReportColumn { report_oid, column_oid } => {
                // Unflag the column for garbage collection
                report::column::ReportColumnMetadata::untrash(report_oid.clone(), column_oid.clone())?;
                record_action(
                    Self::TrashReportColumn {
                        report_oid: report_oid.clone(),
                        column_oid,
                    },
                    is_forward,
                );

                // Emit signal to update report
            }
            Self::RestoreReportColumn { report_oid, trash_column_oid, untrash_column_oid } => {
                // Unflag the old column for garbage collection, and flag the new column in its place
                report::column::ReportColumnMetadata::swap(
                    report_oid.clone(), 
                    trash_column_oid.clone(), 
                    untrash_column_oid.clone()
                )?;
                record_action(
                    Self::RestoreReportColumn {
                        report_oid: report_oid.clone(),
                        trash_column_oid: untrash_column_oid,
                        untrash_column_oid: trash_column_oid,
                    },
                    is_forward,
                );

                // Emit signal to update report
            }



            Self::CreateTableRow {
                table_oid,
                row_oid,
                fixed_parent_datasource,
            } => {
                // Create the row
                let row_oid: i64 = table::row::TableRow::insert(table_oid, row_oid)?;
                record_action(
                    Self::TrashTableRow { 
                        table_oid, 
                        row_oid 
                    }, 
                    is_forward
                );

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::EditTableRowOid {
                table_oid,
                row_oid,
                new_row_oid,
            } => {
                let new_row_oid: i64 = table::row::TableRow::reorder(table_oid, row_oid, new_row_oid)?;
                record_action(
                    Self::EditTableRowOid {
                        table_oid,
                        row_oid: new_row_oid,
                        new_row_oid: Some(row_oid),
                    },
                    is_forward,
                );

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::TrashTableRow { table_oid, row_oid } => {
                if let Some((table_oid, row_oid)) = table::row::TableRow::trash(table_oid, row_oid)? {
                    record_action(Self::UntrashTableRow { table_oid, row_oid }, is_forward);

                    // Send signal to update table
                    //schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
                }
            }
            Self::UntrashTableRow { table_oid, row_oid } => {
                table::row::TableRow::untrash(table_oid, row_oid)?;
                record_action(Self::TrashTableRow { table_oid, row_oid }, is_forward);

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }
            Self::EditTableRowSubtype {
                table_oid,
                row_oid,
                inheritor_table_oid,
            } => {
                let old_inheritor_table_oid: i64 =
                    table::row::TableRow::change_object_type(table_oid, row_oid, inheritor_table_oid)?;
                record_action(
                    Self::EditTableRowSubtype {
                        table_oid,
                        row_oid,
                        inheritor_table_oid: old_inheritor_table_oid,
                    },
                    is_forward,
                );

                // Send signal to update table
                //schema::FullMetadata::emit_affected_schema(app, vec![table_oid])?;
            }

            Self::EditTableCellContents(cell) => {
                let execution_result: Result<(), Error> = {
                    // Update the contents of the cell
                    match cell.set() {
                        Ok(old_cell) => {
                            record_action(Self::EditTableCellContents(old_cell), is_forward);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };

                // Send signal to update that cell + any dependent cells
                //cell::Cell::emit_affected_cells(app, cell.table_oid, cell.column_oid, cell.row_oid)?;

                // Throw error if execution failed
                if let Err(e) = execution_result {
                    return Err(e);
                }
            }
            Self::CreateObject { table_oid, column_oid, row_oid, object_table_oid } => {
                let object_row_oid: i64 = table::row::TableRow::insert(object_table_oid, None)?;
                let cell: table::row::TableCell = table::row::TableCell {
                    table_oid,
                    column_oid,
                    row_oid,
                    content: table::row::TableCellContent::Object { 
                        table_oid: object_table_oid, 
                        value: Some(object_row_oid) 
                    }
                };
                let old_cell = cell.set()?;
                record_action(Self::EditTableCellContents(old_cell), is_forward);

                // Send signal to update that cell + any dependent cells
                //cell::Cell::emit_affected_cells(app, cell.table_oid, cell.column_oid, cell.row_oid)?;
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
