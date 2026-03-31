use crate::util::error;
use crate::util::channel::Sender;
use serde::{Deserialize};
use std::sync::Mutex;
use tauri::ipc::{Channel as TauriChannel, JavaScriptChannelId};
use tauri::{AppHandle, Emitter, Manager, Webview, WebviewUrl, WebviewWindowBuilder};

#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Dialog {
    CreateTable,
    EditTable {
        table_oid: i64
    },
    CreateReport,
    EditReport {
        report_oid: i64
    },

    CreateColumn {
        schema_oid: i64,
        column_ordering: Option<i64>
    },
    EditColumn {
        column_oid: i64
    },
    AddParameter {
        id: i64 
    },

    Schema {
        title: String,
        query_string: String
    },
    Object {
        title: String,
        query_string: String
    }
}

/// Unique index for a window.
static WINDOW_IDX: Mutex<i64> = Mutex::new(1);

impl Dialog {
    pub async fn open(&self, app: &AppHandle) -> Result<(), error::Error> {
        let mut window_idx = WINDOW_IDX.lock().unwrap();
        let label: String = format!("window{}", *window_idx);
        *window_idx += 1;

        match &self {
            Self::CreateTable => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App("/src/dialog/schema.html?mode=table".into()),
                )
                .title("Create New Table")
                .inner_size(400.0, 400.0)
                .maximizable(false)
                .build()?;
            },
            Self::EditTable { table_oid } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(format!("/src/dialog/schema.html?schema_oid={table_oid}&mode=table").into()),
                )
                .title("Edit Table")
                .inner_size(400.0, 400.0)
                .maximizable(false)
                .build()?;
            },
            Self::CreateReport => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App("/src/dialog/schema.html?mode=report".into()),
                )
                .title("Create New Report")
                .inner_size(400.0, 400.0)
                .maximizable(false)
                .build()?;
            },
            Self::EditReport { report_oid } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(format!("/src/dialog/schema.html?schema_oid={report_oid}&mode=report").into()),
                )
                .title("Edit Report")
                .inner_size(400.0, 400.0)
                .maximizable(false)
                .build()?;
            },
            Self::CreateColumn { schema_oid, column_ordering } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(
                        format!(
                            "/src/dialog/column.html?schema_oid={schema_oid}{}",
                            match column_ordering {
                                Some(o) => format!("&column_ordering={o}"),
                                None => String::from(""),
                            }
                        )
                        .into(),
                    ),
                )
                .title("Add New Column")
                .inner_size(600.0, 600.0)
                .maximizable(false)
                .build()?;
            },
            Self::EditColumn { column_oid } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(format!(
                        "/src/dialog/column.html?column_oid={column_oid}"
                    ).into()),
                )
                .title("Edit Column")
                .inner_size(600.0, 600.0)
                .maximizable(false)
                .build()?;
            },
            Self::AddParameter { id } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(format!("/src/dialog/parameter.html?id={id}").into()),
                )
                .title("Add Parameter")
                .inner_size(400.0, 400.0)
                .maximizable(false)
                .build()?;
            },
            Self::Schema { title, query_string } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(format!("/src/schema.html?{query_string}").into()),
                )
                .title(&*title)
                .inner_size(800.0, 600.0)
                .build()?;
            },
            Self::Object { title, query_string } => {
                WebviewWindowBuilder::new(app, label,
                    WebviewUrl::App(
                        format!("/src/object.html?{query_string}")
                            .into(),
                    ),
                )
                .title(&*title)
                .inner_size(800.0, 600.0)
                .build()?;
            }
        }
        return Ok(());
    }
}

#[tauri::command]
/// Open a separate window for the contents of a report.
pub async fn dialog_open(app: AppHandle, dialog: Dialog) -> Result<(), error::Error> {
    return dialog.open(&app).await;
}

#[tauri::command]
/// Closes the current dialog window.
pub fn dialog_close(window: tauri::Window) -> Result<(), error::Error> {
    window.close()?;
    return Ok(());
}
