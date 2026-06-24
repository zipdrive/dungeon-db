use tauri::Manager;
use tauri_plugin_dialog::DialogExt;

mod data;
mod util;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            data::init_new,
            data::save,
            data::save_as,
            data::load,
            data::undo,
            data::redo,
            util::dialog::dialog_open,
            util::dialog::dialog_close,
            data::query,
            data::get_table_metadata,
            data::get_report_metadata,
            data::get_schema_metadata,
            data::get_column,
            data::get_cell,
            data::get_processid,
            data::get_table_row_labels,
            data::get_image_src,
            data::download_file,
            data::upload_file,
            data::execute
        ])
        .on_window_event(|window, event| {
            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if window.label() == "main" {
                        if data::has_unsaved_changes() {
                            // If there are unsaved changes, prompt user to save file before closing window
                            match window
                                .app_handle()
                                .dialog()
                                .message("Do you want to save your changes?")
                                .buttons(tauri_plugin_dialog::MessageDialogButtons::YesNoCancel)
                                .blocking_show_with_result()
                            {
                                tauri_plugin_dialog::MessageDialogResult::Yes => {
                                    // Save the file before closing
                                    let _ = data::save_shortcut(window.app_handle());
                                }
                                tauri_plugin_dialog::MessageDialogResult::No => {
                                    // Do not save the file before closing
                                }
                                _ => {
                                    // Do not close the app
                                    api.prevent_close();
                                    return;
                                }
                            }
                        }

                        // Since the user is trying to close the main window, also close every other window
                        for (_, subwindow) in window.webview_windows().iter() {
                            let _ = subwindow.close();
                        }
                    }
                }
                _ => {}
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
