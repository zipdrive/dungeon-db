use tauri::Manager;
use tauri_plugin_dialog::DialogExt;

mod data;
mod util;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_http::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            #[cfg(desktop)]
            {
                use tauri_plugin_global_shortcut::{
                    Code, GlobalShortcutExt, Modifiers, Shortcut, ShortcutState,
                };

                let undo_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyZ);
                let redo_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyY);

                let new_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyN);
                let save_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyS);
                let load_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyO);

                app.handle().plugin(
                    tauri_plugin_global_shortcut::Builder::new()
                        .with_handler(move |_app, shortcut, event| {
                            if event.state() == ShortcutState::Pressed {
                                if shortcut == &undo_shortcut {
                                    let _ = data::undo(_app);
                                } else if shortcut == &redo_shortcut {
                                    let _ = data::redo(_app);
                                } else if shortcut == &new_shortcut {
                                    let _ = data::init_new_shortcut(_app);
                                } else if shortcut == &load_shortcut {
                                    let _ = data::load_shortcut(_app);
                                } else if shortcut == &save_shortcut {
                                    let _ = data::save_shortcut(_app);
                                }
                            };
                        })
                        .build(),
                )?;

                app.global_shortcut().register(undo_shortcut)?;
                app.global_shortcut().register(redo_shortcut)?;
                app.global_shortcut().register(new_shortcut)?;
                app.global_shortcut().register(load_shortcut)?;
                app.global_shortcut().register(save_shortcut)?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            data::init_new,
            data::init_existing,
            data::save,
            data::load,
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
