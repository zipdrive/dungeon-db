mod util;
mod data;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_global_shortcut::Builder::new().build())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            #[cfg(desktop)]
            {
                use tauri_plugin_global_shortcut::{Code, GlobalShortcutExt, Modifiers, Shortcut, ShortcutState};

                let undo_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyZ);
                let redo_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyY);

                let cut_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyX);
                let copy_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyC);
                let paste_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyV);

                let new_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyN);
                let save_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyS);
                let open_shortcut = Shortcut::new(Some(Modifiers::CONTROL), Code::KeyO);

                app.handle().plugin(
                    tauri_plugin_global_shortcut::Builder::new().with_handler(move |_app, shortcut, event| {
                        if event.state() == ShortcutState::Pressed {
                            if shortcut == &undo_shortcut {
                                data::undo(_app);
                            } else if shortcut == &redo_shortcut {
                                data::redo(_app); 
                            }
                        }
                    })
                    .build(),
                )?;

                app.global_shortcut().register(undo_shortcut)?;
                app.global_shortcut().register(redo_shortcut)?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            data::init,
            util::dialog::dialog_open,
            util::dialog::dialog_close,
            data::query,
            data::get_table_metadata,
            data::get_report_metadata,
            data::get_column,
            data::get_blob,
            data::download_blob,
            data::execute
        ])
        .on_window_event(|window, event| {
            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if window.label() == "main" {
                        // TODO show save popup?
                    }
                }
                _ => {}
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
