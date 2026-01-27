mod backend;
mod util;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            backend::init,
            backend::dialog_close,
            backend::dialog_create_table,
            backend::create_table,
            backend::get_table_list,
            backend::dialog_create_table_column,
            backend::create_table_column,
            backend::get_table_column_list,
            backend::push_row,
            backend::insert_row,
            backend::get_table_data,
        ])
        .on_window_event(|window, event| {
            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if window.label() == "main" {
                        backend::close();
                    }
                },
                _ => {}
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
