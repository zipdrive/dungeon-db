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
            backend::dialog_create_table_column,
            backend::dialog_edit_table_column,
            backend::dialog_table_data,
            backend::dialog_child_table_data,
            backend::dialog_object_data,
            backend::get_table_list,
            backend::get_report_list,
            backend::get_object_type_list,
            backend::get_subtype_list,
            backend::get_master_list_option_dropdown_values,
            backend::get_table_column,
            backend::get_table_column_list,
            backend::get_table_column_dropdown_values,
            backend::get_table_column_reference_values,
            backend::get_table_column_object_values,
            backend::get_table_data,
            backend::get_table_row,
            backend::get_object_data,
            backend::execute,
            backend::undo,
            backend::redo,
        ])
        .on_window_event(|window, event| {
            match event {
                tauri::WindowEvent::CloseRequested { api, .. } => {
                    if window.label() == "main" {
                        // TODO show save popup?
                    }
                },
                _ => {}
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
