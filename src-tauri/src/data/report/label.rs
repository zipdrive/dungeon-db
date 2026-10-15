use rusqlite::Connection;
use crate::util::db;
use crate::util::error::Error;
use crate::util::channel::Sender;
use crate::util::encode::json_encode_string;
use crate::data::table;
use crate::data::table::row::{TableCellContent, TableCellTextContentFormat};
use crate::data::report::column::ReportColumnMetadata;
use crate::data::report::row::ReportRow;

pub fn conn_get_subreport_label(conn: &Connection, report_oid: i64, oid_filters: Vec<(String, Vec<i64>)>, block_recursion: &Vec<(i64, i64)>) -> Result<String, Error> {
    // Get the primary key columns
    let key_columns: Vec<ReportColumnMetadata> = ReportColumnMetadata::conn_query_all(conn, report_oid.clone())?
        .into_iter()
        .filter(|col| col.is_primary_key)
        .collect();
    
    // Get the values from the row
    let mut items: Vec<String> = Vec::new();
    ReportRow::conn_send(
        conn, 
        Sender::Callback(Box::new(|row| {
            let mut keys: Vec<(String, String)> = Vec::new();
            for cell in row.cells {
                if let Some(column) = key_columns.iter().find(|c| c.oid == cell.column_oid) {
                    keys.push(match cell.content {
                        TableCellContent::Boolean { value } => (
                            column.name.clone(),
                            String::from(if value { "true" } else { "false" })
                        ),
                        TableCellContent::Integer { value } => (
                            column.name.clone(),
                            if let Some(value) = value { format!("{value}") } else { String::from("null") }
                        ),
                        TableCellContent::Number { value } => (
                            column.name.clone(),
                            if let Some(value) = value { format!("{value}") } else { String::from("null") }
                        ),
                        TableCellContent::Text { value, format } => (
                            column.name.clone(),
                            if let Some(value) = value { 
                                if let TableCellTextContentFormat::Json = format { value.clone() } else { format!("\"{}\"", json_encode_string(&value)) }
                            } else { 
                                String::from("null") 
                            }
                        ),
                        TableCellContent::Date { label, .. }
                        | TableCellContent::Datetime { label, .. } => (
                            column.name.clone(),
                            if let Some(label) = label { 
                                format!("\"{}\"", json_encode_string(&label))
                            } else { 
                                String::from("null") 
                            }
                        ),
                        TableCellContent::File { value } => (
                            column.name.clone(),
                            if let Some(file) = value {
                                format!("\"{}\"", json_encode_string(file.name()))
                            } else {
                                String::from("null")
                            }
                        ),
                        TableCellContent::Object { table_oid: referenced_table_oid, value, .. } => 
                            if let Some(referenced_row_oid) = value {
                                let object_label = table::label::conn_get_object_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                (
                                    column.name.clone(),
                                    format!("{{ {object_label} }}")
                                )
                            } else {
                                (
                                    column.name.clone(),
                                    String::from("null")
                                )
                            },
                        TableCellContent::SingleSelectDropdown { table_oid: referenced_table_oid, value } => 
                            if let Some(referenced_row_oid) = value {
                                let (plain_label, json_label) = table::label::conn_get_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                (
                                    column.name.clone(),
                                    json_label
                                )
                            } else {
                                (
                                    column.name.clone(),
                                    String::from("null")
                                )
                            },
                        TableCellContent::MultiSelectDropdown { table_oid: referenced_table_oid, value } => {
                            let mut items: Vec<String> = Vec::new();
                            for referenced_row_oid in value {
                                let item = table::label::conn_get_json_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                items.push(item);
                            }
                            let label = match items.into_iter().reduce(|acc, e| format!("{acc}, {e}")) {
                                Some(items) => format!("[ {items} ]"),
                                None => String::from("[]")
                            };
                            (
                                column.name.clone(),
                                label
                            )
                        }
                        TableCellContent::Subreport { oid_filters, report_oid, .. } => {
                            let label: String = conn_get_subreport_label(conn, report_oid.clone(), oid_filters.clone(), block_recursion)?;
                            (
                                column.name.clone(),
                                label 
                            )
                        }
                    });
                }
            }

            if keys.len() == 0 {
                items.push(String::from("{}"));
            } else if keys.len() == 1 {
                items.push(keys[0].1.clone());
            } else {
                items.push(format!("{{ {} }}", 
                    keys.into_iter()
                        .map(|(k, v)| format!("\"{}\": {v}", json_encode_string(&k)))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                ));
            }
            Ok(())
        })),
        report_oid, 
        oid_filters,
        key_columns.clone(),
        false
    )?;
    Ok(match items.into_iter().reduce(|acc, e| format!("{acc}, {e}")) {
        Some(items) => format!("[ {items} ]"),
        None => String::from("[]")
    })
}

/// Gets the label for a subreport.
pub fn get_subreport_label(report_oid: i64, oid_filters: Vec<(String, Vec<i64>)>) -> Result<String, Error> {
    let conn = db::open()?;
    conn_get_subreport_label(&conn, report_oid, oid_filters, &Vec::new())
}