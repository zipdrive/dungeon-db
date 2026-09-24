use rusqlite::{Connection, params};
use crate::data::table::row::{TableCellContent, TableCellTextContentFormat, TableRow};
use crate::data::table::column::TableColumnMetadata;
use crate::util::encode::json_encode_string;
use crate::util::db::{sql_one};
use crate::util::db;
use crate::util::error::Error;

fn conn_get_keys(conn: &Connection, table_oid: i64, row_oid: i64, block_recursion: &Vec<(i64, i64)>) -> Result<Vec<(String, Option<String>, String)>, Error> {
    // Get the values from the row
    let row = TableRow::conn_get(conn, table_oid, row_oid)?;
    
    // Get the primary keys
    let mut keys: Vec<(String, Option<String>, String)> = Vec::new();
    for (_, _, column) in TableColumnMetadata::conn_query_all(conn, table_oid)? {
        if column.is_primary_key {
            let cell = row.cells.iter().find(|c| c.column_oid == column.oid);
            if let Some(cell) = cell {
                keys.push(
                    match &cell.content {
                        TableCellContent::Boolean { value } => (
                            column.name,
                            Some(String::from(if *value { "true" } else { "false" })),
                            String::from(if *value { "true" } else { "false" })
                        ),
                        TableCellContent::Integer { value } => (
                            column.name,
                            if let Some(value) = value { Some(format!("{value}")) } else { None },
                            if let Some(value) = value { format!("{value}") } else { String::from("null") }
                        ),
                        TableCellContent::Number { value } => (
                            column.name,
                            if let Some(value) = value { Some(format!("{value}")) } else { None },
                            if let Some(value) = value { format!("{value}") } else { String::from("null") }
                        ),
                        TableCellContent::Text { value, format } => (
                            column.name,
                            if let Some(value) = value { 
                                Some(value.clone())
                            } else { 
                                None
                            },
                            if let Some(value) = value { 
                                if let TableCellTextContentFormat::Json = format { value.clone() } else { format!("\"{}\"", json_encode_string(value)) }
                            } else { 
                                String::from("null") 
                            }
                        ),
                        TableCellContent::Date { label, .. }
                        | TableCellContent::Datetime { label, .. } => (
                            column.name,
                            if let Some(label) = label { 
                                Some(label.clone())
                            } else { 
                                None
                            },
                            if let Some(label) = label { 
                                format!("\"{}\"", json_encode_string(label))
                            } else { 
                                String::from("null") 
                            }
                        ),
                        TableCellContent::File { value } => (
                            column.name,
                            if let Some(file) = value {
                                Some(file.name().clone())
                            } else {
                                None
                            },
                            if let Some(file) = value {
                                format!("\"{}\"", json_encode_string(file.name()))
                            } else {
                                String::from("null")
                            }
                        ),
                        TableCellContent::Object { table_oid: referenced_table_oid, value, .. } => 
                            if let Some(referenced_row_oid) = value {
                                let object_label = conn_get_object_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                (
                                    column.name,
                                    Some(object_label.clone()),
                                    format!("{{ {object_label} }}")
                                )
                            } else {
                                (
                                    column.name,
                                    None,
                                    String::from("null")
                                )
                            },
                        TableCellContent::SingleSelectDropdown { table_oid: referenced_table_oid, value } => 
                            if let Some(referenced_row_oid) = value {
                                let (plain_label, json_label) = conn_get_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                (
                                    column.name,
                                    plain_label,
                                    json_label
                                )
                            } else {
                                (
                                    column.name,
                                    None,
                                    String::from("null")
                                )
                            },
                        TableCellContent::MultiSelectDropdown { table_oid: referenced_table_oid, value } => {
                            let mut items: Vec<String> = Vec::new();
                            for referenced_row_oid in value {
                                let item = conn_get_json_label(conn, referenced_table_oid.clone(), referenced_row_oid.clone(), block_recursion)?;
                                items.push(item);
                            }
                            let label = match items.into_iter().reduce(|acc, e| format!("{acc}, {e}")) {
                                Some(items) => format!("[ {items} ]"),
                                None => String::from("[]")
                            };
                            (
                                column.name,
                                Some(label.clone()),
                                label
                            )
                        }
                        TableCellContent::Subreport { report_oid } => {
                            let label: String = String::from("[]"); // TODO
                            (
                                column.name,
                                Some(label.clone()),
                                label 
                            )
                        }
                    }
                );
            }
        }
    }
    Ok(keys)
}

fn conn_get_label(conn: &Connection, table_oid: i64, row_oid: i64, block_recursion: &Vec<(i64, i64)>) -> Result<(Option<String>, String), Error> {
    // Block infinite recursion
    if block_recursion.iter().any(|(recurs_table_oid, recurs_row_oid)| *recurs_table_oid == table_oid && *recurs_row_oid == row_oid) {
        return Ok((Some(String::from("...")), String::from("{ ... }")));
    }
    let mut new_block_recursion: Vec<(i64, i64)> = block_recursion.clone();
    new_block_recursion.push((table_oid.clone(), row_oid.clone()));

    // Get the primary keys of the table
    let keys = conn_get_keys(&conn, table_oid, row_oid, &new_block_recursion)?;
    Ok((
        if keys.len() == 0 {
            Some(String::from("— NO PRIMARY KEY —"))
        } else if keys.len() == 1 {
            keys[0].1.clone()
        } else {
            None
        },
        match keys.into_iter()
            .map(|(key, _, value)| format!("\"{}\": {value}", json_encode_string(&key)))
            .reduce(|acc, e| format!("{acc}, {e}")) {
            Some(keys) => format!("{{ {keys} }}"),
            None => String::from("{{}}")
        }
    ))
}

fn conn_get_json_label(conn: &Connection, table_oid: i64, row_oid: i64, block_recursion: &Vec<(i64, i64)>) -> Result<String, Error> {
    // Block infinite recursion
    if block_recursion.iter().any(|(recurs_table_oid, recurs_row_oid)| *recurs_table_oid == table_oid && *recurs_row_oid == row_oid) {
        return Ok(String::from("{ ... }"));
    }
    let mut new_block_recursion: Vec<(i64, i64)> = block_recursion.clone();
    new_block_recursion.push((table_oid.clone(), row_oid.clone()));

    // Get the primary keys of the table
    Ok(match conn_get_keys(&conn, table_oid, row_oid, &new_block_recursion)?
        .into_iter()
        .map(|(key, _, value)| format!("\"{}\": {value}", json_encode_string(&key)))
        .reduce(|acc, e| format!("{acc}, {e}")) {
        Some(keys) => format!("{{ {keys} }}"),
        None => String::from("{{}}")
    })
}

fn conn_get_object_label(conn: &Connection, table_oid: i64, row_oid: i64, block_recursion: &Vec<(i64, i64)>) -> Result<String, Error> {
    // Get the table and row of the object
    let (object_table_oid, object_table_name, object_row_oid) = sql_one(
        &conn, 
        format!("SELECT o.OBJECT_TABLE_OID, t.NAME AS OBJECT_TABLE_NAME, o.OBJECT_ROW_OID FROM OBJECT{table_oid} o INNER JOIN METADATA_TABLE t ON t.OID = o.OBJECT_TABLE_OID WHERE o.OID = ?1"), 
        params![row_oid], 
        |row| Ok((
            row.get::<_, i64>("OBJECT_TABLE_OID")?,
            row.get::<_, String>("OBJECT_TABLE_NAME")?,
            row.get::<_, i64>("OBJECT_ROW_OID")?
        ))
    )?;

    // Block infinite recursion
    if block_recursion.iter().any(|(recurs_table_oid, recurs_row_oid)| *recurs_table_oid == table_oid && *recurs_row_oid == row_oid) {
        return Ok(format!("\"{}\": {{ ... }}", json_encode_string(&object_table_name)));
    }
    let mut new_block_recursion: Vec<(i64, i64)> = block_recursion.clone();
    new_block_recursion.push((object_table_oid.clone(), object_row_oid.clone()));
    
    // Get the primary keys of the object
    Ok(match conn_get_keys(&conn, object_table_oid, object_row_oid, &new_block_recursion)?
        .into_iter()
        .map(|(key, _, value)| format!("\"{}\": {value}", json_encode_string(&key)))
        .reduce(|acc, e| format!("{acc}, {e}")) {
        Some(keys) => format!("\"{}\": {{ {keys} }}", json_encode_string(&object_table_name)),
        None => format!("\"{}\": {{}}", json_encode_string(&object_table_name))
    })
}


/// Gets the label for a SingleSelect or MultiSelect.
pub fn get_select_label(table_oid: i64, row_oid: i64) -> Result<String, Error> {
    let conn = db::open()?;
    let (plain_label, json_label) = conn_get_label(&conn, table_oid, row_oid, &Vec::new())?;
    Ok(plain_label.unwrap_or(json_label))
}

/// Gets the label for an Object.
pub fn get_object_label(table_oid: i64, row_oid: i64) -> Result<String, Error> {
    let conn = db::open()?;
    conn_get_object_label(&conn, table_oid, row_oid, &Vec::new())
}