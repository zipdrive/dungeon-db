use std::collections::HashMap;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Row, Error as RusqliteError};
use serde::Serialize;
use tauri::ipc::Channel;
use crate::backend::db;
use crate::util::error;






/// Creates a new table.
pub fn create(name: String) -> Result<i64, error::Error> {
    let action = db::begin_db_action()?;
    action.trans.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (3);", [])?;
    let table_oid: i64 = action.trans.last_insert_rowid();
    action.trans.execute(
        "INSERT INTO METADATA_TABLE (OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name]
    )?;
    let create_table_cmd: String = format!("CREATE TABLE TABLE{} (OID INTEGER PRIMARY KEY);", table_oid);
    action.trans.execute(&create_table_cmd, [])?;
    let create_view_cmd = format!("CREATE VIEW TABLE{table_oid}_SURROGATE (OID, DISPLAY_VALUE) AS SELECT OID, OID FROM TABLE{table_oid};");
    action.trans.execute(&create_view_cmd, [])?;
    return Ok(table_oid);
}

// Deletes the table with the given OID and all associated local columns.
pub fn delete(oid: i64) -> Result<(), error::Error> {
    let action = db::begin_db_action()?;

    // Drop data from the table
    let drop_cmd: String = format!("DROP TABLE TABLE{};", oid);
    action.trans.execute(&drop_cmd, [])?;

    // Drop tables associated locally with the table


    // Finally, drop the table's metadata
    action.trans.execute("DELETE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = ?1;", [oid])?;
    return Ok(());
}



#[derive(Serialize)]
/// The most bare-bones version of table metadata, used solely for populating the list of tables
pub struct BasicMetadata {
    oid: i64,
    name: String
}

/// Sends a list of tables through the provided channel.
pub fn send_metadata_list(table_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let action = db::begin_readonly_db_action()?;

    action.query_iterate("SELECT OID, NAME FROM METADATA_TABLE ORDER BY NAME ASC;", [], 
        &mut |row| {
            table_channel.send(BasicMetadata {  
                oid: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}



#[derive(Serialize)]
struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    //data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>
}