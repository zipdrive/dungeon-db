use rusqlite::{Transaction, Statement, params};
use tauri::ipc::Channel;
use serde::{Serialize, Deserialize};
use crate::backend::{db, table};
use crate::util::error;


/// Creates a new table.
pub fn create(name: String, master_table_oid_list: &Vec<i64>) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Add metadata for the table
    trans.execute("INSERT INTO METADATA_TYPE (MODE) VALUES (4);", [])?;
    let table_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_TABLE (TYPE_OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name]
    )?;

    // Create the table
    let create_table_cmd: String = format!("
    CREATE TABLE TABLE{table_oid} (
        OID INTEGER PRIMARY KEY, 
        TRASH INTEGER NOT NULL DEFAULT 0
    ) STRICT;");
    trans.execute(&create_table_cmd, [])?;

    // Add inheritance from each master table
    for master_table_oid in master_table_oid_list.iter() {
        // Insert metadata indicating that this table inherits from the master table
        trans.execute(
            "INSERT INTO METADATA_TABLE_INHERITANCE (INHERITOR_TABLE_OID, MASTER_TABLE_OID) VALUES (?1, ?2);",
            params![table_oid, master_table_oid]
        )?;

        // Add a column to the table that references a row in the master list
        let alter_table_cmd: String = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN MASTER{master_table_oid}_OID INTEGER NOT NULL REFERENCES TABLE{master_table_oid} (OID) ON UPDATE CASCADE ON DELETE CASCADE;");
        trans.execute(&alter_table_cmd, [])?;
    }
    
    // Update the surrogate view
    table::update_surrogate_view(&trans, table_oid.clone())?;

    // Commit the transaction
    trans.commit()?;
    return Ok(table_oid);
}



#[derive(Serialize, Clone)]
pub struct BasicMetadata {
    oid: i64,
    name: String,
    hierarchy_level: i64
}

// Sends all object types that inherit directly from the inherited object type.
fn send_inheritor_metadata_list(trans: &Transaction, obj_type: BasicMetadata, obj_type_channel: &Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut select_inheritors_cmd = trans.prepare("SELECT t.TYPE_OID, t.NAME FROM METADATA_TABLE t INNER JOIN METADATA_TABLE_INHERITANCE i ON i.INHERITOR_TABLE_OID = t.TYPE_OID WHERE i.MASTER_TABLE_OID = ?1")?;

    let obj_types = select_inheritors_cmd.query_map(params![obj_type.oid], |row| Ok(BasicMetadata { oid: row.get("TYPE_OID")?, name: row.get("NAME")?, hierarchy_level: obj_type.hierarchy_level + 1 }))?;
    for obj_type_result in obj_types {
        let obj_type = obj_type_result?;
        obj_type_channel.send(obj_type.clone())?;
        send_inheritor_metadata_list(trans, obj_type, obj_type_channel)?;
    }
    return Ok(());
}

/// Sends all object types through the given channel.
pub fn send_metadata_list(obj_type_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let mut select_toplevel_cmd = trans.prepare("SELECT t.TYPE_OID, t.NAME FROM METADATA_TABLE t WHERE t.TYPE_OID NOT IN (SELECT DISTINCT INHERITOR_TABLE_OID FROM METADATA_TABLE_INHERITANCE)")?;

    let obj_types = select_toplevel_cmd.query_map([], |row| Ok(BasicMetadata { oid: row.get("TYPE_OID")?, name: row.get("NAME")?, hierarchy_level: 0 }))?;
    for obj_type_result in obj_types {
        let obj_type = obj_type_result?;
        obj_type_channel.send(obj_type.clone())?;
        send_inheritor_metadata_list(&trans, obj_type, &obj_type_channel)?;
    }

    return Ok(());
}