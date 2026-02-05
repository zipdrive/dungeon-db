use rusqlite::{Transaction, Statement, params};
use serde::{Serialize, Deserialize};
use crate::backend::{db};
use crate::util::error;




#[derive(Serialize)]
pub struct BasicMetadata {
    oid: i64,
    name: String,
    hierarchy_level: i64
}

// Sends all object types that inherit directly from the inherited object type.
fn send_inheritor_metadata_list(obj_type: BasicMetadata, select_inheritors_cmd: &Statement, obj_type_channel: &Channel<BasicMetadata>) -> Result<(), error::Error> {
    for obj_type_result in select_inheritors_cmd.query_mapped(params![obj_type.oid], |row| Ok(BasicMetadata { oid: row.get("TYPE_OID")?, name: row.get("NAME")?, hierarchy_level: obj_type.hierarchy_level + 1 })) {
        let obj_type = obj_type_result?;
        obj_type_channel.send(obj_type)?;
        send_inheritor_metadata_list(obj_type, select_inheritors_cmd, obj_type_channel)?;
    }
    return Ok(());
}

/// Sends all object types through the given channel.
pub fn send_metadata_list(obj_type_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let select_toplevel_cmd = trans.prepare("SELECT t.TYPE_OID, t.NAME FROM METADATA_TABLE t WHERE t.TYPE_OID NOT IN (SELECT DISTINCT INHERITOR_TABLE_OID FROM METADATA_TABLE_INHERITANCE)")?;
    let select_inheritors_cmd = trans.prepare("SELECT t.TYPE_OID, t.NAME FROM METADATA_TABLE t INNER JOIN METADATA_TABLE_INHERITANCE i ON i.INHERITOR_TABLE_OID = t.TYPE_OID WHERE i.MASTER_TABLE_OID = ?1")?;

    for obj_type_result in select_toplevel_cmd.query_mapped([], |row| Ok(BasicMetadata { oid: row.get("TYPE_OID")?, name: row.get("NAME"), hierarchy_level: 0 })) {
        let obj_type = obj_type_result?;
        obj_type_channel.send(obj_type)?;
        send_inheritor_metadata_list(obj_type, &select_inheritors_cmd, &obj_type_channel)?;
    }

    return Ok(());
}