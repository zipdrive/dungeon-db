use serde::Serialize;
use rusqlite::{Transaction, params};
use crate::backend::db;
use crate::util::error;
use tauri::ipc::Channel;


/// Create a new report parameter.
pub fn create(trans: &Transaction) -> Result<i64, error::Error> {
    // Create datasource
    trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
    let datasource_oid: i64 = trans.last_insert_rowid();
    // Create parameter
    trans.execute("INSERT INTO METADATA_PARAMETER (OID) VALUES (?1)", params![datasource_oid])?;
    // Return the datasource/parameter OID
    return Ok(datasource_oid);
}


#[derive(Serialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum VirtualParameter {
    /// A column in the base table.
    Column {
        column_oid: i64,
        source_name: String,
        column_name: String,
        linked_table_oid: i64,
        is_many_to_one: bool
    },

    /// Inheritance from a master list.
    MasterList {
        master_table_oid: i64,
        master_table_name: String
    },

    /// A reference to the base table from another table.
    Reference {
        column_oid: i64,
        source_name: String,
        column_name: String,
        linked_table_oid: i64
    },

    /// Base table is the master list for another table
    Inheritance {
        inheritor_table_oid: i64,
        inheritor_table_name: String 
    }
}

/// Send a list of possible parameters.
pub fn send_parameter_list(base_table_oid: i64, virtual_param_channel: Channel<VirtualParameter>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Start with columns
    db::query_iterate(
        &trans,
        "SELECT 
                c.OID AS COLUMN_OID, 
                tbl.NAME AS SOURCE_NAME,
                c.NAME AS COLUMN_NAME,
                CASE WHEN typ.MODE = 3 OR typ.MODE = 4 OR typ.MODE = 5 THEN tbl.TYPE_OID ELSE NULL END AS LINKED_TABLE_OID,
                CASE WHEN typ.MODE = 5 THEN 1 ELSE 0 END AS IS_MANY_TO_ONE
            FROM METADATA_TYPE typ 
            INNER JOIN METADATA_TABLE tbl ON typ.OID = tbl.TYPE_OID
            INNER JOIN METADATA_TABLE_COLUMN c ON tbl.TYPE_OID = c.TABLE_OID
            WHERE typ.OID = ?1 AND c.TRASH = 0
            ORDER BY c.COLUMN_ORDERING ASC;",
        params![base_table_oid],
        &mut |row| {
            virtual_param_channel.send(VirtualParameter::Column { 
                column_oid: row.get("COLUMN_OID")?, 
                source_name: row.get("SOURCE_NAME")?, 
                column_name: row.get("COLUMN_NAME")?, 
                linked_table_oid: row.get("LINKED_TABLE_OID")?,
                is_many_to_one: row.get("IS_MANY_TO_ONE")?
            })?;
            return Ok(());
        },
    )?;

    // Then, tables that the base table inherits from
    db::query_iterate(
        &trans,
        "SELECT 
                tbl.TYPE_OID AS MASTER_TABLE_OID, 
                tbl.NAME AS MASTER_TABLE_NAME
            FROM METADATA_TABLE_INHERITANCE inh
            INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = inh.MASTER_TABLE_OID
            INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.TYPE_OID
            WHERE inh.INHERITOR_TABLE_OID = ?1 AND inh.TRASH = 0 AND tbl.TRASH = 0
            ORDER BY tbl.NAME ASC;",
        params![base_table_oid],
        &mut |row| {
            virtual_param_channel.send(VirtualParameter::MasterList { 
                master_table_oid: row.get("MASTER_TABLE_OID")?,
                master_table_name: row.get("MASTER_TABLE_NAME")?
            })?;
            return Ok(());
        },
    )?;

    // Then, columns that reference the base table
    db::query_iterate(
        &trans,
        "SELECT 
                c.OID AS COLUMN_OID,
                tbl.NAME AS SOURCE_NAME,
                c.NAME AS COLUMN_NAME,
                tbl.TYPE_OID AS LINKED_TABLE_OID
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = c.TABLE_OID
            INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.TYPE_OID
            WHERE c.TYPE_OID = ?1 AND c.TRASH = 0 AND tbl.TRASH = 0
            ORDER BY tbl.NAME, c.NAME ASC;",
        params![base_table_oid],
        &mut |row| {
            virtual_param_channel.send(VirtualParameter::Reference { 
                column_oid: row.get("COLUMN_OID")?,
                source_name: row.get("SOURCE_NAME")?,
                column_name: row.get("COLUMN_NAME")?,
                linked_table_oid: row.get("LINKED_TABLE_OID")?
            })?;
            return Ok(());
        },
    )?;

    // Then, tables that inherit from the base table
    db::query_iterate(
        &trans,
        "SELECT 
                tbl.TYPE_OID AS INHERITOR_TABLE_OID, 
                tbl.NAME AS INHERITOR_TABLE_NAME
            FROM METADATA_TABLE_INHERITANCE inh
            INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = inh.INHERITOR_TABLE_OID
            INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.TYPE_OID
            WHERE inh.MASTER_TABLE_OID = ?1 AND inh.TRASH = 0 AND tbl.TRASH = 0
            ORDER BY tbl.NAME ASC;",
        params![base_table_oid],
        &mut |row| {
            virtual_param_channel.send(VirtualParameter::Inheritance { 
                inheritor_table_oid: row.get("INHERITOR_TABLE_OID")?,
                inheritor_table_name: row.get("INHERITOR_TABLE_NAME")?
            })?;
            return Ok(());
        },
    )?;
    return Ok(());
}