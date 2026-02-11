use std::collections::HashMap;

use crate::backend::{data_type, db, table, table_data};
use crate::util::error;
use rusqlite::{params, OptionalExtension, Statement, ToSql, Transaction};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;

/// Creates a new table.
pub fn create(name: String, master_table_oid_list: &Vec<i64>) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Add metadata for the table
    trans.execute("INSERT INTO METADATA_TYPE (MODE) VALUES (4);", [])?;
    let table_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_TABLE (TYPE_OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name],
    )?;

    // Create the table
    let create_table_cmd: String = format!(
        "
    CREATE TABLE TABLE{table_oid} (
        OID INTEGER PRIMARY KEY, 
        TRASH INTEGER NOT NULL DEFAULT 0
    ) STRICT;"
    );
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
#[serde(rename_all = "camelCase")]
pub struct BasicMetadata {
    oid: i64,
    name: String,
    hierarchy_level: i64,
}

/// Sends all object types through the given channel.
pub fn send_metadata_list(
    obj_type_oid: Option<i64>,
    obj_type_channel: Channel<BasicMetadata>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let select_statement: &str;
    let select_params: &[&dyn ToSql] = match obj_type_oid {
        Some(o) => {
            // If only retrieving the inheritors of a specific master type, filter the top level of the recursion query
            // by the OID of that master type.

            select_statement = "
            WITH RECURSIVE SUBTYPE_QUERY (LEVEL, MASTER_TYPE_OID, TYPE_OID, TYPE_NAME) AS (
                SELECT
                    0 AS LEVEL,
                    NULL AS MASTER_TYPE_OID,
                    typ.OID AS TYPE_OID,
                    tbl.NAME AS TYPE_NAME
                FROM METADATA_TYPE typ
                INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = typ.OID
                WHERE tbl.TRASH = 0 AND typ.OID = ?1
                UNION
                SELECT
                    s.LEVEL + 1 AS LEVEL,
                    s.TYPE_OID AS MASTER_TYPE_OID,
                    u.INHERITOR_TABLE_OID AS TYPE_OID,
                    tbl.NAME AS TYPE_NAME
                FROM SUBTYPE_QUERY s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = u.INHERITOR_TABLE_OID
                WHERE u.TRASH = 0 AND tbl.TRASH = 0
                ORDER BY 1 DESC, 4 ASC
            )
            SELECT
                LEVEL,
                TYPE_OID,
                TYPE_NAME
            FROM SUBTYPE_QUERY";
            params![o.clone()]
        }
        None => {
            // If retrieving all object types, only filter the topmost level of the recursion query by
            // object types that have no master type.

            select_statement = "
            WITH RECURSIVE SUBTYPE_QUERY (LEVEL, MASTER_TYPE_OID, TYPE_OID, TYPE_NAME) AS (
                SELECT
                    0 AS LEVEL,
                    NULL AS MASTER_TYPE_OID,
                    typ.OID AS TYPE_OID,
                    tbl.NAME AS TYPE_NAME
                FROM METADATA_TYPE typ
                INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = typ.OID
                WHERE tbl.TRASH = 0 
                    AND tbl.TRASH = 0 
                    AND typ.MODE = 4 
                    AND typ.OID NOT IN (SELECT DISTINCT INHERITOR_TABLE_OID FROM METADATA_TABLE_INHERITANCE)
                UNION
                SELECT
                    s.LEVEL + 1 AS LEVEL,
                    s.TYPE_OID AS MASTER_TYPE_OID,
                    u.INHERITOR_TABLE_OID AS TYPE_OID,
                    tbl.NAME AS TYPE_NAME
                FROM SUBTYPE_QUERY s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = u.INHERITOR_TABLE_OID
                WHERE u.TRASH = 0 AND tbl.TRASH = 0
                ORDER BY 1 DESC, 4 ASC
            )
            SELECT
                LEVEL,
                TYPE_OID,
                TYPE_NAME
            FROM SUBTYPE_QUERY";
            params![]
        }
    };

    // Send each queried object type to the frontend
    db::query_iterate(&trans, select_statement, select_params, &mut |row| {
        let level: i64 = row.get("LEVEL")?;
        let type_oid: i64 = row.get("TYPE_OID")?;
        let type_name: String = row.get("TYPE_NAME")?;

        obj_type_channel.send(BasicMetadata {
            oid: type_oid,
            name: type_name,
            hierarchy_level: level,
        })?;

        return Ok(());
    })?;
    return Ok(());
}

pub fn send_obj_data(
    obj_type_oid: i64,
    obj_row_oid: i64,
    obj_data_channel: Channel<table_data::RowCell>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let mut subtypes: HashMap<i64, i64> = HashMap::new();
    subtypes.insert(obj_type_oid, obj_row_oid);
    let mut max_level: i64 = 0;
    let mut max_level_subtype: Vec<i64> = vec![obj_type_oid];

    // Query a list of all subtypes of the given type
    let mut subtype_statement = trans.prepare(
        "WITH RECURSIVE SUBTYPE_QUERY (LEVEL, MASTER_TYPE_OID, TYPE_OID) AS (
                SELECT
                    1 AS LEVEL,
                    u.MASTER_TABLE_OID AS MASTER_TYPE_OID,
                    u.INHERITOR_TABLE_OID AS TYPE_OID
                FROM METADATA_TABLE_INHERITANCE u 
                WHERE u.TRASH = 0 AND u.MASTER_TABLE_OID = ?1
                UNION
                SELECT
                    s.LEVEL + 1 AS LEVEL,
                    s.TYPE_OID AS MASTER_TYPE_OID,
                    u.INHERITOR_TABLE_OID AS TYPE_OID
                FROM SUBTYPE_QUERY s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                WHERE u.TRASH = 0
                ORDER BY 1 DESC
            )
            SELECT
                LEVEL,
                MASTER_TYPE_OID,
                TYPE_OID
            FROM SUBTYPE_QUERY",
    )?;
    let subtype_rows = subtype_statement.query_map(params![obj_type_oid], |row| {
        let level: i64 = row.get("LEVEL")?;
        let master_type_oid: i64 = row.get("MASTER_TYPE_OID")?;
        let type_oid: i64 = row.get("TYPE_OID")?;
        return Ok((level, master_type_oid, type_oid));
    })?;

    // Find each table with a row associated with the obj_row_oid in the original object table
    for subtype_row_result in subtype_rows {
        let (level, master_type_oid, inheritor_type_oid) = subtype_row_result.unwrap();
        if !subtypes.contains_key(&inheritor_type_oid) && subtypes.contains_key(&master_type_oid) {
            let master_row_oid: i64 = subtypes[&master_type_oid];
            let select_from_type_table_cmd: String = format!("SELECT OID FROM TABLE{inheritor_type_oid} WHERE MASTER{master_type_oid}_OID = ?1 AND TRASH = 0");
            match trans
                .query_one(
                    &select_from_type_table_cmd,
                    params![master_row_oid],
                    |row| row.get(0),
                )
                .optional()?
            {
                Some(inheritor_row_oid) => {
                    subtypes.insert(inheritor_type_oid, inheritor_row_oid);

                    if level > max_level {
                        max_level = level;
                        max_level_subtype = vec![inheritor_type_oid];
                    } else if level == max_level {
                        max_level_subtype.push(inheritor_type_oid);
                    }
                }
                None => {}
            }
        }
    }

    // Check that there is only one subtype on the lowest level found
    if max_level_subtype.len() > 1 {
        return Err(error::Error::AdhocError("Invalid database state detected - A single object cannot have multiple final subtypes."));
    }
    let final_obj_type_oid: i64 = max_level_subtype[0];
    let final_obj_row_oid: i64 = subtypes[&final_obj_type_oid];

    // Send the columns and values of the row
    table_data::send_table_row(final_obj_type_oid, final_obj_row_oid, obj_data_channel)?;
    return Ok(());
}
