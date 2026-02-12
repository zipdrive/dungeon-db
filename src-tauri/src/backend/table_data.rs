use crate::backend::data_type::Primitive;
use crate::backend::{data_type, db, obj_type, table, table_column};
use crate::util::error;
use rusqlite::blob::ZeroBlob;
use rusqlite::{
    params, vtab::array::Array, Error as RusqliteError, OptionalExtension, Row, Transaction,
};
use serde::Serialize;
use serde_json::{Result as SerdeJsonResult, Value};
use std::collections::{HashMap, HashSet, LinkedList};
use std::path::Path;
use tauri::ipc::Channel;
use time::format_description::well_known;
use time::macros::time;
use time::{Date, PrimitiveDateTime, UtcDateTime};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use base64::Engine;
use base64::prelude::{BASE64_STANDARD as base64standard};

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum Cell {
    RowStart {
        row_oid: i64,
        row_index: i64,
    },
    ColumnValue {
        table_oid: i64,
        row_oid: i64,
        column_oid: i64,
        column_name: String,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists {
        row_exists: bool,
        table_oid: i64,
    },
    ColumnValue {
        table_oid: i64,
        row_oid: i64,
        column_oid: i64,
        column_name: String,
        column_type: data_type::MetadataColumnType,
        column_ordering: i64,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
}

struct RowOidParamAlias {
    type_oid: i64,
    type_param_alias: String,
    type_row_oid: i64,
    level: i64,
}

/// Inserts a new row into the table.
fn insert_inplace(
    trans: &Transaction,
    table_oid: i64,
    parent_row_oid: Option<i64>,
    row_oid: Option<i64>,
    known_supertype_oids: Option<Vec<RowOidParamAlias>>,
) -> Result<i64, error::Error> {
    rusqlite::vtab::array::load_module(trans)?;

    let mut type_row_oids: Vec<(String, i64)> = match &known_supertype_oids {
        Some(o) => o
            .iter()
            .map(|alias| (alias.type_param_alias.clone(), alias.type_row_oid))
            .collect(),
        None => Vec::new(),
    };
    match row_oid {
        Some(o) => { type_row_oids.push((String::from(":t"), o)); },
        None => {}
    }
    match parent_row_oid {
        Some(o) => { type_row_oids.push((String::from(":p"), o)); },
        None => {}
    }

    let select_cmd: String = format!("
        WITH RECURSIVE SUPERTYPE_QUERY (LEVEL, SUPERTYPE_OID, INHERITOR_TYPE_OID, COL_NAME, COL_VALUE_EXPRESSION) AS (
            SELECT
                0 AS LEVEL,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID,
                'MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID' AS COL_NAME,
                ':m' || FORMAT('%d', u.MASTER_TABLE_OID) AS COL_VALUE_EXPRESSION
            FROM METADATA_TABLE_INHERITANCE u
            WHERE u.TRASH = 0 AND u.INHERITOR_TABLE_OID = ?1
            UNION
            SELECT
                s.LEVEL + 1 AS LEVEL,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID,
                'MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID' AS COL_NAME,
                ':m' || FORMAT('%d', u.MASTER_TABLE_OID) AS COL_VALUE_EXPRESSION
            FROM SUPERTYPE_QUERY s
            LEFT JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.SUPERTYPE_OID
            WHERE u.TRASH = 0
        ),
        TYPE_QUERY (TYPE_OID) AS (
            SELECT ?1
            UNION
            SELECT
                u.MASTER_TABLE_OID
            FROM TYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.TYPE_OID
            WHERE u.TRASH = 0 AND u.MASTER_TABLE_OID NOT IN rarray(?2)
        )
        SELECT
            COALESCE(MAX(s.LEVEL), 9223372036854775807) AS MAX_LEVEL,
            t.TYPE_OID AS TYPE_OID,
            'INSERT INTO TABLE' || FORMAT('%d', t.TYPE_OID) || 
                CASE 
                WHEN t.TYPE_OID = ?1 THEN 
                    COALESCE(' (' || {}{}
                        GROUP_CONCAT(s.COL_NAME, ',' ORDER BY s.SUPERTYPE_OID) || 
                        ') VALUES (' || {}{}
                        GROUP_CONCAT(s.COL_VALUE_EXPRESSION, ',' ORDER BY s.SUPERTYPE_OID) ||
                        ')',
                        {}
                    ) 
                ELSE
                    COALESCE(' (' ||
                        GROUP_CONCAT(s.COL_NAME, ',' ORDER BY s.SUPERTYPE_OID) || 
                        ') VALUES (' || 
                        GROUP_CONCAT(s.COL_VALUE_EXPRESSION, ',' ORDER BY s.SUPERTYPE_OID) ||
                        ')',
                        ' DEFAULT VALUES'
                    ) 
                END AS INSERT_CMD
        FROM TYPE_QUERY t
        LEFT JOIN SUPERTYPE_QUERY s ON s.INHERITOR_TYPE_OID = t.TYPE_OID
        GROUP BY t.TYPE_OID
        ORDER BY 1 DESC
        ",
        match row_oid {
            Some(_) => "'OID, ' || ",
            None => ""
        },
        match parent_row_oid {
            Some(_) => "'PARENT_OID, ' || ",
            None => ""
        },
        match row_oid {
            Some(_) => "':t, ' || ",
            None => ""
        },
        match parent_row_oid {
            Some(_) => "':p, ' || ",
            None => ""
        },
        match row_oid {
            Some(_) => {
                match parent_row_oid {
                    Some(_) => "' (OID, PARENT_OID) VALUES (:t, :p)'",
                    None => "' (OID) VALUES (:t)'"
                }
            },
            None => {
                match parent_row_oid {
                    Some(_) => "' (PARENT_OID) VALUES (:p)'",
                    None => "' DEFAULT VALUES'"
                }
            }
        }
    );

    let mut select_supertype_statement = trans.prepare(&select_cmd)?;
    let existing_supertype_oids: Array = Array::new(match known_supertype_oids {
        Some(a) => a.iter().map(|alias| alias.type_oid.into()).collect(),
        None => Vec::new(),
    });
    let supertype_rows = select_supertype_statement.query_map(
        params![table_oid, existing_supertype_oids],
        |row| {
            Ok((
                row.get::<_, i64>("TYPE_OID")?,
                row.get::<_, String>("INSERT_CMD")?,
            ))
        },
    )?;

    for supertype_row_result in supertype_rows {
        let (type_oid, insert_cmd) = supertype_row_result.unwrap();

        let params: Vec<(&str, i64)> = type_row_oids
            .iter()
            .filter(|tup| insert_cmd.contains(&tup.0))
            .map(|tup| (tup.0.as_str(), tup.1))
            .collect();

        trans.execute(&insert_cmd, &*params)?;
        let type_row_oid: i64 = trans.last_insert_rowid();

        type_row_oids.push((format!(":m{type_oid}"), type_row_oid));
    }
    return Ok(type_row_oids.last().unwrap().1);
}

/// Flags a row as being trash.
fn trash_inplace(
    trans: &Transaction,
    table_oid: i64,
    row_oid: i64,
) -> Result<(i64, i64), error::Error> {
    // Check if there is a deeper subtype level that would also need to be trashed
    let mut select_immediate_subtype_statement = trans.prepare(
        "SELECT
        u.INHERITOR_TABLE_OID AS TYPE_OID
        FROM METADATA_TABLE_INHERITANCE u
        WHERE u.MASTER_TABLE_OID = ?1",
    )?;
    let immediate_subtype_rows = select_immediate_subtype_statement
        .query_map(params![table_oid], |row| row.get::<_, i64>("TYPE_OID"))?;
    for immediate_subtype_result in immediate_subtype_rows {
        let subtype_oid = immediate_subtype_result?;
        let select_subtype_row_cmd: String = format!(
            "SELECT OID FROM TABLE{subtype_oid} WHERE MASTER{table_oid}_OID = ?1 AND TRASH = 0"
        );
        match trans
            .query_one(&select_subtype_row_cmd, params![row_oid], |row| {
                row.get::<_, i64>("OID")
            })
            .optional()?
        {
            Some(subtype_row_oid) => {
                // Stop iteration at the first located subtype OID with a non-trash row associated with this table
                // Return the table OID and row OID of the deepest level that was trashed
                return trash_inplace(trans, subtype_oid, subtype_row_oid);
            }
            None => {}
        }
    }

    // Get every supertype
    let mut select_supertype_statement = trans.prepare(
        "
        WITH RECURSIVE TYPE_QUERY (LEVEL, TYPE_OID, SELECT_CMD) AS (
            SELECT 
                0 AS LEVEL,
                ?1 AS TYPE_OID,
                NULL AS SELECT_CMD
            UNION
            SELECT
                s.LEVEL + 1 AS LEVEL,
                u.MASTER_TABLE_OID AS TYPE_OID,
                'SELECT MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID FROM TABLE' || FORMAT('%d', s.TYPE_OID) || ' WHERE OID = :m' || FORMAT('%d', s.TYPE_OID) AS SELECT_CMD
            FROM TYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.TYPE_OID
            WHERE u.TRASH = 0
        )
        SELECT
            MAX(t.LEVEL) AS MAX_LEVEL,
            t.TYPE_OID,
            MAX(t.SELECT_CMD) AS SELECT_CMD,
            'UPDATE TABLE' || FORMAT('%d', t.TYPE_OID) || ' SET TRASH = 1 WHERE OID = :m' || FORMAT('%d', t.TYPE_OID) AS UPDATE_CMD
        FROM TYPE_QUERY t
        GROUP BY t.TYPE_OID
        ORDER BY 1 ASC
        "
    )?;
    let supertype_rows = select_supertype_statement.query_map(params![table_oid], |row| {
        Ok((
            row.get::<_, i64>("TYPE_OID")?,
            row.get::<_, Option<String>>("SELECT_CMD")?,
            row.get::<_, String>("UPDATE_CMD")?,
        ))
    })?;

    // This Vec collects the parameters mapping a table OID to the corresponding row OID in that table
    let mut type_row_oids: Vec<(String, i64)> = vec![(format!(":m{table_oid}"), row_oid)];

    // Mark as trash every row in a master list that this row depends on
    for supertype_row_result in supertype_rows {
        let (type_oid, select_cmd, update_cmd) = supertype_row_result.unwrap();

        // Get the row OID
        match select_cmd {
            Some(s) => {
                let temp_params: Vec<(&str, i64)> = type_row_oids
                    .iter()
                    .filter(|tup| s.contains(&tup.0))
                    .map(|tup| (tup.0.as_str(), tup.1))
                    .collect();
                let type_row_oid: i64 = trans.query_one(&s, &*temp_params, |row| row.get(0))?;
                type_row_oids.push((format!(":m{type_oid}"), type_row_oid));
            }
            None => {}
        }

        // Flag the row as being trash
        let params: Vec<(&str, i64)> = type_row_oids
            .iter()
            .filter(|tup| update_cmd.contains(&tup.0))
            .map(|tup| (tup.0.as_str(), tup.1))
            .collect();
        trans.execute(&update_cmd, &*params)?;
    }
    return Ok((table_oid, row_oid));
}

/// Unflags a row as being trash.
fn untrash_inplace(trans: &Transaction, table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut type_row_oids: Vec<(String, i64)> = vec![(format!(":m{table_oid}"), row_oid)];
    let mut select_supertype_statement = trans.prepare(
        "
        WITH RECURSIVE TYPE_QUERY (LEVEL, TYPE_OID, SELECT_CMD) AS (
            SELECT 
                0 AS LEVEL,
                ?1 AS TYPE_OID,
                NULL AS SELECT_CMD
            UNION
            SELECT
                s.LEVEL + 1 AS LEVEL,
                u.MASTER_TABLE_OID AS TYPE_OID,
                'SELECT MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID FROM TABLE' || FORMAT('%d', s.TYPE_OID) || ' WHERE OID = :m' || FORMAT('%d', s.TYPE_OID) AS SELECT_CMD
            FROM TYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.TYPE_OID
            WHERE u.TRASH = 0
        )
        SELECT
            MAX(t.LEVEL) AS MAX_LEVEL,
            t.TYPE_OID,
            MAX(t.SELECT_CMD) AS SELECT_CMD,
            'UPDATE TABLE' || FORMAT('%d', t.TYPE_OID) || ' SET TRASH = 0 WHERE OID = :m' || FORMAT('%d', t.TYPE_OID) AS UPDATE_CMD
        FROM TYPE_QUERY t
        GROUP BY t.TYPE_OID
        ORDER BY 1 ASC
        "
    )?;
    let supertype_rows = select_supertype_statement.query_map(params![table_oid], |row| {
        Ok((
            row.get::<_, i64>("TYPE_OID")?,
            row.get::<_, Option<String>>("SELECT_CMD")?,
            row.get::<_, String>("UPDATE_CMD")?,
        ))
    })?;

    for supertype_row_result in supertype_rows {
        let (type_oid, select_cmd, update_cmd) = supertype_row_result.unwrap();

        // Get the row OID
        match select_cmd {
            Some(s) => {
                let temp_params: Vec<(&str, i64)> = type_row_oids
                    .iter()
                    .filter(|tup| s.contains(&tup.0))
                    .map(|tup| (tup.0.as_str(), tup.1))
                    .collect();
                let type_row_oid: i64 = trans.query_one(&s, &*temp_params, |row| row.get(0))?;
                type_row_oids.push((format!(":m{type_oid}"), type_row_oid));
            }
            None => {}
        }

        // Unflag the row as being trash
        let params: Vec<(&str, i64)> = type_row_oids
            .iter()
            .filter(|tup| update_cmd.contains(&tup.0))
            .map(|tup| (tup.0.as_str(), tup.1))
            .collect();
        trans.execute(&update_cmd, &*params)?;
    }
    return Ok(());
}

/// Insert a row into the data such that the OID places it before any existing rows with that OID.
pub fn insert(table_oid: i64, parent_row_oid: Option<i64>, row_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // If OID is already in database, shift every row with OID >= row_oid up by 1
    let select_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID = ?1;");
    let existing_row_oid = trans
        .query_one(&select_cmd, params![row_oid], |row| {
            return Ok(row.get::<_, i64>(0)?);
        })
        .optional()?;

    match existing_row_oid {
        None => {
            // Insert with OID = row_oid
            let row_oid = insert_inplace(&trans, table_oid, parent_row_oid, Some(row_oid), None)?;

            // Return the row_oid
            trans.commit()?;
            return Ok(row_oid);
        }
        Some(_) => {
            let existing_prev_row_oid = trans
                .query_one(&select_cmd, params![row_oid - 1], |row| {
                    return Ok(row.get::<_, i64>(0)?);
                })
                .optional()?;

            match existing_prev_row_oid {
                None => {
                    // Insert with OID = row_oid - 1
                    let row_oid = insert_inplace(&trans, table_oid, parent_row_oid, Some(row_oid - 1), None)?;

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                }
                Some(_) => {
                    // Increment every OID >= row_oid up by 1 to make room for the new row
                    let select_all_cmd = format!(
                        "SELECT OID FROM TABLE{table_oid} WHERE OID >= ?1 ORDER BY OID DESC;"
                    );
                    db::query_iterate(&trans, &select_all_cmd, params![row_oid], &mut |row| {
                        let update_cmd =
                            format!("UPDATE TABLE{table_oid} SET OID = OID + 1 WHERE OID = ?1;");
                        trans.execute(&update_cmd, params![row.get::<_, i64>(0)?])?;
                        return Ok(());
                    })?;

                    // Insert the row
                    let row_oid = insert_inplace(&trans, table_oid, parent_row_oid, Some(row_oid), None)?;

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                }
            }
        }
    }
}

/// Push a row into the table with a default OID.
pub fn push(table_oid: i64, parent_row_oid: Option<i64>) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Insert the row
    let row_oid = insert_inplace(&trans, table_oid, parent_row_oid, None, None)?;

    // Return the row OID
    trans.commit()?;
    return Ok(row_oid);
}

/// Retypes the subtype of an row.
/// Returns the old subtype of the row.
pub fn retype(
    base_obj_type_oid: i64,
    base_obj_row_oid: i64,
    new_obj_type_oid: i64,
) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move any existing subtype rows to the trash
    let (old_obj_type_oid, _) =
        trash_inplace(&trans, base_obj_type_oid.clone(), base_obj_row_oid.clone())?;

    println!("Changing {base_obj_type_oid}:{base_obj_row_oid} from {old_obj_type_oid} to {new_obj_type_oid}");

    // This Vec collects the parameters mapping a table OID to the hierarchy level of that table and the corresponding row OID in that table
    let mut type_row_oids: Vec<RowOidParamAlias> = vec![RowOidParamAlias {
        type_oid: base_obj_type_oid,
        type_param_alias: format!(":m{base_obj_type_oid}"),
        level: i64::MAX,
        type_row_oid: base_obj_row_oid,
    }];
    // This Vec collects table parameter aliases where a row OID corresponding to the row in the base table does not exist, and thus any query dependent on that parameter will automatically fail
    let mut nonexistent_type_row_oids: Vec<String> = Vec::new();

    {
        // Get every supertype that is also a subtype of the base table
        let mut select_supertype_statement = trans.prepare(
            "
            WITH RECURSIVE SUBTYPE_QUERY (TYPE_OID) AS (
                SELECT
                    ?1 AS TYPE_OID
                UNION
                SELECT
                    u.INHERITOR_TABLE_OID AS TYPE_OID
                FROM SUBTYPE_QUERY s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                WHERE u.TRASH = 0
            ),
            SUPERTYPE_QUERY (LEVEL, TYPE_OID, SUPERTYPE_OID, WHERE_CLAUSE) AS (
                SELECT 
                    0 AS LEVEL,
                    u.INHERITOR_TABLE_OID AS TYPE_OID,
                    u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                    'MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID = :m' || FORMAT('%d', u.MASTER_TABLE_OID) AS WHERE_CLAUSE
                FROM METADATA_TABLE_INHERITANCE u
                WHERE u.TRASH = 0 AND u.INHERITOR_TABLE_OID = ?2
                UNION
                SELECT
                    s.LEVEL + 1 AS LEVEL,
                    u.INHERITOR_TABLE_OID AS TYPE_OID,
                    u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                    'MASTER' || FORMAT('%d', u.MASTER_TABLE_OID) || '_OID = :m' || FORMAT('%d', u.MASTER_TABLE_OID) AS WHERE_CLAUSE
                FROM SUPERTYPE_QUERY s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.SUPERTYPE_OID
                WHERE u.TRASH = 0 AND u.MASTER_TABLE_OID IN (SELECT * FROM SUBTYPE_QUERY)
            )
            SELECT
                MAX(t.LEVEL) AS MAX_LEVEL,
                t.TYPE_OID,
                'SELECT OID FROM TABLE' || FORMAT('%d', t.TYPE_OID) || ' WHERE ' || GROUP_CONCAT(t.WHERE_CLAUSE, ' AND ') AS SELECT_CMD
            FROM SUPERTYPE_QUERY t
            GROUP BY t.TYPE_OID
            ORDER BY 1 DESC
            "
        )?;
        let supertype_rows = select_supertype_statement.query_map(
            params![base_obj_type_oid, new_obj_type_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("MAX_LEVEL")?,
                    row.get::<_, i64>("TYPE_OID")?,
                    row.get::<_, String>("SELECT_CMD")?,
                ))
            },
        )?;

        // Find all existent rows inheriting from the given row in the base table
        for supertype_row_result in supertype_rows {
            let (level, type_oid, select_cmd) = supertype_row_result.unwrap();

            // Ensure that the select command does not depend on any nonexistent row OIDs
            if nonexistent_type_row_oids
                .iter()
                .any(|param_alias| select_cmd.contains(param_alias))
            {
                nonexistent_type_row_oids.push(format!(":m{type_oid}"));
                continue;
            }

            // Attempt to find a row in the subtype table corresponding to the row in the base table
            let params: Vec<(&str, i64)> = type_row_oids
                .iter()
                .filter(|alias| select_cmd.contains(&alias.type_param_alias))
                .map(|alias| (alias.type_param_alias.as_str(), alias.type_row_oid))
                .collect();
            match trans
                .query_one(&select_cmd, &*params, |row| row.get(0))
                .optional()?
            {
                Some(type_row_oid) => {
                    type_row_oids.push(RowOidParamAlias {
                        type_oid,
                        type_param_alias: format!(":m{type_oid}"),
                        level,
                        type_row_oid,
                    });
                }
                None => {
                    nonexistent_type_row_oids.push(format!(":m{type_oid}"));
                }
            };
        }
    }

    // Check if the new subtype already has an existing row
    if type_row_oids.last().unwrap().level == 0 {
        // If so, we unflag it as being trash, and we're done!
        untrash_inplace(
            &trans,
            new_obj_type_oid,
            type_row_oids.last().unwrap().type_row_oid,
        )?;
    } else {
        // If not, we create a new row
        let new_obj_row_oid: i64 =
            insert_inplace(&trans, new_obj_type_oid, None, None, Some(type_row_oids))?;

        // Move every master of the new row from the trash
        untrash_inplace(&trans, new_obj_type_oid, new_obj_row_oid)?;
    }

    // Commit the transaction
    trans.commit()?;
    return Ok(old_obj_type_oid);
}

/// Marks a row as trash.
pub fn trash(table_oid: i64, row_oid: i64) -> Result<(i64, i64), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move the row to the trash bin
    let (table_oid, row_oid) = trash_inplace(&trans, table_oid, row_oid)?;

    // Commit the transaction
    trans.commit()?;
    return Ok((table_oid, row_oid));
}

/// Unmarks a row as trash.
pub fn untrash(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move the row from the trash bin
    untrash_inplace(&trans, table_oid, row_oid)?;

    // Commit the transaction
    trans.commit()?;
    return Ok(());
}

/// Delete the row with the given OID.
pub fn delete(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Delete the row
    let delete_cmd = format!("DELETE FROM TABLE{table_oid} WHERE OID = ?1;");
    trans.execute(&delete_cmd, params![row_oid])?;

    // Return the row OID
    trans.commit()?;
    return Ok(());
}

/// Attempts to update a value represented by a primitive in a table.
/// This applies to primitive types, single-select dropdown types, reference types, and object types.
/// Returns the previous value of the cell.
pub fn try_update_primitive_value(
    table_oid: i64,
    row_oid: i64,
    column_oid: i64,
    mut new_value: Option<String>,
) -> Result<Option<String>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Verify that the column has a primitive type
    let column_type = trans.query_one(
        "SELECT
            c.TYPE_OID,
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.OID = ?1",
        params![column_oid],
        |row| {
            Ok(data_type::MetadataColumnType::from_database(
                row.get("TYPE_OID")?,
                row.get("MODE")?,
            ))
        },
    )?;
    match column_type {
        data_type::MetadataColumnType::Primitive(prim) => {
            match prim {
                data_type::Primitive::JSON => {
                    // If column has JSON type, validate the JSON
                    match new_value.clone() {
                        Some(json_str) => match serde_json::from_str::<serde_json::Value>(&*json_str) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Unable to parse JSON: {e}");
                                return Err(error::Error::AdhocError(
                                    "The provided value is invalid JSON.",
                                ));
                            }
                        },
                        None => {}
                    }
                }
                data_type::Primitive::Integer => {
                    match new_value.clone() {
                        Some(num_str) => {
                            // If the value provided is a floating-point number, truncate it into an integer
                            let num: f64 = match num_str.parse() {
                                Ok(n) => n,
                                Err(_) => {
                                    return Err(error::Error::AdhocError(
                                        "The provided value cannot be converted into an integer.",
                                    ));
                                }
                            };
                            new_value = Some(format!("{}", num as i64));
                        }
                        None => {}
                    }
                }
                data_type::Primitive::Date => match new_value.clone() {
                    Some(date_str) => {
                        let date: Date = match Date::parse(&date_str, &well_known::Iso8601::DATE) {
                            Ok(d) => d,
                            Err(_) => {
                                return Err(error::Error::AdhocError(
                                    "The provided value cannot be converted into a date.",
                                ));
                            }
                        };
                        new_value = Some(format!("{}", date.to_julian_day()));
                    }
                    None => {}
                },
                data_type::Primitive::Timestamp => match new_value.clone() {
                    Some(timestamp_str) => {
                        let timestamp: UtcDateTime = match UtcDateTime::parse(
                            &timestamp_str,
                            &well_known::Iso8601::DATE_TIME,
                        ) {
                            Ok(d) => d,
                            Err(_) => {
                                return Err(error::Error::AdhocError(
                                    "The provided value cannot be converted into a timestamp.",
                                ));
                            }
                        };
                        let julian_day: i32 = timestamp.to_julian_day();
                        let dur_numerator = timestamp
                            - UtcDateTime::new(
                                Date::from_julian_day(julian_day).unwrap(),
                                time!(12:00),
                            );
                        let dur_denominator = UtcDateTime::new(
                            Date::from_julian_day(julian_day + 1).unwrap(),
                            time!(12:00),
                        ) - UtcDateTime::new(
                            Date::from_julian_day(julian_day).unwrap(),
                            time!(12:00),
                        );
                        let julian_fraction: f64 = (julian_day as f64)
                            + (dur_numerator.as_seconds_f64() / dur_denominator.as_seconds_f64());
                        new_value = Some(format!("{}", julian_fraction));
                    }
                    None => {}
                },
                _ => {}
            }
            // Ignore other primitive types
        }
        data_type::MetadataColumnType::MultiSelectDropdown(_)
        | data_type::MetadataColumnType::ChildTable(_) => {
            return Err(error::Error::AdhocError(
                "Value of column cannot be updated like a primitive value.",
            ));
        }
        _ => {
            // Ignore the rest
        }
    }

    // Retrieve the previous value
    let select_prev_value_cmd = format!("SELECT CAST(COLUMN{column_oid} AS TEXT) AS PRIOR_VALUE FROM TABLE{table_oid} WHERE OID = ?1;");
    let prev_value: Option<String> =
        trans.query_one(&select_prev_value_cmd, params![row_oid], |row| {
            return Ok(row.get::<_, Option<String>>(0)?);
        })?;

    // Update the value
    let update_cmd = format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2;");
    trans.execute(&update_cmd, params![new_value, row_oid])?;

    // Return OK
    trans.commit()?;
    return Ok(prev_value);
}

/// Updates a BLOB column with a BLOB value.
pub fn try_update_blob_value(table_oid: i64, row_oid: i64, column_oid: i64, path: String) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    println!("Uploading file from {path} to TABLE{table_oid} COLUMN{column_oid} OID = {row_oid}");

    // Load the file from the filesystem
    let buf = match std::fs::read(path) {
        Ok(read_buf) => read_buf,
        Err(_) => {
            return Err(error::Error::AdhocError("Unable to open file."));
        }
    };
    let cropped_file_len: i64 = match i64::try_from(buf.len()) {
        Ok(l) => l,
        Err(_) => {
            return Err(error::Error::AdhocError("File size is greater than 9,223,372,036,854,775,807 bytes."));
        }
    };

    // Update the value with an empty blob
    let update_cmd = format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ZEROBLOB(?1) WHERE OID = ?2;");
    trans.execute(&update_cmd, params![cropped_file_len, row_oid])?;

    // Fill the empty blob with the data from the file
    {
        let table_name: String = format!("TABLE{table_oid}");
        let column_name: String = format!("COLUMN{column_oid}");
        let mut blob = trans.blob_open("main", &*table_name, &*column_name, row_oid, false)?;
        match blob.write_all(&buf) {
            Ok(_) => {},
            Err(_) => {
                return Err(error::Error::AdhocError("Unable to upload file contents to database."));
            }
        }
    }

    // Commit the transaction
    trans.commit()?;
    return Ok(());
}

/// Creates a row in the object table associated with a cell in the base table.
pub fn set_table_object_value(
    table_oid: i64,
    row_oid: i64,
    column_oid: i64,
    obj_type_oid: Option<i64>,
    obj_row_oid: Option<i64>,
) -> Result<(i64, i64), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    if obj_type_oid == None || obj_row_oid == None {
        // Verify that the column is an object
        let column_type = trans.query_one(
            "SELECT
                c.TYPE_OID,
                t.MODE
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1",
            params![column_oid],
            |row| {
                Ok(data_type::MetadataColumnType::from_database(
                    row.get("TYPE_OID")?,
                    row.get("MODE")?,
                ))
            },
        )?;
        match column_type {
            data_type::MetadataColumnType::ChildObject(obj_type_oid) => {
                // Insert a new row into the object table
                let obj_row_oid = insert_inplace(&trans, obj_type_oid, None, None, None)?;

                // Update the value in the base table
                let update_cmd =
                    format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2;");
                trans.execute(&update_cmd, params![obj_row_oid, row_oid])?;

                // Commit the transaction
                trans.commit()?;
                return Ok((obj_type_oid, obj_row_oid));
            }
            _ => {
                return Err(error::Error::AdhocError("Column is not an object type."));
            }
        }
    } else {
        // Move the object from the trash
        untrash_inplace(&trans, obj_type_oid.unwrap(), obj_row_oid.unwrap())?;
        // Set the column in the table to the object's OID
        let update_cmd: String =
            format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2");
        trans.execute(&update_cmd, params![obj_row_oid.unwrap(), row_oid])?;

        // Commit the transaction
        trans.commit()?;
        return Ok((obj_type_oid.unwrap(), obj_row_oid.unwrap()));
    }
}

/// Creates a row in the object table associated with a cell in the base table.
pub fn unset_table_object_value(
    table_oid: i64,
    row_oid: i64,
    column_oid: i64,
    obj_type_oid: i64,
    obj_row_oid: i64,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move the object to the trash
    trash_inplace(&trans, obj_type_oid, obj_row_oid)?;
    // Set the column in the table to NULL
    let update_cmd: String =
        format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = NULL WHERE OID = ?1");
    trans.execute(&update_cmd, params![row_oid])?;

    // Commit the transaction
    trans.commit()?;
    return Ok(());
}

struct Column {
    true_ord: Option<String>,
    display_ord: String,
    table_oid: i64,
    row_ord: String,
    column_oid: i64,
    column_name: String,
    column_type: data_type::MetadataColumnType,
    column_ordering: i64,
    is_nullable: bool,
    is_primary_key: bool,
    invalid_nonunique_oid: HashSet<i64>,
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(
    trans: &Transaction,
    table_oid: i64,
    include_row_oid_clause: bool,
    include_parent_row_oid_clause: bool,
) -> Result<(String, LinkedList<Column>), error::Error> {
    // Build the SELECT query
    let (mut select_cols_cmd, mut select_tbls_cmd) = trans.query_one(
        "
        WITH RECURSIVE SUPERTYPE_QUERY (LEVEL, FINAL_TYPE_OID, SUPERTYPE_OID, INHERITOR_TYPE_OID) AS (
            SELECT
                1 AS LEVEL,
                u.INHERITOR_TABLE_OID AS FINAL_TYPE_OID,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID
            FROM METADATA_TABLE_INHERITANCE u
            INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = u.MASTER_TABLE_OID
            WHERE u.TRASH = 0 AND tbl.TRASH = 0 AND u.INHERITOR_TABLE_OID = ?1
            UNION
            SELECT
                s.LEVEL + 1 AS LEVEL,
                s.FINAL_TYPE_OID,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID
            FROM SUPERTYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.SUPERTYPE_OID
            INNER JOIN METADATA_TABLE tbl ON tbl.TYPE_OID = u.MASTER_TABLE_OID
            WHERE u.TRASH = 0 AND tbl.TRASH = 0
        ),
        CONDENSED_SUPERTYPE_QUERY (MAX_LEVEL, FINAL_TYPE_OID, SUPERTYPE_OID, COL_EXPRESSION, JOIN_CLAUSE) AS (
            SELECT
                0 AS MAX_LEVEL,
                ?1 AS FINAL_TYPE_OID,
                ?1 AS SUPERTYPE_OID,
                't.OID AS t_OID' AS COL_EXPRESSION,
                'FROM TABLE' || FORMAT('%d', ?1) || ' t' AS JOIN_CLAUSE
            UNION
            SELECT
                MAX(LEVEL) AS MAX_LEVEL,
                FINAL_TYPE_OID,
                SUPERTYPE_OID,
                'm' || FORMAT('%d', SUPERTYPE_OID) || '.OID AS m' || FORMAT('%d', SUPERTYPE_OID) || '_OID' AS COL_EXPRESSION,
                'INNER JOIN TABLE' || FORMAT('%d', SUPERTYPE_OID) || ' m' || FORMAT('%d', SUPERTYPE_OID) || ' ON ' || GROUP_CONCAT(
                    CASE WHEN INHERITOR_TYPE_OID = FINAL_TYPE_OID THEN 't'
                    ELSE 'm' || FORMAT('%d', INHERITOR_TYPE_OID)
                    END || '.MASTER' || FORMAT('%d', SUPERTYPE_OID) || '_OID = m' || FORMAT('%d', SUPERTYPE_OID) || '.OID',
                    ' AND '
                ) AS JOIN_CLAUSE
            FROM SUPERTYPE_QUERY
            GROUP BY
                FINAL_TYPE_OID,
                SUPERTYPE_OID
        )
        SELECT
            'ROW_NUMBER() OVER (ORDER BY t.OID) AS ROW_INDEX, ' || GROUP_CONCAT(COL_EXPRESSION, ', ') AS OID_CLAUSE,
            GROUP_CONCAT(JOIN_CLAUSE, ' ' ORDER BY MAX_LEVEL ASC) AS FROM_CLAUSE
        FROM CONDENSED_SUPERTYPE_QUERY
        GROUP BY FINAL_TYPE_OID
        ", 
        params![table_oid], 
        |row| { 
            Ok((row.get("OID_CLAUSE")?, row.get("FROM_CLAUSE")?))
        }
    )?;
    let mut columns = LinkedList::<Column>::new();
    let mut tbl_count: usize = 1;

    db::query_iterate(
        trans,
        "WITH RECURSIVE SUPERTYPE_QUERY (TYPE_OID) AS (
            SELECT
                ?1
            UNION
            SELECT
                u.MASTER_TABLE_OID AS TYPE_OID
            FROM SUPERTYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.TYPE_OID
            WHERE u.TRASH = 0
        )
        SELECT 
            c.OID,
            c.TABLE_OID,
            c.TYPE_OID,
            t.MODE,
            c.IS_NULLABLE,
            c.IS_UNIQUE,
            c.IS_PRIMARY_KEY,
            c.NAME,
            c.COLUMN_ORDERING
        FROM SUPERTYPE_QUERY s
        INNER JOIN METADATA_TABLE_COLUMN c ON s.TYPE_OID = c.TABLE_OID
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid],
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let column_type: data_type::MetadataColumnType =
                data_type::MetadataColumnType::from_database(
                    row.get("TYPE_OID")?,
                    row.get("MODE")?,
                );
            let column_ordering: i64 = row.get("COLUMN_ORDERING")?;

            let column_source_table_oid: i64 = row.get("TABLE_OID")?;
            let source_alias: String = if column_source_table_oid == table_oid {
                String::from("t")
            } else {
                format!("m{column_source_table_oid}")
            };

            let enforce_uniqueness: bool = row.get("IS_UNIQUE")?;
            let mut invalid_nonunique_oid: HashSet<i64> = HashSet::<i64>::new();

            let display_ord: String = format!("COLUMN{column_oid}");
            let true_ord: Option<String>;
            match &column_type {
                data_type::MetadataColumnType::Primitive(prim) => {
                    // Primitive type
                    match prim {
                        data_type::Primitive::Any
                        | data_type::Primitive::Boolean
                        | data_type::Primitive::Integer
                        | data_type::Primitive::Number
                        | data_type::Primitive::Text
                        | data_type::Primitive::JSON => {
                            select_cols_cmd = format!("{select_cols_cmd}, CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS COLUMN{column_oid}");
                        }
                        data_type::Primitive::Date => {
                            select_cols_cmd = format!("{select_cols_cmd}, DATE({source_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN{column_oid}");
                        }
                        data_type::Primitive::Timestamp => {
                            select_cols_cmd = format!("{select_cols_cmd}, STRFTIME('%FT%TZ', {source_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN{column_oid}");
                        }
                        data_type::Primitive::File => {
                            select_cols_cmd = format!("{select_cols_cmd}, CASE 
                            WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL 
                            ELSE 
                                CASE 
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000000001)
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.001)
                                END
                            END AS COLUMN{column_oid}");
                        }
                        data_type::Primitive::Image => {
                            select_cols_cmd = format!("{select_cols_cmd}, CASE WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS COLUMN{column_oid}");
                        }
                    }
                    true_ord = Some(display_ord.clone());

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!(
                            "
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        "
                        );
                        db::query_iterate(trans, &check_nonunique_cmd, [], &mut |row| {
                            invalid_nonunique_oid.insert(row.get(0)?);
                            return Ok(());
                        })?;
                    }
                }
                data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, t{tbl_count}.VALUE AS COLUMN{column_oid}, CAST(t{tbl_count}.OID AS TEXT) AS _COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = {source_alias}.COLUMN{column_oid}");
                    tbl_count += 1;
                    true_ord = Some(format!("_COLUMN{column_oid}"));

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!(
                            "
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        "
                        );
                        db::query_iterate(trans, &check_nonunique_cmd, [], &mut |row| {
                            invalid_nonunique_oid.insert(row.get(0)?);
                            return Ok(());
                        })?;
                    }
                }
                data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, 
                        (SELECT 
                            '[' || GROUP_CONCAT(b.VALUE) || ']' 
                        FROM TABLE{column_type_oid}_MULTISELECT a 
                        INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                        WHERE a.ROW_OID = {source_alias}.OID GROUP BY a.ROW_OID) AS COLUMN{column_oid},
                        (SELECT 
                            GROUP_CONCAT(CAST(b.OID AS TEXT))
                        FROM TABLE{column_type_oid}_MULTISELECT a 
                        INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                        WHERE a.ROW_OID = {source_alias}.OID GROUP BY a.ROW_OID) AS _COLUMN{column_oid}
                        ");
                    true_ord = Some(format!("_COLUMN{column_oid}"));

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!(
                            "
                            WITH TABLE_SURROGATE AS (
                                SELECT 
                                    ROW_OID,
                                    GROUP_CONCAT(CAST(VALUE_OID AS TEXT)) AS COLUMN{column_oid}
                                FROM TABLE{column_type_oid}_MULTISELECT 
                                GROUP BY OID
                            )
                            SELECT t.ROW_OID AS OID FROM TABLE_SURROGATE t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE_SURROGATE
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        "
                        );
                        db::query_iterate(trans, &check_nonunique_cmd, [], &mut |row| {
                            invalid_nonunique_oid.insert(row.get(0)?);
                            return Ok(());
                        })?;
                    }
                }
                data_type::MetadataColumnType::Reference(referenced_table_oid)
                | data_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, t{tbl_count}.DISPLAY_VALUE AS COLUMN{column_oid}, CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS _COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = {source_alias}.COLUMN{column_oid}");
                    tbl_count += 1;
                    true_ord = Some(format!("_COLUMN{column_oid}"));

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!(
                            "
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        "
                        );
                        db::query_iterate(trans, &check_nonunique_cmd, [], &mut |row| {
                            invalid_nonunique_oid.insert(row.get(0)?);
                            return Ok(());
                        })?;
                    }
                }
                data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, (SELECT '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' FROM TABLE{column_type_oid}_SURROGATE a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.OID WHERE b.PARENT_OID = {source_alias}.OID GROUP BY b.PARENT_OID) AS COLUMN{column_oid}");
                    true_ord = None;
                }
            }

            // Push the column information
            columns.push_back(Column {
                true_ord: true_ord,
                display_ord: display_ord,
                table_oid: column_source_table_oid,
                row_ord: format!("{source_alias}_OID"),
                column_oid: column_oid,
                column_name: row.get("NAME")?,
                column_type: column_type,
                column_ordering,
                is_nullable: row.get("IS_NULLABLE")?,
                invalid_nonunique_oid: invalid_nonunique_oid,
                is_primary_key: row.get("IS_PRIMARY_KEY")?,
            });
            return Ok(());
        },
    )?;
    return Ok((
        format!(
            "SELECT {select_cols_cmd} {select_tbls_cmd} WHERE t.TRASH = 0 {}",
            if include_row_oid_clause {
                "AND t.OID = ?1"
            } else if include_parent_row_oid_clause {
                "AND t.PARENT_OID = ?1 LIMIT ?2 OFFSET ?3"
            } else {
                "LIMIT ?1 OFFSET ?2"
            }
        ),
        columns,
    ));
}

/// Sends all cells for the table through a channel.
pub fn send_table_data(
    table_oid: i64,
    parent_row_oid: Option<i64>,
    page_num: i64,
    page_size: i64,
    cell_channel: Channel<Cell>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(
        &trans,
        table_oid,
        false,
        match parent_row_oid {
            Some(_) => true,
            None => false,
        },
    )?;
    let table_select_cmd_params = match parent_row_oid {
        Some(o) => params![o.clone(), page_size, page_size * (page_num - 1)],
        None => params![page_size, page_size * (page_num - 1)],
    };

    // Iterate over the results, sending each cell to the frontend
    db::query_iterate(
        &trans,
        &table_select_cmd,
        table_select_cmd_params,
        &mut |row| {
            // Start by sending the index and OID, which are the first and second ordinal respectively
            let row_index: i64 = row.get("ROW_INDEX")?;
            cell_channel.send(Cell::RowStart {
                row_oid: row.get("t_OID")?,
                row_index: row_index,
            })?;

            let invalid_key: bool = false; // TODO

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let row_oid: i64 = row.get(&*column.row_ord)?;

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None,
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> =
                    Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name),
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name),
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!"),
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(Cell::ColumnValue {
                    table_oid: column.table_oid,
                    row_oid: row_oid,
                    column_oid: column.column_oid,
                    column_name: column.column_name.clone(),
                    column_type: column.column_type.clone(),
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations,
                })?;
            }

            // Conclude the row's iteration
            return Ok(());
        },
    )?;
    return Ok(());
}

/// Sends all cells for a row in the table through a channel.
pub fn send_table_row(
    table_oid: i64,
    row_oid: i64,
    cell_channel: Channel<RowCell>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, true, false)?;

    println!("{table_select_cmd}");

    // Query for the specified row
    match trans.query_row_and_then(
        &table_select_cmd,
        params![row_oid],
        |row| -> Result<(), error::Error> {
            // Start by sending message that confirms the row exists
            cell_channel.send(RowCell::RowExists {
                row_exists: true,
                table_oid,
            })?;

            let invalid_key = false;

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let row_oid: i64 = row.get(&*column.row_ord)?;

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None,
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> =
                    Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name),
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name),
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!"),
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(RowCell::ColumnValue {
                    table_oid: column.table_oid,
                    row_oid: row_oid,
                    column_oid: column.column_oid,
                    column_name: column.column_name.clone(),
                    column_type: column.column_type.clone(),
                    column_ordering: column.column_ordering,
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations,
                })?;
            }

            //
            return Ok(());
        },
    ) {
        Err(error::Error::RusqliteError(e)) => match e {
            RusqliteError::QueryReturnedNoRows => {
                cell_channel.send(RowCell::RowExists {
                    row_exists: false,
                    table_oid,
                })?;
                return Ok(());
            }
            _ => {
                return Err(error::Error::from(e));
            }
        },
        Err(e) => {
            return Err(e);
        }
        Ok(_) => {
            return Ok(());
        }
    }
}

/// Extract the contents of a BLOB into a base64 string.
pub fn get_blob_value(table_oid: i64, row_oid: i64, column_oid: i64) -> Result<String, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Construct a BLOB IO object
    let table_name: String = format!("TABLE{table_oid}");
    let column_name: String = format!("COLUMN{column_oid}");
    let blob = trans.blob_open("main", &*table_name, &*column_name, row_oid, true)?;

    // Read the BLOB into a buffer
    let mut buf_reader = BufReader::new(blob);
    let mut buf: Vec<u8> = Vec::new();
    match buf_reader.read_to_end(&mut buf) {
        Ok(_) => {},
        Err(_) => {
            return Err(error::Error::AdhocError("Unable to read stored file."));
        }
    }

    // Encode in base64
    return Ok(base64standard.encode(&buf));
}


/// Download the contents of a BLOB to a file.
pub fn download_blob_value(table_oid: i64, row_oid: i64, column_oid: i64, path: String) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Load the file from the filesystem
    let mut file = match File::create(path) {
        Ok(f) => f,
        Err(_) => {
            return Err(error::Error::AdhocError("Unable to open file."));
        }
    };

    // Construct a BLOB IO object
    let table_name: String = format!("TABLE{table_oid}");
    let column_name: String = format!("COLUMN{column_oid}");
    let blob = trans.blob_open("main", &*table_name, &*column_name, row_oid, true)?;

    // Read the BLOB into a buffer
    let mut buf_reader = BufReader::new(blob);
    let mut buf: Vec<u8> = Vec::new();
    match buf_reader.read_to_end(&mut buf) {
        Ok(_) => {},
        Err(_) => {
            return Err(error::Error::AdhocError("Unable to read stored file."));
        }
    }

    // Write the contents of the buffer into the file
    match file.write_all(&buf) {
        Ok(_) => {},
        Err(_) => {
            return Err(error::Error::AdhocError("Unable to write to file."));
        }
    }

    return Ok(());
}
