use std::cell::Ref;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Row, Error as RusqliteError, OptionalExtension};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use crate::backend::{data_type, db, table};
use crate::util::error;


#[derive(Serialize)]
#[serde(rename_all="camelCase")]
/// The most bare-bones version of table column metadata, used solely for populating the list of table columns
pub struct Metadata {
    oid: i64,
    name: String,
    column_ordering: i64,
    column_style: String,
    column_type: data_type::MetadataColumnType,
    is_nullable: bool,
    is_unique: bool,
    is_primary_key: bool,
}

/// Create a column based on a formula.
/// This may include columns that are just a static reference to a column in a table.
pub fn create_formula(report_oid: i64, column_name: &str, column_ordering: Option<i64>, column_style: &str, column_formula: &str) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_RPT_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE RPT_OID = ?1 AND COLUMN_ORDERING >= ?2;",
                params![report_oid, o]
            )?;
            o
        },
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING), 0) AS NEW_COLUMN_ORDERING FROM METADATA_RPT_COLUMN WHERE RPT_OID = ?1", 
                params![report_oid], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    // Create the metadata for the column
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN (NAME, COLUMN_ORDERING, CSS_COLUMN_STYLE) VALUES (?1, ?2, ?3);",
        params![column_name, column_ordering, column_style]
    )?;
    let column_oid: i64 = trans.last_insert_rowid();

    // Create the metadata for the formula
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN__FORMULA (RPT_COLUMN_OID, FORMULA) VALUES (?1, ?2);",
        params![column_oid, column_formula]
    )?;

    // Return the OID of the created column
    return Ok(column_oid);
}

/// Creates a column that is a report on rows linked to a row in the original report.
pub fn create_subreport(report_oid: i64, column_name: &str, column_ordering: Option<i64>, column_style: &str, base_parameter_oid: i64) -> Result<(i64, i64), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_RPT_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE RPT_OID = ?1 AND COLUMN_ORDERING >= ?2;",
                params![report_oid, o]
            )?;
            o
        },
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING), 0) AS NEW_COLUMN_ORDERING FROM METADATA_RPT_COLUMN WHERE RPT_OID = ?1", 
                params![report_oid], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    // Create the metadata for the column
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN (NAME, COLUMN_ORDERING, CSS_COLUMN_STYLE) VALUES (?1, ?2, ?3);",
        params![column_name, column_ordering, column_style]
    )?;
    let column_oid: i64 = trans.last_insert_rowid();

    // Create the metadata for the subreport
    trans.execute(
        "INSERT INTO METADATA_RPT DEFAULT VALUES;",
        []
    )?;
    let subreport_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN__SUBREPORT (RPT_COLUMN_OID, RPT_OID, RPT_PARAMETER_OID) VALUES (?1, ?2, ?3);",
        params![column_oid, subreport_oid, base_parameter_oid]
    )?;

    return Ok((column_oid, subreport_oid));
}

/// Flags a column as being trash.
pub fn move_trash(rpt_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the report as trash
    trans.execute("UPDATE METADATA_RPT_COLUMN SET TRASH = 1 WHERE OID = ?1;", params![column_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a column as being trash.
pub fn unmove_trash(table_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Unflag the report as trash
    trans.execute("UPDATE METADATA_RPT_COLUMN SET TRASH = 0 WHERE OID = ?1;", params![column_oid])?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}


/// Get the metadata for a particular column.
pub fn get_metadata(column_oid: i64) -> Result<Option<Metadata>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    return Ok(trans.query_one(
        "SELECT 
                c.OID, 
                c.NAME,
                c.COLUMN_ORDERING, 
                c.COLUMN_CSS_STYLE,
                c.TYPE_OID, 
                t.MODE,
                c.IS_NULLABLE,
                c.IS_UNIQUE,
                c.IS_PRIMARY_KEY
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1 
            ORDER BY c.COLUMN_ORDERING ASC;",
         params![column_oid], 
        |row| {
            return Ok(Metadata {
                oid: row.get("OID")?,
                name: row.get("NAME")?,
                column_ordering: row.get("COLUMN_ORDERING")?,
                column_style: row.get("COLUMN_CSS_STYLE")?,
                column_type: data_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?),
                is_nullable: row.get("IS_NULLABLE")?,
                is_unique: row.get("IS_UNIQUE")?,
                is_primary_key: row.get("IS_PRIMARY_KEY")?,
            });
        }
    ).optional()?);
}

/// Send a metadata list of columns.
pub fn send_metadata_list(table_oid: i64, column_channel: Channel<Metadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(&trans,
        "SELECT 
                c.OID, 
                c.NAME, 
                c.COLUMN_ORDERING,
                c.COLUMN_CSS_STYLE,
                c.TYPE_OID, 
                t.MODE,
                c.IS_NULLABLE,
                c.IS_UNIQUE,
                c.IS_PRIMARY_KEY
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.TABLE_OID = ?1 AND c.TRASH = 0
            ORDER BY c.COLUMN_ORDERING ASC;",
         params![table_oid], 
        &mut |row| {
            column_channel.send(Metadata {
                oid: row.get("OID")?,
                name: row.get("NAME")?,
                column_ordering: row.get("COLUMN_ORDERING")?,
                column_style: row.get("COLUMN_CSS_STYLE")?,
                column_type: data_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?),
                is_nullable: row.get("IS_NULLABLE")?,
                is_unique: row.get("IS_UNIQUE")?,
                is_primary_key: row.get("IS_PRIMARY_KEY")?,
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}


#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase")]
/// A value for a dropdown (i.e. single-select dropdown, multi-select dropdown, reference).
pub struct DropdownValue {
    true_value: Option<String>,
    display_value: Option<String>
}

/// Sets the possible values for a dropdown column.
pub fn set_table_column_dropdown_values(column_oid: i64, dropdown_values: Vec<DropdownValue>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    match trans.query_one(
        "SELECT 
                c.TYPE_OID, 
                t.MODE
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1;",
         params![column_oid], 
        |row| {
            return Ok(data_type::MetadataColumnType::from_database(
                row.get(0)?, 
                row.get(1)?
            ));
        }
    )? {
        data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) 
        | data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
            // Flag all values in the corresponding table as trash
            let flag_cmd = format!("UPDATE TABLE{column_type_oid} SET TRASH = 1;");
            trans.execute(&flag_cmd, [])?;

            // Insert the new values
            for dropdown_value in dropdown_values.iter() {
                match &dropdown_value.true_value {
                    Some(dropdown_oid_str) => {
                        let dropdown_oid: i64 = match str::parse(&dropdown_oid_str) {
                            Ok(o) => o,
                            Err(_) => { return Err(error::Error::AdhocError("Unable to parse dropdown value OID as integer.")); }
                        };
                        let update_cmd = format!("
                        UPDATE TABLE{column_type_oid} 
                        SET 
                            OID = (SELECT MAX(OID) AS NEW_OID FROM TABLE{column_type_oid}) + 1, 
                            VALUE = ?1
                        WHERE OID = ?2;");
                        trans.execute(&update_cmd, params![dropdown_value.display_value, dropdown_oid])?;
                    },
                    None => {
                        let insert_cmd = format!("INSERT INTO TABLE{column_type_oid} (VALUE) VALUES (?1);");
                        trans.execute(&insert_cmd, params![dropdown_value.display_value])?;
                    }
                }
            }
        },
        _ => {}
    };
    return Ok(());
}

/// Retrieves the list of allowed dropdown values for a column.
pub fn get_table_column_dropdown_values(column_oid: i64) -> Result<Vec<DropdownValue>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let mut dropdown_values: Vec<DropdownValue> = Vec::new();
    match trans.query_one(
        "SELECT 
                c.TYPE_OID, 
                t.MODE
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1;",
         params![column_oid], 
        |row| {
            return Ok(data_type::MetadataColumnType::from_database(
                row.get(0)?, 
                row.get(1)?
            ));
        }
    )? {
        data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) 
        | data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
            // Select the values from the corresponding table
            let select_cmd = format!("SELECT VALUE FROM TABLE{column_type_oid};");
            db::query_iterate(&trans, 
                &select_cmd, 
                [], 
            &mut |row| {
                dropdown_values.push(DropdownValue { 
                    true_value: row.get::<_, Option<String>>(0)?, 
                    display_value: row.get::<_, Option<String>>(0)? 
                });
                return Ok(());
            })?;
        },
        _ => {}
    };
    return Ok(dropdown_values);
}

/// Retrieves the list of allowed dropdown values for a column.
pub fn send_table_column_dropdown_values(column_oid: i64, dropdown_value_channel: Channel<DropdownValue>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    match trans.query_one(
        "SELECT 
                c.TYPE_OID, 
                t.MODE
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1;",
         params![column_oid], 
        |row| {
            return Ok(data_type::MetadataColumnType::from_database(
                row.get(0)?, 
                row.get(1)?
            ));
        }
    )? {
        data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) 
        | data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
            // Select the values from the corresponding table
            let select_cmd = format!("SELECT VALUE FROM TABLE{column_type_oid};");
            db::query_iterate(&trans, 
                &select_cmd, 
                [], 
            &mut |row| {
                dropdown_value_channel.send(DropdownValue { 
                    true_value: row.get::<_, Option<String>>(0)?, 
                    display_value: row.get::<_, Option<String>>(0)? 
                })?;
                return Ok(());
            })?;
        },
        data_type::MetadataColumnType::Reference(referenced_table_oid) => {
            // Select the values from the TABLE0_SURROGATE view
            let select_cmd = format!("SELECT CAST(OID AS TEXT) AS OID, DISPLAY_VALUE FROM TABLE{referenced_table_oid}_SURROGATE;");
            db::query_iterate(&trans, 
                &select_cmd, 
                [], 
            &mut |row| {
                dropdown_value_channel.send(DropdownValue { 
                    true_value: row.get::<_, Option<String>>("OID")?, 
                    display_value: row.get::<_, Option<String>>("DISPLAY_VALUE")? 
                })?;
                return Ok(());
            })?;
        },
        _ => {}
    };
    return Ok(());
}


#[derive(Serialize)]
pub struct BasicTypeMetadata {
    oid: i64,
    name: String
}

/// Send a list of basic metadata for a particular kind of type with associated tables (i.e. Reference, ChildObject, ChildTable).
pub fn send_type_metadata_list(column_type: data_type::MetadataColumnType, type_channel: Channel<BasicTypeMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(&trans, 
        "SELECT 
            tbl.OID,
            tbl.OID AS PARENT_OID,
            tbl.NAME
        FROM METADATA_TABLE tbl
        INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.OID
        WHERE typ.MODE = ?1
        ORDER BY tbl.NAME;", 
        [column_type.get_type_mode()], 
        &mut |row| {
            type_channel.send(BasicTypeMetadata {
                oid: row.get("OID")?,
                name: row.get("NAME")?
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}