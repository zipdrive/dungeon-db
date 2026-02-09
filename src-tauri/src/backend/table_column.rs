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

/// Creates a new column in a table.
pub fn create(table_oid: i64, column_name: &str, column_type: data_type::MetadataColumnType, column_ordering: Option<i64>, column_style: &str, is_nullable: bool, is_unique: bool, is_primary_key: bool) -> Result<i64, error::Error> {
    let is_nullable_bit = if is_nullable { 1 } else { 0 };
    let is_unique_bit = if is_unique { 1 } else { 0 };
    let is_primary_key_bit = if is_primary_key { 1 } else { 0 };

    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE COLUMN_ORDERING >= ?1;",
                params![o]
            )?;
            o
        },
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING) + 1, 0) AS NEW_COLUMN_ORDERING FROM METADATA_TABLE_COLUMN", 
                [], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    let column_type = column_type.create_for_table(&trans, &table_oid)?;
    match &column_type {
        data_type::MetadataColumnType::Primitive(prim) => {
            // Add the column to the table's metadata
            trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME,TYPE_OID, COLUMN_ORDERING, COLUMN_CSS_STYLE, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                params![table_oid, column_name, prim.get_type_oid(), column_ordering, column_style, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = trans.last_insert_rowid();

            // Add the column to the table
            let sqlite_type = prim.get_sqlite_type();
            let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} {sqlite_type};");
            trans.execute(&alter_table_cmd, [])?;

            // Update table's surrogate view
            table::update_surrogate_view(&trans, table_oid)?;

            // Return the column OID
            trans.commit()?;
            return Ok(column_oid);
        },
        data_type::MetadataColumnType::SingleSelectDropdown(referenced_table_oid)
        | data_type::MetadataColumnType::Reference(referenced_table_oid)
        | data_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
            // Add the column to the table's metadata
            trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME,TYPE_OID, COLUMN_ORDERING, COLUMN_CSS_STYLE, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                params![table_oid, column_name, referenced_table_oid, column_ordering, column_style, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = trans.last_insert_rowid();

            // Add the column to the table as a reference to another table
            let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} INTEGER REFERENCES TABLE{referenced_table_oid} (OID) ON UPDATE CASCADE ON DELETE SET DEFAULT;");
            trans.execute(&alter_table_cmd, [])?;

            // Update table's surrogate view
            table::update_surrogate_view(&trans, table_oid)?;

            // Return the column's OID
            trans.commit()?;
            return Ok(column_oid);
        },
        data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid)
        | data_type::MetadataColumnType::ChildTable(column_type_oid) => {
            // Add the column to the table's metadata
            trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME,TYPE_OID, COLUMN_ORDERING, COLUMN_CSS_STYLE, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                params![table_oid, column_name, column_type_oid, column_ordering, column_style, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = trans.last_insert_rowid();

            // Update table's surrogate view
            table::update_surrogate_view(&trans, table_oid)?;

            // Return the column OID
            trans.commit()?;
            return Ok(column_oid);
        }
    }
}

/// Edits a column's metadata and/or type.
pub fn edit(table_oid: i64, column_oid: i64, column_name: &str, column_type: data_type::MetadataColumnType, column_style: &str, is_nullable: bool, is_unique: bool, is_primary_key: bool) -> Result<Option<i64>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Record the old values of the column metadata
    trans.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (
            TRASH, 
            TABLE_OID, 
            NAME, 
            TYPE_OID, 
            COLUMN_CSS_STYLE, 
            COLUMN_ORDERING, 
            IS_NULLABLE, 
            IS_UNIQUE, 
            IS_PRIMARY_KEY, 
            DEFAULT_VALUE
        )
        SELECT
            1 AS TRASH,
            TABLE_OID,
            NAME,
            TYPE_OID,
            COLUMN_CSS_STYLE,
            COLUMN_ORDERING,
            IS_NULLABLE,
            IS_UNIQUE,
            IS_PRIMARY_KEY,
            DEFAULT_VALUE
        FROM METADATA_TABLE_COLUMN
        WHERE OID = ?1", 
        params![column_oid])?;
    let trash_column_oid: i64 = trans.last_insert_rowid();

    match trans.query_one(
        "SELECT
            c.TYPE_OID,
            t.MODE,
            c.TABLE_OID
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.OID = ?1;", 
        params![column_oid], 
        |row| {
            let prior_column_type = data_type::MetadataColumnType::from_database(row.get(0)?, row.get(1)?);
            let table_oid: i64 = row.get(2)?;
            return Ok((prior_column_type, table_oid));
        }
    ).optional()? {
        Some((prior_column_type, table_oid)) => {
            // Update the table's metadata
            trans.execute(
                "UPDATE METADATA_TABLE_COLUMN
                SET
                    NAME = ?1,
                    TYPE_OID = ?2,
                    COLUMN_CSS_STYLE = ?3,
                    IS_NULLABLE = ?4,
                    IS_UNIQUE = ?5,
                    IS_PRIMARY_KEY = ?6
                WHERE OID = ?7;", 
                params![column_name, column_type.get_type_oid(), column_style, is_nullable, is_unique, is_primary_key, column_oid]
            )?;

            if prior_column_type != column_type {
                // Attempt to transfer over data
                let trans_table_created: bool;

                // Start by deconstructing any tables and dropping any columns for the previous type
                match prior_column_type {
                    data_type::MetadataColumnType::Primitive(_)
                    | data_type::MetadataColumnType::Reference(_)
                    | data_type::MetadataColumnType::ChildObject(_)  => {
                        // Create temporary table to hold prior data
                        let create_temp_cmd = format!("CREATE TABLE TRANS_COLUMN{trash_column_oid} AS SELECT OID, COLUMN{column_oid} AS VALUE FROM TABLE{table_oid};");
                        trans.execute(&create_temp_cmd, [])?;
                        trans_table_created = true;

                        // Delete the previous column from the data
                        let alter_cmd = format!("ALTER TABLE TABLE{table_oid} DROP COLUMN COLUMN{column_oid};");
                        trans.execute(&alter_cmd, [])?;
                    },
                    data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                        // Create temporary table to hold prior data
                        let create_temp_cmd = format!("CREATE TABLE TRANS_COLUMN{trash_column_oid} AS SELECT OID, COLUMN{column_oid} AS VALUE FROM TABLE{table_oid};");
                        trans.execute(&create_temp_cmd, [])?;
                        trans_table_created = true;

                        // Drop the column from the data table
                        let alter_cmd = format!("ALTER TABLE TABLE{table_oid} DROP COLUMN COLUMN{column_oid};");
                        trans.execute(&alter_cmd, [])?;

                        // Drop the dropdown values table
                        let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                        trans.execute(&drop_cmd, [])?;

                        // Delete the dropdown type from the metadata
                        trans.execute(
                            "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                            params![column_type_oid]
                        )?;
                    },
                    data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                        // Do not create a temporary table
                        trans_table_created = false;

                        // Drop the relationship table
                        let drop_relationship_cmd = format!("DROP TABLE TABLE{column_type_oid}_MULTISELECT;");
                        trans.execute(&drop_relationship_cmd, [])?;

                        // Drop the dropdown values table
                        let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                        trans.execute(&drop_cmd, [])?;

                        // Delete the type from the metadata
                        trans.execute(
                            "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                            params![column_type_oid]
                        )?;
                    },
                    data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                        // Do not create a temporary table
                        trans_table_created = false;

                        // Drop the surrogate view of the child table
                        let drop_view_cmd = format!("DROP VIEW TABLE{column_type_oid}_SURROGATE;");
                        trans.execute(&drop_view_cmd, [])?;

                        // Drop the child table
                        let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                        trans.execute(&drop_cmd, [])?;

                        // Delete the child table from the metadata
                        trans.execute(
                            "DELETE FROM METADATA_TABLE WHERE OID = ?1", 
                            params![column_type_oid]
                        )?;

                        // Delete the type from the metadata
                        trans.execute(
                            "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                            params![column_type_oid]
                        )?;
                    }
                }


                // Then construct any tables/columns for the new type, and upload data if applicable
                let column_type = column_type.create_for_table(&trans, &table_oid)?;
                match column_type {
                    data_type::MetadataColumnType::Primitive(prim) => {
                        // Add the column to the table
                        let sqlite_type = prim.get_sqlite_type();
                        let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} {sqlite_type};");
                        trans.execute(&alter_table_cmd, [])?;

                        // Copy over previous data
                        if trans_table_created {
                            let copy_cmd = format!("
                            UPDATE OR IGNORE TABLE{table_oid} AS t
                            SET COLUMN{column_oid} = CAST(trans.VALUE AS {sqlite_type})
                            FROM TRANS_COLUMN{trash_column_oid} AS trans
                            WHERE t.OID = trans.OID;
                            ");
                            trans.execute(&copy_cmd, [])?;
                        }
                    },
                    data_type::MetadataColumnType::SingleSelectDropdown(referenced_table_oid)
                    | data_type::MetadataColumnType::Reference(referenced_table_oid)
                    | data_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                        // Update the table's metadata with the newly-constructed type
                        trans.execute(
                            "UPDATE METADATA_TABLE_COLUMN
                            SET
                                TYPE_OID = ?1
                            WHERE OID = ?2;", 
                            params![referenced_table_oid, column_oid]
                        )?;

                        // Add the column to the table
                        let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} INTEGER REFERENCES TABLE{referenced_table_oid} (OID) ON UPDATE CASCADE ON DELETE SET NULL;");
                        trans.execute(&alter_table_cmd, [])?;

                        // Copy over previous data
                        if trans_table_created {
                            let copy_cmd = format!("
                            UPDATE OR IGNORE TABLE{table_oid} AS t
                            SET COLUMN{column_oid} = CAST(trans.VALUE AS TEXT)
                            FROM TRANS_COLUMN{trash_column_oid} AS trans
                            WHERE t.OID = trans.OID;
                            ");
                            trans.execute(&copy_cmd, [])?;
                        }
                    },
                    data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid)
                    | data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                        // Update the table's metadata with the newly-constructed type
                        trans.execute(
                            "UPDATE METADATA_TABLE_COLUMN
                            SET
                                TYPE_OID = ?1
                            WHERE OID = ?2;", 
                            params![column_type_oid, column_oid]
                        )?;
                    }
                }
            }

            // Update table's surrogate view
            table::update_surrogate_view(&trans, table_oid)?;

            // Commit the changes
            trans.commit()?;
            return Ok(Some(trash_column_oid));
        },
        None => {
            return Ok(None);
        }
    };
}

/// Reorders a column.
pub fn reorder(table_oid: i64, column_oid: i64, column_ordering: Option<i64>) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Determine the new ordering of the column
    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE COLUMN_ORDERING >= ?1;",
                params![o]
            )?;
            o
        },
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING) + 1, 0) AS NEW_COLUMN_ORDERING FROM METADATA_TABLE_COLUMN", 
                [], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    // Update the ordering
    trans.execute("UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = ?1 WHERE OID = ?2", params![column_ordering, column_oid])?;

    // Commit the transaction
    trans.commit()?;
    return Ok(column_ordering);
}

/// Flags a column as being trash.
pub fn move_trash(table_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute("UPDATE METADATA_TABLE_COLUMN SET TRASH = 1 WHERE OID = ?1;", params![column_oid])?;

    // Update table's surrogate view
    table::update_surrogate_view(&trans, table_oid)?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a column as being trash.
pub fn unmove_trash(table_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Unflag the table as trash
    trans.execute("UPDATE METADATA_TABLE_COLUMN SET TRASH = 0 WHERE OID = ?1;", params![column_oid])?;

    // Update table's surrogate view
    table::update_surrogate_view(&trans, table_oid)?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Delete the column with the given OID.
pub fn delete(column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    match trans.query_one(
        "SELECT
            c.TYPE_OID,
            t.MODE,
            c.TABLE_OID
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.OID = ?1;", 
        params![column_oid], 
        |row| {
            return Ok((
                row.get::<_, i64>(2)?,
                data_type::MetadataColumnType::from_database(row.get(0)?, row.get(1)?)
            ));
        }
    ).optional()? {
        Some((table_oid, column_type)) => {
            match column_type {
                data_type::MetadataColumnType::Primitive(_)
                | data_type::MetadataColumnType::Reference(_)
                | data_type::MetadataColumnType::ChildObject(_)  => {
                    // Delete the column from the data
                    let alter_cmd = format!("ALTER TABLE TABLE{table_oid} DROP COLUMN COLUMN{column_oid};");
                    trans.execute(&alter_cmd, [])?;

                    // Delete the column from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
                        params![column_oid]
                    )?;
                    trans.commit()?;
                    return Ok(());
                },
                data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    // Drop the column from the data table
                    let alter_cmd = format!("ALTER TABLE TABLE{table_oid} DROP COLUMN COLUMN{column_oid};");
                    trans.execute(&alter_cmd, [])?;

                    // Drop the dropdown values table
                    let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                    trans.execute(&drop_cmd, [])?;

                    // Delete the column from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
                        params![column_oid]
                    )?;

                    // Delete the type from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                        params![column_type_oid]
                    )?;
                    trans.commit()?;
                    return Ok(());
                },
                data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    // Drop the relationship table
                    let drop_relationship_cmd = format!("DROP TABLE TABLE{column_type_oid}_MULTISELECT;");
                    trans.execute(&drop_relationship_cmd, [])?;

                    // Drop the dropdown values table
                    let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                    trans.execute(&drop_cmd, [])?;

                    // Delete the column from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
                        params![column_oid]
                    )?;

                    // Delete the type from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                        params![column_type_oid]
                    )?;
                    trans.commit()?;
                    return Ok(());
                },
                data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    // Drop the surrogate view of the child table
                    let drop_view_cmd = format!("DROP VIEW TABLE{column_type_oid}_SURROGATE;");
                    trans.execute(&drop_view_cmd, [])?;

                    // Drop the child table
                    let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                    trans.execute(&drop_cmd, [])?;

                    // Delete the child table from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TABLE WHERE OID = ?1", 
                        params![column_type_oid]
                    )?;

                    // Delete the column from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
                        params![column_oid]
                    )?;

                    // Delete the type from the metadata
                    trans.execute(
                        "DELETE FROM METADATA_TYPE WHERE OID = ?1", 
                        params![column_type_oid]
                    )?;
                    trans.commit()?;
                    return Ok(());
                }
            }
        },
        None => {}
    };
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
                c.NAME, 
                c.COLUMN_ORDERING,
                c.COLUMN_CSS_STYLE,
                c.TYPE_OID, 
                t.MODE,
                c.IS_NULLABLE,
                c.IS_UNIQUE,
                c.IS_PRIMARY_KEY
            FROM SUPERTYPE_QUERY s
            INNER JOIN METADATA_TABLE_COLUMN c ON s.TYPE_OID = c.TABLE_OID
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.TRASH = 0
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