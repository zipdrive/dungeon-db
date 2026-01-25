use std::collections::HashMap;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Row, Error as RusqliteError};
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;
use crate::backend::{column, db};
use crate::util::error;

#[derive(Serialize, Deserialize)]
#[serde(tag="primitiveType")]
pub enum Primitive {
    Boolean,    // Mode = 0 && OID = 1
    Integer,    // Mode = 0 && OID = 2
    Number,     // Mode = 0 && OID = 3
    Date,       // Mode = 0 && OID = 4
    Timestamp,  // Mode = 0 && OID = 5
    Text,       // Mode = 0 && OID = 6
    JSON,       // Mode = 0 && OID = 7
    File,       // Mode = 0 && OID = 8
    Image,      // Mode = 0 && OID = 9
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase", tag="mode")]
pub enum MetadataColumnType {
    Primitive(Primitive),          // Mode = 0
    SingleSelectDropdown(i64),     // Mode = 1
    MultiSelectDropdown(i64),      // Mode = 2
    Reference(i64),                // Mode = 3
    ChildObject(i64),              // Mode = 4
    ChildTable(i64),               // Mode = 5
}

impl MetadataColumnType {
    /// Converts a type from the database OID and mode.
    fn from_database(type_oid: i64, type_mode: i64) -> Result<MetadataColumnType, error::Error> {
        match type_mode {
            0 => {
                match type_oid {
                    1 => { return Ok(Self::Primitive(Primitive::Boolean)); },
                    2 => { return Ok(Self::Primitive(Primitive::Integer)); },
                    3 => { return Ok(Self::Primitive(Primitive::Number)); },
                    4 => { return Ok(Self::Primitive(Primitive::Date)); },
                    5 => { return Ok(Self::Primitive(Primitive::Timestamp)); },
                    6 => { return Ok(Self::Primitive(Primitive::Text)); },
                    7 => { return Ok(Self::Primitive(Primitive::JSON)); },
                    8 => { return Ok(Self::Primitive(Primitive::File)); },
                    9 => { return Ok(Self::Primitive(Primitive::Image)); },
                    _ => {
                        return Err(error::Error::AdhocError("Unknown primitive type encountered."));
                    }
                }
            },
            1 => { return Ok(Self::SingleSelectDropdown(type_oid)); },
            2 => { return Ok(Self::MultiSelectDropdown(type_oid)); },
            3 => { return Ok(Self::Reference(type_oid)); },
            4 => { return Ok(Self::ChildObject(type_oid)); },
            5 => { return Ok(Self::ChildTable(type_oid)); },
            _ => {
                return Err(error::Error::AdhocError("Unknown type encountered."));
            }
        }
    }

    /// Gets the corresponding type OID of a column type.
    fn get_type_oid(&self) -> i64 {
        return match self {
            Self::Primitive(prim) => match prim {
                Primitive::Boolean => 1,
                Primitive::Integer => 2,
                Primitive::Number => 3,
                Primitive::Date => 4,
                Primitive::Timestamp => 5,
                Primitive::Text => 6,
                Primitive::JSON => 7,
                Primitive::File => 8,
                Primitive::Image => 9,
            },
            Self::SingleSelectDropdown(type_oid) 
            | Self::MultiSelectDropdown(type_oid)
            | Self::Reference(type_oid)
            | Self::ChildObject(type_oid)
            | Self::ChildTable(type_oid) => type_oid.clone()
        }
    }

    /// Gets the corresponding type mode of a column type.
    fn get_type_mode(&self) -> i64 {
        return match self {
            Self::Primitive(_) => 0,
            Self::SingleSelectDropdown(_) => 1,
            Self::MultiSelectDropdown(_) => 2,
            Self::Reference(_) => 3,
            Self::ChildObject(_) => 4,
            Self::ChildTable(_) => 5
        }
    }
}

#[derive(Serialize)]
/// The most bare-bones version of table column metadata, used solely for populating the list of table columns
pub struct Metadata {
    oid: i64,
    name: String,
    width: i64,
    column_type: MetadataColumnType,
    is_nullable: bool,
    is_unique: bool,
    is_primary_key: bool,
}

/// Creates a new column in a table.
pub fn create(table_oid: i64, column_type: MetadataColumnType, column_ordering: i64, column_width: i64, is_nullable: bool, is_unique: bool, is_primary_key: bool) -> Result<i64, error::Error> {
    let is_nullable_bit = if is_nullable { 1 } else { 0 };
    let is_unique_bit = if is_unique { 1 } else { 0 };
    let is_primary_key_bit = if is_primary_key { 1 } else { 0 };

    let action = db::begin_db_action()?;
    action.trans.execute(
        "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE TABLE_OID = ?1 AND COLUMN_ORDERING >= ?2;",
        params![table_oid, column_ordering]
    )?;

    match &column_type {
        MetadataColumnType::Primitive(prim) => {
            // Add the column to the table's metadata
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TYPE_OID, COLUMN_ORDERING, COLUMN_WIDTH, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![&column_type.get_type_oid(), column_ordering, column_width, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = action.trans.last_insert_rowid();
            
            // Add the column to the table
            let sqlite_type = match prim {
                Primitive::Boolean => "TINYINT",
                Primitive::Integer => "INTEGER",
                Primitive::Number => "FLOAT",
                Primitive::Date => "DATE",
                Primitive::Timestamp => "TIMESTAMP",
                Primitive::Text | Primitive::JSON => "TEXT",
                Primitive::File | Primitive::Image => "BLOB",
            };
            let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} {sqlite_type};");
            action.trans.execute(&alter_table_cmd, [])?;

            // Return the column OID
            return Ok(column_oid);
        },
        MetadataColumnType::SingleSelectDropdown(_) => {
            // Create the column type, use that as the OID for the type
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (?1);", 
                params![column_type.get_type_mode()]
            )?;
            let column_type_oid = action.trans.last_insert_rowid();

            // Create the data table
            let create_table_cmd = format!("CREATE TABLE TABLE{column_type_oid} (VALUE TEXT PRIMARY KEY) WITHOUT ROWID;");
            action.trans.execute(&create_table_cmd, [])?;

            // Add the column to the table's metadata
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TYPE_OID, COLUMN_ORDERING, COLUMN_WIDTH, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![column_type_oid, column_ordering, column_width, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = action.trans.last_insert_rowid();

            // Add the column to the table
            let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} TEXT REFERENCES TABLE{column_type_oid} (VALUE) ON UPDATE CASCADE ON DELETE SET NULL;");
            action.trans.execute(&alter_table_cmd, [])?;

            // Return the column OID
            return Ok(column_oid);
        },
        MetadataColumnType::MultiSelectDropdown(_) => {
            // Create the column type, use that as the OID for the type
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (?1);", 
                params![column_type.get_type_mode()]
            )?;
            let column_type_oid = action.trans.last_insert_rowid();

            // Add the column to the table's metadata
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TYPE_OID, COLUMN_ORDERING, COLUMN_WIDTH, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![column_type_oid, column_ordering, column_width, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = action.trans.last_insert_rowid();

            // Create the data table
            let create_table_cmd = format!("CREATE TABLE TABLE{column_type_oid} (VALUE TEXT PRIMARY KEY) WITHOUT ROWID;");
            action.trans.execute(&create_table_cmd, [])?;

            // Create the *-to-* relationship table
            let create_relationship_cmd = format!("CREATE TABLE TABLE{column_type_oid}_MULTISELECT (OID INTEGER REFERENCES TABLE{table_oid} (OID) ON UPDATE CASCADE ON DELETE CASCADE, VALUE TEXT REFERENCES TABLE{column_type_oid} (VALUE) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY (OID, VALUE));");
            action.trans.execute(&create_relationship_cmd, [])?;

            // Return the column's OID
            return Ok(column_oid);
        },
        MetadataColumnType::Reference(referenced_table_oid)
        | MetadataColumnType::ChildObject(referenced_table_oid) => {
            // Add the column to the table's metadata
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TYPE_OID, COLUMN_ORDERING, COLUMN_WIDTH, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![referenced_table_oid, column_ordering, column_width, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = action.trans.last_insert_rowid();

            // Add the column to the table
            let alter_table_cmd = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN COLUMN{column_oid} INTEGER REFERENCES TABLE{referenced_table_oid} (OID) ON UPDATE CASCADE ON DELETE SET DEFAULT;");
            action.trans.execute(&alter_table_cmd, [])?;

            // Return the column's OID
            return Ok(column_oid);
        },
        MetadataColumnType::ChildTable(_) => {
            // Create the column type, use that as the OID for the type
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (?1);", 
                params![column_type.get_type_mode()]
            )?;
            let column_type_oid = action.trans.last_insert_rowid();

            // Add the column to the table's metadata
            action.trans.execute(
                "INSERT INTO METADATA_TABLE_COLUMN (TYPE_OID, COLUMN_ORDERING, COLUMN_WIDTH, IS_NULLABLE, IS_UNIQUE, IS_PRIMARY_KEY) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
                params![column_type_oid, column_ordering, column_width, is_nullable_bit, is_unique_bit, is_primary_key_bit]
            )?;
            let column_oid = action.trans.last_insert_rowid();

            // Create a child table for the type
            let create_table_cmd = format!("CREATE TABLE TABLE{column_type_oid} (OID INTEGER PRIMARY KEY, _PARENT_OID_ INTEGER NOT NULL REFERENCES TABLE{table_oid} (OID));");
            action.trans.execute(&create_table_cmd, [])?;

            // Create a surrogate view for the child table
            let create_view_cmd = format!("CREATE VIEW TABLE{column_type_oid}_SURROGATE (OID, DISPLAY_VALUE) AS SELECT OID, OID FROM TABLE{column_type_oid};");
            action.trans.execute(&create_view_cmd, [])?;

            // Return the column OID
            return Ok(column_oid);
        }
    }
}

pub fn send_metadata_list(table_oid: i64, column_channel: Channel<Metadata>) -> Result<(), error::Error> {
    let action = db::begin_readonly_db_action()?;

    action.query_iterate(
        "SELECT 
                OID, 
                NAME, 
                COLUMN_WIDTH,
                TYPE_OID, 
                TYPE_MODE,
                IS_NULLABLE,
                IS_UNIQUE,
                IS_PRIMARY_KEY
            FROM METADATA_TABLE_COLUMN c
            INNER JOIN METADATA_TABLE_COLUMN_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1 
            ORDER BY COLUMN_ORDERING ASC;",
         params![table_oid], 
        &mut |row| {
            column_channel.send(Metadata {  
                oid: row.get(0)?,
                name: row.get(1)?,
                width: row.get(2)?,
                column_type: MetadataColumnType::from_database(row.get(3)?, row.get(4)?)?,
                is_nullable: row.get(5)?,
                is_unique: row.get(6)?,
                is_primary_key: row.get(7)?,
            })?;
            return Ok(());
        }
    )?;
    return Ok(());
}