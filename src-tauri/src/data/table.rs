use crate::data::column;
use crate::data::column_type;
use crate::data::datasource::Datasource;
use crate::data::query::QueryBuilder;
use crate::data::schema;
use crate::data::surrogate;
use crate::data::view::regenerate_schema_views;
use crate::util::channel::Sender;
use crate::util::db;
use crate::util::error::Error;
use rocket::serde::{Serialize as RocketSerialize};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use tauri::AppHandle;
use tauri::Emitter;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

/// Data structure representing the table metadata
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FullMetadata {
    pub schema: schema::FullMetadata,
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state)
    }
}

impl Borrow<i64> for FullMetadata {
    fn borrow(&self) -> &i64 {
        self.schema.borrow()
    }
}

impl FullMetadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Get the schema metadata
        let schema_metadata = schema::FullMetadata::get(&conn, oid)?;

        // Return the metadata
        Ok(Self {
            schema: schema_metadata,
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create schema
        self.schema.create(&trans)?;

        // Create the table
        let create_table_cmd: String = format!(
            "
            CREATE TABLE TABLE{} (
                OID INTEGER PRIMARY KEY, 
                TRASH INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            ",
            self.schema.oid
        );
        trans.execute(&create_table_cmd, [])?;

        // To update the inheritance, now that there is a constructed table for it
        self.schema.set(&trans)?;

        // Create the table metadata
        trans.execute(
            "INSERT INTO METADATA_TABLE (OID) VALUES (?1)",
            params![self.schema.oid],
        )?;
        // Create a datasource for the table
        trans.execute(
            "INSERT INTO METADATA_DATASOURCE (TABLE_OID) VALUES (?1)",
            params![self.schema.oid],
        )?;

        // Regenerate views related to the schema
        regenerate_schema_views(&trans, self.schema.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Overwrite the schema metadata
        self.schema.set(&trans)?;

        // Regenerate views related to the schema
        regenerate_schema_views(&trans, self.schema.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}


#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct DropdownValue {
    id: i64,
    name: String
}

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct DropdownValueEmit {
    processid: i64,
    dropdown_value: DropdownValue
}

const PUSH_DROPDOWN_VALUE_SIGNAL: &'static str = "table_row_label";

impl DropdownValue {
    pub fn emit_table_row_labels(app: AppHandle, processid: i64, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;
        
        let select_sql: String = format!("SELECT l.OID, l.SELECT_LABEL AS LABEL FROM SCHEMA{table_oid}_VIEW s INNER JOIN TABLE{table_oid}_LABEL_VIEW l ON s.OID = l.OID ORDER BY s.ROW_INDEX");
        println!("{select_sql}");
        let mut select_stmt = conn.prepare(&select_sql)?;
        let select_rows = select_stmt.query_and_then([], |row| Ok::<(i64, String), rusqlite::Error>((row.get::<_, i64>("OID")?, row.get::<_, String>("LABEL")?)))?;
        for row_result in select_rows {
            let (oid, label) = row_result?;
            println!("Sending processid={processid}, id={oid}, name={label}");
            app.emit(PUSH_DROPDOWN_VALUE_SIGNAL, DropdownValueEmit {
                processid: processid.clone(),
                dropdown_value: Self { 
                    id: oid, 
                    name: label 
                }
            })?;
        }

        Ok(())
    }
}