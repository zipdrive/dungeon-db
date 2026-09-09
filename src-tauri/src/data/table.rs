use crate::data::column;
use crate::data::column_type;
use crate::data::datasource::Datasource;
use crate::data::schema;
use crate::data::view::regenerate_schema_views;
use crate::util::channel::Sender;
use crate::util::db;
use crate::util::db::{sql_execute, sql_iter};
use crate::util::error::Error;
use rusqlite::Connection;
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
        sql_execute(
            &trans, 
            format!(
                "
                CREATE TABLE TABLE{} (
                    OID INTEGER PRIMARY KEY, 
                    TRASH INTEGER NOT NULL DEFAULT 0
                ) STRICT;
                ",
                self.schema.oid
            ), 
            []
        )?;

        // To update the inheritance, now that there is a constructed table for it
        self.schema.set(&trans)?;

        // Create the table metadata
        sql_execute(
            &trans,
            "
            INSERT INTO METADATA_TABLE (OID) 
            VALUES (?1)
            ",
            params![self.schema.oid],
        )?;
        // Create a datasource for the table
        sql_execute(
            &trans,
            "
            INSERT INTO METADATA_DATASOURCE (TABLE_OID) 
            VALUES (?1)
            ",
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
    value: i64,
    label: String
}

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct DropdownValueEmit {
    processid: i64,
    dropdown_value: DropdownValue
}

const PUSH_DROPDOWN_VALUE_SIGNAL: &'static str = "table_row_label";

impl DropdownValue {
    /// Queries for all tables inheriting from this one.
    pub fn query_inheritor_tables<'a>(
        mut sender: Sender<'a, Self>,
        table_oid: i64
    ) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        sql_iter(
            &conn,
            "
            SELECT
                0 AS LEVEL,
                OID,
                NAME
            FROM METADATA_SCHEMA 
            WHERE OID = ?1

            UNION ALL 

            SELECT 
                MAX_DEPTH AS LEVEL,
                inh.INHERITOR_SCHEMA_OID AS OID,
                s.NAME
            FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW inh 
            INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID
            WHERE inh.MASTER_SCHEMA_OID = ?1

            ORDER BY 1, 3
            ",
            params![table_oid],
            |row| {
                sender.send(Self {
                    value: row.get("OID")?,
                    label: row.get("NAME")?
                })?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }

    pub fn query_table_row_labels<'a>(mut sender: Sender<'a, Self>, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;
        
        sql_iter(
            &conn,
            format!(
                "
                SELECT 
                    l.OID, 
                    COALESCE(l.PLAIN_LABEL, l.JSON_LABEL) AS LABEL 
                FROM SCHEMA{table_oid}_VIEW l 
                ORDER BY l.ROW_INDEX
                "
            ),
            [],
            |row| {
                sender.send(Self {
                    value: row.get::<_, i64>("OID")?, 
                    label: row.get::<_, String>("LABEL")? 
                })?;
                Ok(None::<()>)
            }
        )?;

        Ok(())
    }
}