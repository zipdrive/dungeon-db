use crate::util::error::Error;
use crate::util::db;
use crate::data::{schema, datasource};
use rusqlite::{Transaction, OptionalExtension, params};
use serde::Serialize;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

/// Data structure representing the table metadata
#[derive(Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct Metadata {
    pub schema: schema::Metadata
}

impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state)
    }
}

impl Borrow<schema::Metadata> for Metadata {
    fn borrow(&self) -> &schema::Metadata {
        &self.schema
    }
}

impl Metadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Get the schema metadata
        let schema_metadata = schema::Metadata::get(&conn, oid)?;

        // Return the metadata
        Ok(Self {
            schema: schema_metadata
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create schema
        self.schema.create(&trans)?;
        // Create the report metadata
        trans.execute("INSERT INTO METADATA_REPORT (OID) VALUES (?1)", params![self.schema.oid])?;

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

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}
