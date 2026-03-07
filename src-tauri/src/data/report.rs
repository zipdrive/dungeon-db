use crate::util::error::Error;
use crate::util::db;
use crate::data::{schema, datasource};
use rusqlite::{Transaction, OptionalExtension, params};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

/// Data structure representing the table metadata
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct FullMetadata {
    pub schema: schema::FullMetadata
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state)
    }
}

impl Borrow<schema::FullMetadata> for FullMetadata {
    fn borrow(&self) -> &schema::FullMetadata {
        &self.schema
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
