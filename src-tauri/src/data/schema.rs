use crate::util::error::Error;
use crate::data::datasource;
use rusqlite::{Connection, Transaction, params};
use serde::Serialize;

/// Data structure representing the schema metadata.
#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct Metadata {
    pub oid: i64,
    pub name: String
}

impl Metadata {
    /// Gets the metadata.
    pub fn get(conn: &Connection, oid: i64) -> Result<Self, Error> {
        Ok(conn.query_one(
            "SELECT NAME FROM METADATA_SCHEMA WHERE OID = ?1",
            params![oid],
            |row| {
                Ok(Self {
                    oid,
                    name: row.get("NAME")?
                })
            }
        )?)
    }

    /// Creates a new schema.
    pub fn create(&mut self, trans: &Transaction) -> Result<(), Error> {
        // Create schema metadata
        trans.execute("INSERT INTO METADATA_SCHEMA (NAME) VALUES (?1)", params![&self.name])?;
        self.oid = trans.last_insert_rowid();
        Ok(())
    }

    /// Overwrites the metadata of the schema.
    pub fn set(&self, trans: &Transaction) -> Result<(), Error> {
        // Overwrite schema metadata
        trans.execute("UPDATE METADATA_SCHEMA SET NAME = ?1 WHERE OID = ?2", params![&self.name, self.oid])?;
        Ok(())
    }
}
