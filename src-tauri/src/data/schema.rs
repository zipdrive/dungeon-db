use crate::util::db;
use crate::util::error::Error;
use crate::data::{datasource, table, report};
use rusqlite::{Connection, Transaction, OptionalExtension, params};
use serde::Serialize;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

pub enum Schema {
    Table(table::Metadata),
    Report(report::Metadata)
}

impl Schema {
    /// Gets the type of schema from the OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn: Connection = db::open()?;

        let schema_type: String = conn.query_one(
            "
            SELECT 'table' AS SCHEMA_TYPE FROM METADATA_TABLE WHERE OID = ?1
            UNION
            SELECT 'report' AS SCHEMA_TYPE FROM METADATA_REPORT WHERE OID = ?1
            ", 
            params![oid], 
            |row| row.get::<_, String>("SCHEMA_TYPE")
        )?;
        if schema_type == "table" {
            Ok(Self::Table(table::Metadata::get(oid)?))
        } else {
            Ok(Self::Report(report::Metadata::get(oid)?))
        }
    }
}



/// Data structure representing the schema metadata.
#[derive(Serialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct Metadata {
    pub oid: i64,
    pub name: String
}

impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl Borrow<i64> for Metadata {
    fn borrow(&self) -> &i64 {
        &self.oid
    }
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
