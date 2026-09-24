use crate::util::channel::Sender;
use crate::util::db;
use crate::util::db::{sql_execute, sql_iter, sql_one, sql_collect};
use crate::util::error::Error;
use rusqlite::Connection;
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{HashSet};
use std::hash::{Hash, Hasher};

mod column_type;
pub mod column;
mod view;
pub mod row;
pub mod label;


#[derive(Serialize, Clone)]
pub struct TableListItem {
    pub oid: i64,
    pub name: String
}

impl TableListItem {
    /// Send a list of all tables.
    pub fn send_all(mut sender: Sender<Self>) -> Result<(), Error> {
        let conn = db::open()?;
        sql_iter(
            &conn, 
            "SELECT OID, NAME FROM METADATA_TABLE ORDER BY NAME", 
            [], 
            |row| {
                sender.send(Self {
                    oid: row.get::<_, i64>("OID")?,
                    name: row.get::<_, String>("NAME")?
                })?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }
}




/// Data structure representing the table metadata
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TableMetadata {
    pub oid: i64,
    pub name: String,
    pub master_oids: Vec<i64>
}

impl Hash for TableMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl Borrow<i64> for TableMetadata {
    fn borrow(&self) -> &i64 {
        &self.oid
    }
}

impl TableMetadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, oid)
    }

    /// Gets the metadata for a table.
    pub fn conn_get(conn: &Connection, oid: i64) -> Result<Self, Error> {
        // Get the OID and name from the table metadata view
        let (oid, name) = sql_one(
            &conn, 
            "
SELECT
    OID,
    NAME
FROM METADATA_TABLE
WHERE OID = ?1
            ", 
            params![oid], 
            |row| {
                return Ok((
                    row.get::<_, i64>("OID")?,
                    row.get::<_, String>("NAME")?
                ));
            }
        )?;

        // Get the master OIDs from the table inheritance metadata view
        let master_oids = sql_collect(
            &conn, 
            "
SELECT
    MASTER_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE INHERITOR_TABLE_OID = ?1
            ", 
            params![oid], 
            |row| row.get::<_, i64>("MASTER_TABLE_OID")
        )?;

        // Return the metadata
        Ok(Self {
            oid,
            name,
            master_oids
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create row in table metadata
        trans.execute(
            "INSERT INTO __METADATA_TABLE (NAME) VALUES (?1)", 
            params![self.name]
        )?;
        self.oid = trans.last_insert_rowid();

        // Create row in datasource metadata
        sql_execute(
            &trans,
            "INSERT INTO __METADATA_DATASOURCE (TABLE_OID) VALUES (?1)",
            params![self.oid],
        )?;

        // Create the storage table
        sql_execute(
            &trans, 
            format!(
                "
CREATE TABLE __TABLE{} (
    OID INTEGER PRIMARY KEY, 
    TRASH INTEGER NOT NULL DEFAULT 0
) STRICT;
                ",
                self.oid
            ), 
            []
        )?;

        // Overwrite master OIDs
        self.conn_set_master_oids(&trans)?;

        // Rebuild the table views
        view::rebuild(&trans, self.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set_metadata(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Overwrite name
        trans.execute(
            "
UPDATE __METADATA_TABLE SET 
    NAME = ?1
WHERE OID = ?2
            ", 
            params![self.name, self.oid]
        )?;

        // Overwrite the master OIDs
        self.conn_set_master_oids(&trans)?;

        // Rebuild the table views
        view::rebuild(&trans, self.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrite the master OIDs.
    fn conn_set_master_oids(&self, conn: &Connection) -> Result<(), Error> {
        // Trash all existing master OIDs
        conn.execute(
            "UPDATE __METADATA_TABLE_INHERITANCE SET TRASH = TRUE WHERE INHERITOR_TABLE_OID = ?1", 
            params![self.oid]
        )?;

        let master_columns: HashSet<String> = HashSet::from_iter(sql_collect(
            conn, 
            format!("SELECT name FROM pragma_table_info(__TABLE{}) WHERE name LIKE 'MASTER%'", self.oid), 
            [], 
            |row| row.get::<_, String>("name")
        )?);

        for master_oid in self.master_oids.iter() {
            // Update list in database
            conn.execute(
                "
INSERT INTO __METADATA_TABLE_INHERITANCE (MASTER_TABLE_OID, INHERITOR_TABLE_OID) VALUES (?1, ?2)
ON CONFLICT SET TRASH = FALSE
                ", 
                params![master_oid, self.oid]
            )?;

            if !master_columns.contains(&format!("MASTER{master_oid}_OID")) {
                sql_execute(
                    conn,
                    format!(
                        "
ALTER TABLE __TABLE{}
ADD COLUMN MASTER{master_oid}_OID INTEGER 
REFERENCES __TABLE{master_oid} (OID)
    ON UPDATE CASCADE 
    ON DELETE CASCADE
                        ",
                        self.oid
                    ), 
                    []
                )?;
            }
        }
        Ok(())
    }
}
