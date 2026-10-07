use crate::util::channel::Sender;
use crate::util::db;
use crate::util::db::{sql_execute, sql_iter, sql_one, sql_collect};
use crate::util::encode::json_encode_string;
use crate::util::error::Error;
use rusqlite::Connection;
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{HashSet};
use std::hash::{Hash, Hasher};

pub mod column_type;
pub mod column;
mod view;
pub mod row;
pub mod label;


#[derive(Serialize, Clone)]
pub struct TableListItem {
    pub oid: i64,
    pub name: String,
    pub disabled: bool
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
                    name: row.get::<_, String>("NAME")?,
                    disabled: false
                })?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }

    /// Send a list of all tables that are valid as a master of the given table.
    pub fn send_masters(mut sender: Sender<Self>, table_oid: Option<i64>) -> Result<(), Error> {
        if let Some(table_oid) = table_oid {
            let conn = db::open()?;
            sql_iter(
                &conn, 
                "
    SELECT 
        t.OID, 
        t.NAME,
        (inh.MASTER_TABLE_OID IS NOT NULL OR t.OID = ?1) AS DISABLED
    FROM METADATA_TABLE t 
    LEFT JOIN METADATA_TABLE_INHERITANCE_PATH inh 
        ON inh.INHERITOR_TABLE_OID = t.OID
            AND inh.MASTER_TABLE_OID = ?1
    ORDER BY t.NAME
    ", 
                params![table_oid], 
                |row| {
                    sender.send(Self {
                        oid: row.get("OID")?,
                        name: row.get("NAME")?,
                        disabled: row.get("DISABLED")?
                    })?;
                    Ok(None::<()>)
                }
            )?;
            Ok(())
        } else {
            Self::send_all(sender)
        }
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

    /// Finds the unique table with a matching name.
    pub fn conn_find(conn: &Connection, name: String) -> Result<Self, Error> {
        // Get the OID of all matching tables
        let matching_oids = sql_collect(
            &conn, 
            "
SELECT
    OID
FROM METADATA_TABLE
WHERE NAME = ?1
            ", 
            params![name], 
            |row| row.get::<_, i64>("OID")
        )?;
        if matching_oids.len() == 0 {
            Err(Error::adhoc(format!("No table with name \"{}\" exists!", json_encode_string(&name))))
        } else if matching_oids.len() > 1 {
            Err(Error::adhoc(format!("More than one table with name \"{}\" exists!", json_encode_string(&name))))
        } else {
            Self::get(matching_oids[0])
        }
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


    /// Trash the table.
    pub fn trash(table_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Trash the table
        Self::conn_trash(&trans, table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Trash the table.
    /// Uses the given connection.
    pub fn conn_trash(conn: &Connection, oid: i64) -> Result<(), Error> {
        sql_execute(
            conn, 
            "UPDATE __METADATA_TABLE SET TRASH = TRUE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }

    /// Untrash the table.
    pub fn untrash(table_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Untrash the table
        Self::conn_untrash(&trans, table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Untrash the column.
    /// Uses the given connection.
    pub fn conn_untrash(conn: &Connection, oid: i64) -> Result<(), Error> {
        sql_execute(
            conn, 
            "UPDATE __METADATA_TABLE SET TRASH = FALSE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }
}
