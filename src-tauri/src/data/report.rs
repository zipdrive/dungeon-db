use crate::util::channel::Sender;
use crate::util::db::{sql_collect, sql_one, sql_execute, sql_iter};
use crate::util::db;
use crate::util::error::Error;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

mod column_type;
pub mod column;


#[derive(Serialize, Clone)]
pub struct ReportListItem {
    pub oid: i64,
    pub name: String
}

impl ReportListItem {
    /// Send a list of all reports.
    pub fn send_all(mut sender: Sender<Self>) -> Result<(), Error> {
        let conn = db::open()?;
        sql_iter(
            &conn, 
            "SELECT OID, NAME FROM METADATA_REPORT ORDER BY NAME", 
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
pub struct ReportMetadata {
    pub oid: i64,
    pub name: String,
    pub filter_formula: Option<String>,
    pub group_by_column_oids: Vec<i64>,
    pub order_by_column_oids: Vec<i64>
}

impl Hash for ReportMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl Borrow<i64> for ReportMetadata {
    fn borrow(&self) -> &i64 {
        &self.oid
    }
}

impl ReportMetadata {
    /// Gets the metadata for a report.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, oid)
    }

    /// Gets the metadata for a report.
    pub fn conn_get(conn: &Connection, oid: i64) -> Result<Self, Error> {
        // Get the OID, name, and filter formula from the report metadata view
        let (oid, name, filter_formula) = sql_one(
            &conn, 
            "
SELECT
    OID,
    NAME,
    FILTER
FROM METADATA_REPORT
WHERE OID = ?1
            ", 
            params![oid], 
            |row| {
                return Ok((
                    row.get::<_, i64>("OID")?,
                    row.get::<_, String>("NAME")?,
                    row.get::<_, Option<String>>("FILTER")?
                ));
            }
        )?;

        // Query for GROUP BY columns
        let group_by_column_oids = sql_collect(
            &conn, 
            "
SELECT 
    COLUMN_OID
FROM METADATA_REPORT_GROUPBY
WHERE REPORT_OID = ?1
ORDER BY OID
            ", 
            params![oid], 
            |row| row.get::<_, i64>("COLUMN_OID")
        )?;
        // Query for ORDER BY columns
        let order_by_column_oids = sql_collect(
            &conn, 
            "
SELECT 
    COLUMN_OID
FROM METADATA_REPORT_ORDERBY
WHERE REPORT_OID = ?1
ORDER BY OID
            ", 
            params![oid], 
            |row| row.get::<_, i64>("COLUMN_OID")
        )?;

        // Return the metadata
        Ok(Self {
            oid,
            name,
            filter_formula,
            group_by_column_oids,
            order_by_column_oids
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create row in table metadata
        trans.execute(
            "INSERT INTO __METADATA_TABLE (NAME, FILTER) VALUES (?1, ?2)", 
            params![self.name, self.filter_formula]
        )?;
        self.oid = trans.last_insert_rowid();

        // Overwrite the GROUP BY and ORDER BY columns
        self.conn_set_groupby_orderby(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set_metadata(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create row in table metadata
        trans.execute(
            "
UPDATE __METADATA_TABLE SET 
    NAME = ?1, 
    FILTER = ?2 
WHERE OID = ?3
            ", 
            params![self.name, self.filter_formula]
        )?;

        // Overwrite the GROUP BY and ORDER BY columns
        self.conn_set_groupby_orderby(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    fn conn_set_groupby_orderby(&self, conn: &Connection) -> Result<(), Error> {
        // Delete prior rows in __METADATA_REPORT_GROUPBY table
        sql_execute(
            conn, 
            "
UPDATE __METADATA_REPORT_GROUPBY AS g SET 
    TRASH = TRUE
FROM __METADATA_REPORT_COLUMN c
WHERE c.OID = g.COLUMN_OID 
    AND c.REPORT_OID = ?1
            ", 
            params![self.oid]
        )?;

        // Insert new rows in __METADATA_REPORT_GROUPBY table
        for groupby_column_oid in self.group_by_column_oids.iter() {
            sql_execute(
                conn, 
                "INSERT INTO __METADATA_REPORT_GROUPBY (COLUMN_OID) VALUES (?1)", 
                params![groupby_column_oid]
            )?;
        }

        // Delete prior rows in __METADATA_REPORT_ORDERBY table
        sql_execute(
            conn, 
            "
UPDATE __METADATA_REPORT_ORDERBY AS o SET 
    TRASH = TRUE
FROM __METADATA_REPORT_COLUMN c
WHERE c.OID = o.COLUMN_OID 
    AND c.REPORT_OID = ?1
            ", 
            params![self.oid]
        )?;

        // Insert new rows in __METADATA_REPORT_ORDERBY table
        for orderby_column_oid in self.order_by_column_oids.iter() {
            sql_execute(
                conn, 
                "INSERT INTO __METADATA_REPORT_ORDERBY (COLUMN_OID) VALUES (?1)", 
                params![orderby_column_oid]
            )?;
        }

        Ok(())
    }
}
