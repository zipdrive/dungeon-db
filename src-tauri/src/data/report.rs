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
    pub schema: schema::FullMetadata,
    pub filter_formula: Option<String>,
    pub group_by_column_oids: Vec<i64>
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

        // Query for filter formula
        let filter_formula: Option<String> = conn.query_one(
            "SELECT FILTER_FORMULA FROM METADATA_REPORT WHERE OID = ?1",
            params![oid],
            |row| row.get::<_, Option<String>>("FILTER_FORMULA")
        )?;

        // Query for GROUP BY columns
        let mut group_by_column_oids: Vec<i64> = Vec::new();
        {
            let mut group_by_column_oids_statement = conn.prepare(
                "
                SELECT 
                    COLUMN_OID
                FROM METADATA_REPORT_GROUPBY_VIEW
                WHERE REPORT_OID = ?1
                "
            )?;
            let group_by_column_oids_rows = group_by_column_oids_statement.query_and_then(
                params![oid], 
                |row| row.get::<_, i64>(0)
            )?;
            for group_by_column_oids_result in group_by_column_oids_rows {
                group_by_column_oids.push(group_by_column_oids_result?);
            }
        }

        // Return the metadata
        Ok(Self {
            schema: schema_metadata,
            filter_formula,
            group_by_column_oids
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

        // Set the GROUP BY columns and filter formula
        self.set_transact(&trans)?;

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

        // Set the GROUP BY columns and filter formula
        self.set_transact(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for GROUP BY columns and filters.
    fn set_transact(&self, trans: &Transaction) -> Result<(), Error> {
        // Update the filter formula applied to each row of the table
        trans.execute(
            "UPDATE METADATA_REPORT SET FILTER_FORMULA = ?1 WHERE OID = ?2",
            params![self.filter_formula, self.schema.oid]
        )?;

        // Trash all previous rows of GROUP BY
        trans.execute("UPDATE METADATA_REPORT_GROUPBY SET TRASH = TRUE WHERE REPORT_OID = ?1", params![self.schema.oid])?;
        // Set new rows of GROUP BY
        for group_by_column_oid in self.group_by_column_oids.iter() {
            trans.execute(
                "
                INSERT INTO METADATA_REPORT_GROUPBY 
                    (REPORT_OID, COLUMN_OID)
                    VALUES
                    (?1, ?2)
                ON CONFLICT DO UPDATE SET 
                    TRASH = FALSE
                WHERE EXISTS(
                    SELECT
                        c.OID
                    FROM METADATA_COLUMN_VIEW c
                    WHERE c.OID = excluded.COLUMN_OID
                        AND (c.SCHEMA_OID = excluded.REPORT_OID
                            OR EXISTS(SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = excluded.REPORT_OID)
                        )
                )
                ",
                params![self.schema.oid, group_by_column_oid]
            )?;
        }
        Ok(())
    }
}
