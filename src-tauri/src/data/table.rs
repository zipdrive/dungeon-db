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
    pub schema: schema::Metadata,
    pub master_tables: HashSet<Metadata>
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

        // Get the OIDs of every table that this table inherits from
        let mut master_tables: HashSet<Metadata> = HashSet::new();
        {
            let mut master_table_oid_statement = conn.prepare(
                "
                SELECT 
                    u.MASTER_TABLE_OID 
                FROM METADATA_TABLE_INHERITANCE u
                INNER JOIN METADATA_TABLE tbl ON tbl.OID = u.MASTER_TABLE_OID
                WHERE u.INHERITOR_TABLE_OID = ?1 
                    AND u.TRASH = 0
                    AND tbl.TRASH = 0
                "
            )?;
            let master_table_oid_rows = master_table_oid_statement.query_and_then(
                params![schema_metadata.oid], 
                |row| row.get::<_, i64>(0)
            )?;
            for master_table_oid_result in master_table_oid_rows {
                master_tables.insert(Metadata::get(master_table_oid_result?)?);
            }
        }

        // Return the metadata
        Ok(Self {
            schema: schema_metadata,
            master_tables
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create schema
        self.schema.create(&trans)?;
        // Create the table metadata
        trans.execute("INSERT INTO METADATA_TABLE (OID) VALUES (?1)", params![self.schema.oid])?;
        // Create a datasource for the table
        datasource::Datasource::Table { oid: 0, table: self.clone(), label: self.schema.name.clone() }.find(&trans, Vec::new())?;

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
        
        // Set inheritance from each master table
        self.set_inheritance(&trans)?;

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

        // Set inheritance from each master table
        self.set_inheritance(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Sets the inheritance pattern for a table.
    fn set_inheritance(&self, trans: &Transaction) -> Result<(), Error> {
        // Clear all metadata describing inheritance
        trans.execute(
            "UPDATE METADATA_TABLE_INHERITANCE SET TRASH = 1 WHERE INHERITOR_TABLE_OID = ?1",
            params![self.schema.oid]
        )?;

        // Add inheritance from each master table
        for master_table in self.master_tables.iter() {
            // Upsert the inheritance row
            trans.execute(
                "INSERT INTO METADATA_TABLE_INHERITANCE (INHERITOR_TABLE_OID, MASTER_TABLE_OID) VALUES (?1, ?2) ON CONFLICT DO UPDATE SET TRASH = 0",
                params![self.schema.oid, master_table.schema.oid]
            )?;

            let master_table_name: String = format!("TABLE{}", self.schema.oid);
            let master_column_name: String = format!("MASTER{}_OID", master_table.schema.oid);
            if !trans.column_exists(Some("main"), &master_table_name, &master_column_name)? {
                // Add a column to the table that references a row in the master list
                let alter_table_cmd: String = format!(
                    "
                    ALTER TABLE TABLE{} 
                        ADD COLUMN MASTER{}_OID INTEGER
                        REFERENCES TABLE{} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE CASCADE
                    ",
                    self.schema.oid,
                    master_table.schema.oid,
                    master_table.schema.oid
                );
                trans.execute(&alter_table_cmd, [])?;
            }
        }
        Ok(())
    }
}
