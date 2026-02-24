use crate::util::error::Error;
use crate::data::schema;
use crate::data::parameter;
use rusqlite::{Transaction, params};

/// Data structure representing the table metadata
#[derive(Clone)]
pub struct Metadata {
    pub schema: schema::Metadata,
    pub master_table_oid_list: Vec<i64>
}

impl Metadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Get the schema metadata
        let schema_metadata = schema::Metadata::get(&conn, oid)?;

        // Get the OIDs of every table that this table inherits from
        let mut master_table_oid_list: Vec<i64> = Vec::new();
        {
            let mut master_table_oid_statement = trans.prepare(
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
            let master_table_oid_rows = master_table_oid_statement.query_and_then(params![table_oid], |row| row.get::<_, i64>(0))?;
            for master_table_oid_result in master_table_oid_rows {
                master_table_oid_list.push(master_table_oid_result?);
            }
        }

        // Return the metadata
        Ok(Self {
            schema: schema_metadata,
            master_table_oid_list
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create schema
        self.oid = self.schema.create(trans)?;
        // Create the table metadata
        trans.execute("INSERT INTO METADATA_TABLE (OID) VALUES (?1)", params![self.oid])?;

        // Create the table
        let create_table_cmd: String = format!(
            "
            CREATE TABLE TABLE{table_oid} (
                OID INTEGER PRIMARY KEY, 
                TRASH INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            "
        );
        trans.execute(&create_table_cmd, [])?;
        
        // Set inheritance from each master table
        self.set_inheritance(master_table_oid_list)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Overwrite the schema metadata
        self.schema.set()?;

        // Set inheritance from each master table
        self.set_inheritance(master_table_oid_list)?;

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
        for master_table_oid in self.master_table_oid_list.iter() {
            // Check if a row in the inheritance table already exists
            match trans.query_one("SELECT OID FROM METADATA_TABLE_INHERITANCE WHERE INHERITOR_TABLE_OID = ?1 AND MASTER_TABLE_OID = ?2", params![table_oid, master_table_oid], |row| row.get::<_, i64>(0)).optional()? {
                Some(parameter_oid) => {
                    // Update the inheritance table to indicate that the inheritance is not trash
                    trans.execute(
                        "UPDATE METADATA_TABLE_INHERITANCE SET TRASH = 0 WHERE OID = ?1",
                        params![parameter_oid]
                    )?;
                },
                None => {
                    // Get new parameter OID
                    let parameter_oid: i64 = parameter::create(trans)?;

                    // Insert a new row into the inheritance table
                    trans.execute(
                        "INSERT INTO METADATA_TABLE_INHERITANCE (OID, INHERITOR_TABLE_OID, MASTER_TABLE_OID) VALUES (?1, ?2, ?3)",
                        params![parameter_oid, table_oid, master_table_oid]
                    )?;
                    
                    // Add a column to the table that references a row in the master list
                    let alter_table_cmd: String = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN MASTER{master_table_oid}_OID INTEGER NOT NULL REFERENCES TABLE{master_table_oid} (OID) ON UPDATE CASCADE ON DELETE CASCADE;");
                    trans.execute(&alter_table_cmd, [])?;
                }
            }
        }
        Ok(())
    }
}
