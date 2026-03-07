use crate::util::channel::Sender;
use crate::util::db;
use crate::util::error::Error;
use crate::data::{datasource, table, report};
use rusqlite::{Connection, Transaction, OptionalExtension, params};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

#[derive(Serialize, Clone)]
pub struct FlatListItemMetadata {
    oid: i64,
    name: String
}

impl FlatListItemMetadata {
    /// Queries for all tables.
    pub fn query_tables<'a>(mut sender: Sender<'a, Self>) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare(
            "SELECT s.OID, s.NAME FROM METADATA_TABLE tbl INNER JOIN METADATA_SCHEMA s ON s.OID = tbl.OID ORDER BY s.NAME"
            )?
            .query_and_then([], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }

    /// Queries for all reports.
    pub fn query_reports<'a>(mut sender: Sender<'a, Self>) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare(
            "SELECT s.OID, s.NAME FROM METADATA_REPORT r INNER JOIN METADATA_SCHEMA s ON s.OID = r.OID ORDER BY s.NAME"
            )?
            .query_and_then([], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }
}



#[derive(Serialize, Clone)]
pub struct HierarchicalListItemMetadata {
    oid: i64,
    name: String,
    master_oid: Option<i64>,
    level: i64
}

impl HierarchicalListItemMetadata {
    /// Queries for all tables.
    pub fn query_tables<'a>(mut sender: Sender<'a, Self>) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare("
            WITH TABLE_HIERARCHY (OID, NAME, MASTER_OID, LEVEL) AS (
                SELECT
                    s.OID,
                    s.NAME,
                    NULL AS MASTER_OID,
                    0 AS LEVEL
                FROM METADATA_TABLE tbl
                INNER JOIN METADATA_SCHEMA s ON s.OID = tbl.OID
                WHERE tbl.OID NOT IN (SELECT INHERITOR_TABLE_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM TABLE_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_TABLE_OID

                ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
            )
            SELECT * FROM TABLE_HIERARCHY
            ")?
            .query_and_then([], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?,
                    master_oid: row.get("MASTER_OID")?,
                    level: row.get("LEVEL")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }

    /// Queries for all tables inheriting from this one.
    pub fn query_inheritor_tables<'a>(mut sender: Sender<'a, Self>, master_table_oid: i64) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare("
            WITH TABLE_HIERARCHY (OID, NAME, MASTER_OID, LEVEL) AS (
                SELECT
                    s.OID,
                    s.NAME,
                    NULL AS MASTER_OID,
                    0 AS LEVEL
                FROM METADATA_SCHEMA s
                WHERE s.OID = ?1

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM TABLE_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID

                ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
            )
            SELECT * FROM TABLE_HIERARCHY
            ")?
            .query_and_then(params![master_table_oid], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?,
                    master_oid: row.get("MASTER_OID")?,
                    level: row.get("LEVEL")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }

    /// Queries for all reports.
    pub fn query_reports<'a>(mut sender: Sender<'a, Self>) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare("
            WITH REPORT_HIERARCHY (OID, NAME, MASTER_OID, LEVEL) AS (
                SELECT
                    s.OID,
                    s.NAME,
                    NULL AS MASTER_OID,
                    0 AS LEVEL
                FROM METADATA_REPORT r
                INNER JOIN METADATA_SCHEMA s ON s.OID = r.OID
                WHERE r.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM REPORT_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID
                INNER JOIN METADATA_REPORT r ON r.OID = inh.INHERITOR_SCHEMA_OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID

                ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
            )
            SELECT * FROM REPORT_HIERARCHY
            ")?
            .query_and_then([], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?,
                    master_oid: row.get("MASTER_OID")?,
                    level: row.get("LEVEL")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }
}



#[derive(Serialize, Clone)]
pub struct ToggledHierarchicalListItemMetadata {
    oid: i64,
    name: String,
    master_oid: Option<i64>,
    level: i64,
    disabled: bool
}

impl ToggledHierarchicalListItemMetadata {
    /// Queries for all reports.
    pub fn query_master_schemas<'a>(mut sender: Sender<'a, Self>, schema_oid: Option<i64>, is_table: bool) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        // Run query for flat table data
        for list_item_result in conn.prepare(
            if is_table {
                // If the schema is a table, allow for inheritance from other tables
                "
                WITH SCHEMA_HIERARCHY (OID, NAME, MASTER_OID, LEVEL, DISABLED) AS (
                    SELECT
                        s.OID,
                        s.NAME,
                        NULL AS MASTER_OID,
                        0 AS LEVEL,
                        (OID IS ?1) AS DISABLED
                    FROM METADATA_SCHEMA s ON s.OID = r.OID
                    WHERE s.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                    UNION

                    SELECT
                        s.OID,
                        s.NAME,
                        h.OID AS MASTER_OID,
                        h.LEVEL + 1 AS LEVEL,
                        (h.DISABLED OR s.OID IS ?1) AS DISABLED
                    FROM SCHEMA_HIERARCHY h
                    INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID
                    INNER JOIN METADATA_REPORT r ON r.OID = inh.INHERITOR_SCHEMA_OID
                    INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID

                    ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
                )
                SELECT * FROM SCHEMA_HIERARCHY
                "
        } else {
            // If the schema is a report, then only allow inheritance from other reports
            "
            WITH REPORT_HIERARCHY (OID, NAME, MASTER_OID, LEVEL, DISABLED) AS (
                SELECT
                    s.OID,
                    s.NAME,
                    NULL AS MASTER_OID,
                    0 AS LEVEL,
                    (OID IS ?1) AS DISABLED
                FROM METADATA_REPORT r
                INNER JOIN METADATA_SCHEMA s ON s.OID = r.OID
                WHERE r.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL,
                    (h.DISABLED OR s.OID IS ?1) AS DISABLED
                FROM REPORT_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID
                INNER JOIN METADATA_REPORT r ON r.OID = inh.INHERITOR_SCHEMA_OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID

                ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
            )
            SELECT * FROM REPORT_HIERARCHY
            "
        })?
            .query_and_then(params![schema_oid], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?,
                    master_oid: row.get("MASTER_OID")?,
                    level: row.get("LEVEL")?,
                    disabled: row.get("DISABLED")?
                })
            })? {
            
            sender.send(list_item_result?)?;
        }
        Ok(())
    }
}





#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum Schema {
    Table(table::FullMetadata),
    Report(report::FullMetadata)
}

impl Borrow<FullMetadata> for Schema {
    fn borrow(&self) -> &FullMetadata {
        match self {
            Self::Table(table_metadata) => &table_metadata.schema,
            Self::Report(report_metadata) => &report_metadata.schema
        }
    }
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
            Ok(Self::Table(table::FullMetadata::get(oid)?))
        } else {
            Ok(Self::Report(report::FullMetadata::get(oid)?))
        }
    }

    /// Retrieves the OID of the schema.
    pub fn get_oid(&self) -> i64 {
        let metadata: &FullMetadata = self.borrow();
        return metadata.oid.clone();
    }
}



/// Data structure representing the schema metadata.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct FullMetadata {
    pub oid: i64,
    pub name: String,
    pub master_schemas: HashSet<Schema>
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl Borrow<i64> for FullMetadata {
    fn borrow(&self) -> &i64 {
        &self.oid
    }
}

impl FullMetadata {
    /// Gets the metadata.
    pub fn get(conn: &Connection, oid: i64) -> Result<Self, Error> {
        // Query for schemas that this schema inherits from
        let mut master_schemas: HashSet<Schema> = HashSet::new();
        {
            let mut master_schema_oid_statement = conn.prepare(
                "
                SELECT 
                    u.MASTER_SCHEMA_OID 
                FROM METADATA_SCHEMA_INHERITANCE u
                INNER JOIN METADATA_SCHEMA s ON s.OID = u.MASTER_SCHEMA_OID
                WHERE u.INHERITOR_SCHEMA_OID = ?1 
                    AND u.TRASH = 0
                    AND tbl.TRASH = 0
                "
            )?;
            let master_schema_oid_rows = master_schema_oid_statement.query_and_then(
                params![oid], 
                |row| row.get::<_, i64>(0)
            )?;
            for master_schema_oid_result in master_schema_oid_rows {
                master_schemas.insert(Schema::get(master_schema_oid_result?)?);
            }
        }

        // Query for name of schema
        Ok(conn.query_one(
            "SELECT NAME FROM METADATA_SCHEMA WHERE OID = ?1",
            params![oid],
            |row| {
                Ok(Self {
                    oid,
                    name: row.get("NAME")?,
                    master_schemas
                })
            }
        )?)
    }

    /// Flags the schema for garbage collection.
    pub fn trash(oid: i64) -> Result<(), Error> {
        let mut conn: Connection = db::open()?;
        let trans: Transaction = conn.transaction()?;
        trans.execute("UPDATE METADATA_SCHEMA SET TRASH = TRUE WHERE OID = ?1", params![oid])?;
        trans.commit()?;
        Ok(())
    }

    /// Unflags the schema for garbage collection.
    pub fn untrash(oid: i64) -> Result<(), Error> {
        let mut conn: Connection = db::open()?;
        let trans: Transaction = conn.transaction()?;
        trans.execute("UPDATE METADATA_SCHEMA SET TRASH = FALSE WHERE OID = ?1", params![oid])?;
        trans.commit()?;
        Ok(())
    }

    /// Creates a new schema.
    pub fn create(&mut self, trans: &Transaction) -> Result<(), Error> {
        // Create schema metadata
        trans.execute("INSERT INTO METADATA_SCHEMA (NAME) VALUES (?1)", params![&self.name])?;
        self.oid = trans.last_insert_rowid();

        // Create the inheritance pattern
        self.set_inheritance(trans)?;
        Ok(())
    }

    /// Overwrites the metadata of the schema.
    pub fn set(&self, trans: &Transaction) -> Result<(), Error> {
        // Overwrite schema metadata
        trans.execute("UPDATE METADATA_SCHEMA SET NAME = ?1 WHERE OID = ?2", params![&self.name, self.oid])?;

        // Overwrite the inheritance pattern
        self.set_inheritance(trans)?;
        Ok(())
    }

    /// Sets the inheritance pattern for the schema.
    fn set_inheritance(&self, trans: &Transaction) -> Result<(), Error> {
        // Clear all metadata describing inheritance
        trans.execute(
            "UPDATE METADATA_SCHEMA_INHERITANCE SET TRASH = 1 WHERE INHERITOR_SCHEMA_OID = ?1",
            params![self.oid]
        )?;

        // Check if self is a table
        let table_name: String = format!("TABLE{}", self.oid);
        let is_table: bool = trans.table_exists(Some("main"), &table_name)?;

        // Add inheritance from each master schema
        for master_schema in self.master_schemas.iter() {
            let master_schema_oid: i64 = master_schema.get_oid();

            // Upsert the inheritance row
            trans.execute(
                "INSERT INTO METADATA_SCHEMA_INHERITANCE (INHERITOR_SCHEMA_OID, MASTER_SCHEMA_OID) VALUES (?1, ?2) ON CONFLICT DO UPDATE SET TRASH = 0",
                params![self.oid, master_schema_oid]
            )?;

            // Update the corresponding table, if both master and inheritor schemas are tables
            if is_table {
                if let Schema::Table(_) = master_schema {
                    let master_column_name: String = format!("MASTER{master_schema_oid}_OID");
                    if !trans.column_exists(Some("main"), &table_name, &master_column_name)? {
                        // Add a column to the table that references a row in the master list
                        let alter_table_cmd: String = format!(
                            "
                            ALTER TABLE TABLE{} 
                                ADD COLUMN MASTER{master_schema_oid}_OID INTEGER
                                REFERENCES TABLE{master_schema_oid} (OID) 
                                ON UPDATE CASCADE 
                                ON DELETE CASCADE
                            ",
                            self.oid
                        );
                        trans.execute(&alter_table_cmd, [])?;
                    }
                }
            }
        }
        Ok(())
    }
}
