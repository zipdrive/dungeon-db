use crate::util::channel::Sender;
use crate::util::db;
use crate::util::error::Error;
use crate::data::{datasource, table, report};
use rusqlite::{Connection, Transaction, OptionalExtension, params};
use serde::{Serialize, Deserialize};
use std::collections::{HashSet, HashMap};
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
#[serde(rename_all="camelCase")]
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
                INNER JOIN METADATA_SCHEMA s ON s.OID = tbl.OID AND NOT s.TRASH
                WHERE tbl.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM TABLE_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID AND NOT inh.TRASH
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID AND NOT s.TRASH
                WHERE EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = s.OID)

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
                INNER JOIN METADATA_SCHEMA s ON s.OID = r.OID AND NOT s.TRASH
                WHERE r.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM REPORT_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID AND NOT inh.TRASH
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID AND NOT s.TRASH
                WHERE EXISTS(SELECT OID FROM METADATA_REPORT WHERE OID = s.OID)

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
#[serde(rename_all="camelCase")]
pub struct SelectedHierarchicalListItemMetadata {
    oid: i64,
    name: String,
    master_oid: Option<i64>,
    level: i64,
    selected: bool
}

impl SelectedHierarchicalListItemMetadata {
    /// Queries for all tables inheriting from this one.
    pub fn query_inheritor_tables<'a>(mut sender: Sender<'a, Self>, table_oid: i64, row_oid: i64) -> Result<(), Error> {
        let conn: Connection = db::open()?;

        let mut sub_row_oids: HashMap<i64, i64> = HashMap::new();

        // Run query for flat table data
        for list_item_result in conn.prepare("
            WITH TABLE_HIERARCHY (OID, NAME, MASTER_OID, LEVEL) AS (
                SELECT
                    s.OID,
                    s.NAME,
                    NULL AS MASTER_OID,
                    0 AS LEVEL
                FROM METADATA_SCHEMA s
                WHERE s.OID = ?1 AND NOT s.TRASH

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL
                FROM TABLE_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID AND NOT inh.TRASH
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID AND NOT s.TRASH
                WHERE EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = s.OID)

                ORDER BY LEVEL DESC, NAME -- Order depth first, then by name within a depth
            )
            SELECT * FROM TABLE_HIERARCHY
            ")?
            .query_map(params![table_oid], |row| {
                Ok::<Self, rusqlite::Error>(Self {
                    oid: row.get("OID")?,
                    name: row.get("NAME")?,
                    master_oid: row.get("MASTER_OID")?,
                    level: row.get("LEVEL")?,
                    selected: false
                })
            })? {
            
            // Query to see if the subtype is currently selected
            let mut list_item: Self = list_item_result?;
            if let Some(master_table_oid) = list_item.master_oid {
                if let Some(master_row_oid) = sub_row_oids.get(&master_table_oid) {
                    let sql_select: String = format!("SELECT OID FROM TABLE{} WHERE MASTER{master_table_oid}_OID = ?1 AND NOT TRASH", list_item.oid);
                    if let Some(inheritor_row_oid) = conn.query_one(&sql_select, params![master_row_oid], |row| row.get(0)).optional()? {
                        sub_row_oids.insert(list_item.oid, inheritor_row_oid);
                        list_item.selected = true;
                    }
                }
            } else {
                sub_row_oids.insert(table_oid, row_oid);
                list_item.selected = true;
            }
            // Send the subtype as a payload
            sender.send(list_item)?;
        }
        Ok(())
    }
}



#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
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
                        (s.OID IS ?1) AS DISABLED
                    FROM METADATA_SCHEMA s 
                    WHERE (NOT s.TRASH)
                        AND s.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                    UNION

                    SELECT
                        s.OID,
                        s.NAME,
                        h.OID AS MASTER_OID,
                        h.LEVEL + 1 AS LEVEL,
                        (s.OID IS ?1 OR s.OID IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1)) AS DISABLED
                    FROM SCHEMA_HIERARCHY h
                    INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID AND NOT inh.TRASH
                    INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID AND NOT s.TRASH

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
                    (s.OID IS ?1) AS DISABLED
                FROM METADATA_REPORT r
                INNER JOIN METADATA_SCHEMA s ON s.OID = r.OID AND NOT s.TRASH
                WHERE r.OID NOT IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE)

                UNION

                SELECT
                    s.OID,
                    s.NAME,
                    h.OID AS MASTER_OID,
                    h.LEVEL + 1 AS LEVEL,
                    (s.OID IS ?1 OR s.OID IN (SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1)) AS DISABLED
                FROM REPORT_HIERARCHY h
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = h.OID AND NOT inh.TRASH
                INNER JOIN METADATA_REPORT r ON r.OID = inh.INHERITOR_SCHEMA_OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID AND NOT s.TRASH

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
    pub master_schema_oids: HashSet<i64>,
    pub order_by_column_oids: Vec<(i64, bool)>
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
        let mut master_schemas: HashSet<i64> = HashSet::new();
        {
            let mut master_schema_oid_statement = conn.prepare(
                "
                SELECT 
                    u.MASTER_SCHEMA_OID 
                FROM METADATA_SCHEMA_INHERITANCE u
                INNER JOIN METADATA_SCHEMA s ON s.OID = u.MASTER_SCHEMA_OID AND NOT s.TRASH
                WHERE u.INHERITOR_SCHEMA_OID = ?1 
                    AND NOT u.TRASH
                "
            )?;
            let master_schema_oid_rows = master_schema_oid_statement.query_and_then(
                params![oid], 
                |row| row.get::<_, i64>(0)
            )?;
            for master_schema_oid_result in master_schema_oid_rows {
                master_schemas.insert(master_schema_oid_result?);
            }
        }

        // Query for ORDER BY columns
        let mut order_by_column_oids: Vec<(i64, bool)> = Vec::new();
        {
            let mut order_by_column_oids_statement = conn.prepare(
                "
                SELECT 
                    COLUMN_OID,
                    SORT_ASCENDING
                FROM METADATA_SCHEMA_ORDERBY_VIEW
                WHERE SCHEMA_OID = ?1
                "
            )?;
            let order_by_column_oids_rows = order_by_column_oids_statement.query_and_then(
                params![oid], 
                |row| Ok::<(i64, bool), rusqlite::Error>((
                    row.get("COLUMN_OID")?,
                    row.get("SORT_ASCENDING")?
                ))
            )?;
            for order_by_column_oids_result in order_by_column_oids_rows {
                let (order_by_column_oid, order_by_column_ascending) = order_by_column_oids_result?;
                order_by_column_oids.push((
                    order_by_column_oid, 
                    order_by_column_ascending
                ));
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
                    master_schema_oids: master_schemas,
                    order_by_column_oids
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
        self.set_transact(trans)?;
        Ok(())
    }

    /// Overwrites the metadata of the schema.
    pub fn set(&self, trans: &Transaction) -> Result<(), Error> {
        // Overwrite schema metadata
        trans.execute("UPDATE METADATA_SCHEMA SET NAME = ?1 WHERE OID = ?2", params![&self.name, self.oid])?;

        // Overwrite the inheritance pattern
        self.set_transact(trans)?;
        Ok(())
    }

    /// Sets the inheritance pattern for the schema.
    fn set_transact(&self, trans: &Transaction) -> Result<(), Error> {
        // Clear all metadata describing inheritance
        trans.execute(
            "UPDATE METADATA_SCHEMA_INHERITANCE SET TRASH = TRUE WHERE INHERITOR_SCHEMA_OID = ?1",
            params![self.oid]
        )?;

        // Check if self is a table
        let table_name: String = format!("TABLE{}", self.oid);
        let is_table: bool = trans.table_exists(Some("main"), &table_name)?;

        // Add inheritance from each master schema
        for master_schema_oid in self.master_schema_oids.iter() {
            // Upsert the inheritance row
            trans.execute(
                "INSERT INTO METADATA_SCHEMA_INHERITANCE (INHERITOR_SCHEMA_OID, MASTER_SCHEMA_OID) VALUES (?1, ?2) ON CONFLICT DO UPDATE SET TRASH = FALSE",
                params![self.oid, master_schema_oid]
            )?;

            // Update the corresponding table, if both master and inheritor schemas are tables
            if is_table {
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

        // Trash all previous rows of ORDER BY
        trans.execute("UPDATE METADATA_SCHEMA_ORDERBY SET TRASH = TRUE WHERE SCHEMA_OID = ?1", params![self.oid])?;
        // Set new rows of ORDER BY
        for (order_by_column_ordering, (order_by_column_oid, order_by_column_ascending)) in self.order_by_column_oids.iter().enumerate() {
            
            let order_by_column_ordering: i64 = match i64::try_from(order_by_column_ordering) {
                Ok(len) => len,
                Err(_) => {
                    return Err(Error::AdhocError("More than 9,223,372,036,854,775,807 columns."));
                }
            };
            trans.execute(
                "
                INSERT INTO METADATA_SCHEMA_ORDERBY 
                    (SCHEMA_OID, COLUMN_OID, ORDERING, SORT_ASCENDING)
                    VALUES
                    (?1, ?2, ?3, ?4)
                ON CONFLICT DO UPDATE SET 
                    TRASH = FALSE,
                    ORDERING = excluded.ORDERING,
                    SORT_ASCENDING = excluded.SORT_ASCENDING
                WHERE EXISTS(
                    SELECT
                        c.OID
                    FROM METADATA_COLUMN c
                    WHERE c.OID = excluded.COLUMN_OID
                        AND NOT c.TRASH
                        AND (c.SCHEMA_OID = excluded.SCHEMA_OID
                            OR EXISTS(SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = excluded.SCHEMA_OID)
                        )
                )
                ",
                params![self.oid, order_by_column_oid, order_by_column_ordering, order_by_column_ascending]
            )?;
        }

        Ok(())
    }
}
