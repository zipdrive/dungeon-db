use std::collections::HashMap;

use rusqlite::{Connection, params};
use crate::util::db::{RowWrapper, sql_collect, sql_execute, sql_one};
use crate::util::db;
use crate::util::error::Error;
use crate::data::table::TableMetadata;

struct PolymorphismCte {
    /// The OID of the table.
    table_oid: i64,

    /// The inheritor tables.
    inheritors: HashMap<i64, PolymorphismCte>
}

impl PolymorphismCte {
    pub fn push(&self, path: String) -> Result<(), Error> {
        todo!("Need to implement PolymorphismCte.push()");
    }

    pub fn to_sql(&self, master_oid: Option<i64>) -> String {
        if self.inheritors.len() == 0 {
            format!(
                "
CTE{} AS (
    SELECT 
        OID,
        {}
        {} AS OBJECT_TABLE_OID,
        OID AS OBJECT_ROW_OID
    FROM __TABLE{}
    WHERE NOT TRASH 
)
                ",
                self.table_oid,
                match master_oid {
                    Some(master_oid) => format!("MASTER{master_oid}_OID,"),
                    None => String::from("")
                },
                self.table_oid,
                self.table_oid
            )
        } else {
            self.inheritors.iter()
                .map(|(_, inheritor)| inheritor.to_sql(Some(self.table_oid)))
                .fold(
                    format!(
                        "
CTE{} AS (
    SELECT
        t.OID,
        {}
        COALESCE({}) AS OBJECT_TABLE_OID,
        COALESCE({}) AS OBJECT_ROW_OID
    {}
    WHERE NOT t.TRASH
                        ",
                        self.table_oid,
                        match master_oid {
                            Some(master_oid) => format!("MASTER{master_oid}_OID,"),
                            None => String::from("")
                        },
                        self.inheritors.keys()
                            .fold(
                                format!("{}", self.table_oid),
                                |acc, e| format!("CTE{e}.OBJECT_TABLE_OID, {acc}")
                            ),
                        self.inheritors.keys()
                            .fold(
                                String::from("t.OID"),
                                |acc, e| format!("CTE{e}.OBJECT_ROW_OID, {acc}")
                            ),
                        self.inheritors.keys()
                            .fold(
                                format!("FROM __TABLE{} t", self.table_oid),
                                |acc, e| format!("{acc} LEFT JOIN CTE{e} ON CTE{e}.MASTER{}_OID = t.OID", self.table_oid)
                            )
                    ),
                    |acc, e| format!("{acc}, {e}")
                )
        }
    }
}

/// Rebuilds the polymorphism view for a table.
fn rebuild_polymorphism(conn: &Connection, table_oid: i64) -> Result<(), Error> {
    // Drop the old polymorphism view, if it exists
    sql_execute(conn, format!("DROP VIEW IF EXISTS OBJECT{table_oid}"), [])?;

    // Query for all inheritor tables
    let inheritor_datasource_paths: Vec<String> = sql_collect(
        conn, 
        "SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_TABLE_INHERITANCE_PATH WHERE MASTER_TABLE_OID = ?1", 
        params![table_oid], 
        |row| row.get("INHERITOR_DATASOURCE_PATH")
    )?;
    if inheritor_datasource_paths.len() == 0 {
        sql_execute(
            conn, 
            format!(
                "
CREATE VIEW OBJECT{table_oid} AS 
    SELECT 
        OID,
        {table_oid} AS OBJECT_TABLE_OID,
        OID AS OBJECT_ROW_OID
    FROM __TABLE{table_oid}
                "
            ), 
            []
        )?;
    } else {
        let cte: PolymorphismCte = PolymorphismCte { 
            table_oid, 
            inheritors: HashMap::new()
        };
        for path in inheritor_datasource_paths {
            cte.push(path)?;
        }

        sql_execute(
            conn,
            format!(
                "
CREATE VIEW OBJECT{table_oid} AS 
    WITH {}
    SELECT 
        OID,
        OBJECT_TABLE_OID,
        OBJECT_ROW_OID
    FROM CTE{table_oid}
                ",
                cte.to_sql(None)
            ), 
            []
        )?;
    }
    Ok(())
}

fn rebuild_data(conn: &Connection, table_oid: i64) -> Result<(), Error> {
    Ok(())
}

/// Rebuilds the views for the given table.
pub fn rebuild(conn: &Connection, table_oid: i64) -> Result<(), Error> {
    // Get all tables that either inherit from or are inherited by the given table
    let related_oids: Vec<(i64, bool, bool)> = sql_collect(
        conn,
        "
SELECT 
    ?1 AS TABLE_OID,
    TRUE AS REBUILD_POLYMORPHISM,
    TRUE AS REBUILD_DATA

UNION ALL

SELECT
    INHERITOR_TABLE_OID,
    FALSE AS REBUILD_POLYMORPHISM,
    TRUE AS REBUILD_DATA
FROM METADATA_TABLE_INHERITANCE_PATH
WHERE MASTER_TABLE_OID = ?1

UNION ALL 

SELECT
    MASTER_TABLE_OID,
    TRUE AS REBUILD_POLYMORPHISM,
    FALSE AS REBUILD_DATA
FROM METADATA_TABLE_INHERITANCE_PATH
WHERE INHERITOR_TABLE_OID = ?1
        ",
        params![table_oid],
        |row| Ok((
            row.get::<_, i64>("TABLE_OID")?,
            row.get::<_, bool>("REBUILD_POLYMORPHISM")?,
            row.get::<_, bool>("REBUILD_DATA")?
        ))
    )?;

    // Rebuild each of the aforementioned tables' views
    for (table_oid, needs_to_rebuild_polymorphism, needs_to_rebuild_data) in related_oids {
        if needs_to_rebuild_polymorphism {
            rebuild_polymorphism(conn, table_oid.clone())?;
        }
        if needs_to_rebuild_data {
            rebuild_data(conn, table_oid)?;
        }
    }
    Ok(())
}