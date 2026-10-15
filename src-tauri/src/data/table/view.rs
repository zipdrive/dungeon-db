use rusqlite::{Connection, params};
use crate::util::db::{sql_collect};
use crate::util::error::Error;

mod polymorphism;
mod data;

/// Rebuilds the views for the given table.
pub fn rebuild(conn: &Connection, table_oid: i64) -> Result<(), Error> {
    // Get all tables that either inherit from or are inherited by the given table
    let related_oids: Vec<(i64, bool, bool)> = sql_collect(
        conn,
        "
WITH RECURSIVE FLATTENING (INHERITOR_TABLE_OID, MASTER_TABLE_OID) AS (
    SELECT
        u.INHERITOR_TABLE_OID,
        u.MASTER_TABLE_OID
    FROM __METADATA_TABLE_INHERITANCE u

    UNION

    SELECT
        u.INHERITOR_TABLE_OID,
        s.MASTER_TABLE_OID
    FROM FLATTENING s
    INNER JOIN __METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.INHERITOR_TABLE_OID
)

SELECT 
    ?1 AS TABLE_OID,
    TRUE AS REBUILD_POLYMORPHISM,
    TRUE AS REBUILD_DATA

UNION ALL

SELECT
    INHERITOR_TABLE_OID,
    FALSE AS REBUILD_POLYMORPHISM,
    TRUE AS REBUILD_DATA
FROM FLATTENING
WHERE MASTER_TABLE_OID = ?1

UNION ALL 

SELECT
    MASTER_TABLE_OID,
    TRUE AS REBUILD_POLYMORPHISM,
    FALSE AS REBUILD_DATA
FROM FLATTENING
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
            polymorphism::rebuild(conn, table_oid.clone())?;
        }
        if needs_to_rebuild_data {
            data::rebuild(conn, table_oid)?;
        }
    }
    Ok(())
}