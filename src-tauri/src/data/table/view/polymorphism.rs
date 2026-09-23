use std::borrow::Borrow;
use std::collections::{HashSet};
use std::hash::Hash;
use regex::Regex;
use rusqlite::{Connection, params};
use crate::util::db::{sql_collect, sql_execute};
use crate::util::error::Error;

#[derive(Eq)]
struct PolymorphismCte {
    /// The OID of the table.
    table_oid: i64,

    /// The inheritor tables.
    inheritors: HashSet<PolymorphismCte>
}

impl Hash for PolymorphismCte {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_oid.hash(state)
    }
}

impl Borrow<i64> for PolymorphismCte {
    fn borrow(&self) -> &i64 {
        &self.table_oid
    }
}

impl PartialEq for PolymorphismCte {
    fn eq(&self, other: &Self) -> bool {
        self.table_oid == other.table_oid
    }
}

impl PolymorphismCte {
    /// Inserts an inheritor datasource path into the CTE chain.
    pub fn push<S>(&mut self, path: S) -> Result<(), Error> where S : AsRef<str> {
        let inheritor_regex: Regex = Regex::new(r#"^_INHERITOR(\d+)(_INHERITOR(?:\d+))*$"#).unwrap();
        if let Some(inheritor_caps) = inheritor_regex.captures(path.as_ref()) {
            let (_, [inheritor_table_oid_str, remaining_path]) = inheritor_caps.extract();
            let inheritor_table_oid: i64 = inheritor_table_oid_str.parse::<i64>().unwrap();
            let mut cte = self.inheritors.take(&inheritor_table_oid)
                .unwrap_or(Self {
                    table_oid: inheritor_table_oid,
                    inheritors: HashSet::new()
                });
            cte.push(remaining_path)?;
            self.inheritors.insert(cte);
        }
        Ok(())
    }

    /// Converts the CTE (and any child CTEs) to SQL.
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
                .map(|inheritor| inheritor.to_sql(Some(self.table_oid)))
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
                        self.inheritors.iter()
                            .fold(
                                format!("{}", self.table_oid),
                                |acc, e| format!("CTE{}.OBJECT_TABLE_OID, {acc}", e.table_oid)
                            ),
                        self.inheritors.iter()
                            .fold(
                                String::from("t.OID"),
                                |acc, e| format!("CTE{}.OBJECT_ROW_OID, {acc}", e.table_oid)
                            ),
                        self.inheritors.iter()
                            .fold(
                                format!("FROM __TABLE{} t", self.table_oid),
                                |acc, e| format!("{acc} LEFT JOIN CTE{} ON CTE{}.MASTER{}_OID = t.OID", e.table_oid, e.table_oid, self.table_oid)
                            )
                    ),
                    |acc, e| format!("{acc}, {e}")
                )
        }
    }
}

/// Rebuilds the polymorphism view for a table.
pub fn rebuild(conn: &Connection, table_oid: i64) -> Result<(), Error> {
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
        let mut cte: PolymorphismCte = PolymorphismCte { 
            table_oid, 
            inheritors: HashSet::new()
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