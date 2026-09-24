use std::borrow::Borrow;
use std::hash::Hash;
use std::collections::{
    HashMap,
    HashSet
};
use regex::Regex;
use rusqlite::Connection;

use crate::data::table::column::TableColumnMetadata;
use crate::data::table::column_type::{Primitive, TableColumnType};
use crate::util::db::sql_execute;
use crate::util::error::Error;


#[derive(Clone, Eq)]
struct DataCteColumn {
    /// The OID of the column.
    oid: i64,
    
    /// The expression for the column value.
    value_expr: String,

    /// The ordinal for the column value.
    value_ord: String
}

impl PartialEq for DataCteColumn {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

/// A constructor for a CTE that pulls columns from a datasource.
#[derive(Eq)]
struct DataCte {
    /// The main datasource.
    table_oid: i64,

    /// The columns queried in this CTE.
    columns: Vec<DataCteColumn>,

    /// Master tables to this one.
    master_ctes: HashSet<DataCte>
}

impl Hash for DataCte {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_oid.hash(state)
    }
}

impl Borrow<i64> for DataCte {
    fn borrow(&self) -> &i64 {
        &self.table_oid
    }
}

impl PartialEq for DataCte {
    fn eq(&self, other: &Self) -> bool {
        self.table_oid == other.table_oid
    }
}

impl DataCte {
    /// Constructs a new CTE from the datasource.
    pub fn new(table_oid: i64) -> Self {
        Self {
            table_oid,
            columns: Vec::new(),
            master_ctes: HashSet::new()
        }
    }

    /// Registers a child datasource of this datasource via path.
    pub fn add_column<S>(&mut self, path: S, column: TableColumnMetadata) -> Result<(), Error> where S : AsRef<str> {
        let regex: Regex = Regex::new(r#"^_MASTER(\d+)(_MASTER(?:\d+))*$"#).unwrap();
        if let Some(caps) = regex.captures(path.as_ref()) {
            // If the regex matches, there is still at least one datasource to add first

            let (_, [table_oid_str, remaining_path]) = caps.extract();
            let table_oid: i64 = table_oid_str.parse::<i64>().unwrap();

            // Get or create CTE for the datasource
            let mut cte: Self = self.master_ctes.take(&table_oid)
                .unwrap_or(Self::new(table_oid));

            // Add datasources from remaining path, if any
            cte.add_column(remaining_path, column)?;

            // Reinsert the CTE back into the set
            self.master_ctes.insert(cte);
        } else {
            // No datasource to be added, so insert the column
            self.columns.push(DataCteColumn { 
                value_expr: match column.column_type {
                    TableColumnType::Primitive { .. }
                    | TableColumnType::File { .. }
                    | TableColumnType::Object { .. }
                    | TableColumnType::SingleSelect { .. } => {
                        format!("t.COLUMN{}", column.oid)
                    }
                    TableColumnType::MultiSelect { table_oid, .. } => {
                        format!(
                            "(SELECT GROUP_CONCAT(CAST(m.TABLE{table_oid}_OID AS TEXT), ',') FROM MULTISELECT{} m WHERE m.TABLE{}_OID = t.OID GROUP BY m.TABLE{}_OID)",
                            column.oid,
                            self.table_oid,
                            self.table_oid
                        )
                    }
                    _ => {
                        // Virtual column. Do not add to view.
                        return Ok(());
                    }
                }, 
                value_ord: format!("COLUMN{}", column.oid),
                oid: column.oid
            });

            if let TableColumnType::Primitive { primitive, .. } = column.column_type {
                match primitive {
                    Primitive::Date => {
                        // Additionally add label for date
                        self.columns.push(DataCteColumn {
                            oid: column.oid.clone(),
                            value_expr: format!("DATE(t.COLUMN{}, 'julianday')", column.oid),
                            value_ord: format!("COLUMN{}_LABEL", column.oid)
                        });
                    }
                    Primitive::Datetime => {
                        // Additionally add label for datetime
                        self.columns.push(DataCteColumn {
                            oid: column.oid.clone(),
                            value_expr: format!("STRFTIME('%FT%TZ', t.COLUMN{}, 'julianday')", column.oid),
                            value_ord: format!("COLUMN{}_LABEL", column.oid)
                        });
                    }
                    _ => {} // Do not add label column
                }
            }
        }
        Ok(())
    }

    /// Builds the SQL statement for this CTE.
    pub fn to_sql(&self, is_root: bool) -> Result<String, Error> {
        let mut sql: Vec<String> = vec![
            format!(
                "
CTE{} AS (
    SELECT
        -- The row OID
        t.OID AS {}
        -- Columns directly owned by this table
        {}
        -- Columns from master tables
        {}
    FROM __TABLE{} t
    -- Joins to master table CTEs
    {}
    WHERE NOT t.TRASH
)
                ",
                self.table_oid,
                if is_root { String::from("OID") } else { format!("MASTER{}_OID", self.table_oid) },

                // Columns directly owned by this table
                self.columns.iter()
                    .map(|col| format!("{} AS {}", col.value_expr, col.value_ord))
                    .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

                // Columns from master tables
                self.master_ctes.iter()
                    .map(|cte| format!("CTE{}.*", cte.table_oid))
                    .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

                self.table_oid,

                // Joins to child datasources
                {
                    self.master_ctes.iter()
                        .fold(String::from(""),
                            |acc, e| format!(
                                "{acc} INNER JOIN CTE{} ON CTE{}.MASTER{}_OID = t.MASTER{}_OID",
                                e.table_oid,
                                e.table_oid,
                                e.table_oid,
                                e.table_oid
                            )
                        )
                }
            )
        ];

        for cte in self.master_ctes.iter() {
            sql.push(cte.to_sql(false)?);
        }
        Ok(sql.into_iter().reduce(|acc, e| format!("{acc}, {e}")).unwrap())
    }
}


/// Rebuilds the data view for a table.
pub fn rebuild(conn: &Connection, table_oid: i64) -> Result<(), Error> {
    // Drop the old data view, if it exists
    sql_execute(conn, format!("DROP VIEW IF EXISTS TABLE{table_oid}"), [])?;

    // Query for all columns
    let columns: Vec<(i64, String, TableColumnMetadata)> = TableColumnMetadata::conn_query_all(conn, table_oid)?;
    if columns.len() == 0 {
        sql_execute(
            conn, 
            format!(
                "
CREATE VIEW TABLE{table_oid} AS 
    SELECT 
        OID
    FROM __TABLE{table_oid}
    WHERE NOT TRASH
                "
            ), 
            []
        )?;
    } else {
        let mut cte: DataCte = DataCte::new(table_oid);
        for (_, datasource_path, column) in columns {
            cte.add_column(datasource_path, column)?;
        }

        sql_execute(
            conn,
            format!(
                "
CREATE VIEW TABLE{table_oid} AS 
    WITH {}
    SELECT 
        *
    FROM CTE{table_oid}
                ",
                cte.to_sql(true)?
            ), 
            []
        )?;
    }
    Ok(())
}