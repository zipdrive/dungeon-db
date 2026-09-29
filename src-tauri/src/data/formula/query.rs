use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;

use crate::data::formula::func::RecordFunc;
use crate::data::formula::value::RecordValue;
use crate::data::table::TableMetadata; 
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::column_type::TableColumnType;
use crate::util::error::Error;

#[derive(Clone, PartialEq, Eq)]
struct RecordFuncCte {
    /// The alias of the CTE.
    alias: String,

    /// The table being selected from.
    table_oid: i64,

    /// The join to the parent CTE.
    join: String,

    /// The child CTE.
    cte: HashSet<RecordFuncCte>,

    /// The columns to select.
    columns: HashSet<String>
}

impl Hash for RecordFuncCte {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.alias.hash(state)
    }
}

impl Borrow<String> for RecordFuncCte {
    fn borrow(&self) -> &String {
        &self.alias
    }
}

impl RecordFuncCte {
    /// Takes a child CTE from this CTE, or creates it if it does not exist.
    /// The child CTE will need to be added back to this set of child CTEs after manipulation is done to it.
    fn take_child(&mut self, func: &RecordFunc) -> Result<Self, Error> {
        let alias: String = func.alias();
        Ok(match self.cte.take(&alias) {
            Some(child_cte) => child_cte,
            None => match func {
                RecordFunc::Source(_) => {
                    // This shouldn't ever happen
                    return Err(Error::adhoc(format!("Something has gone wrong!")));
                }
                RecordFunc::Reference { column, .. } => {
                    match column.column_type {
                        TableColumnType::Object { table_oid, .. }
                        | TableColumnType::SingleSelect { table_oid, .. } => {
                            Self {
                                join: format!("LEFT JOIN {alias} ON t.COLUMN{} = {alias}_OID", column.oid),
                                table_oid,
                                alias,
                                cte: HashSet::new(),
                                columns: HashSet::new()
                            }
                        }
                        TableColumnType::MultiSelect { table_oid, .. } => {
                            Self {
                                join: format!(
                                    "
LEFT JOIN MULTISELECT{} ON t.OID = MULTISELECT{}.TABLE{}_OID 
LEFT JOIN {alias} ON MULTISELECT{}.TABLE{table_oid}_OID = {alias}_OID
                                    ",
                                    column.oid,
                                    column.oid,
                                    self.table_oid,
                                    column.oid
                                ),
                                alias,
                                table_oid,
                                cte: HashSet::new(),
                                columns: HashSet::new()
                            }
                        }
                        _ => {
                            return Err(Error::adhoc(format!("Column of type \"{}\" is not a Record.", column.column_type.name())))
                        }
                    }
                }
                RecordFunc::Backreference { column, .. } => {
                    match column.column_type {
                        TableColumnType::Object { table_oid, .. }
                        | TableColumnType::SingleSelect { table_oid, .. } => {
                            Self {
                                join: format!("LEFT JOIN {alias} ON t.OID = {alias}_COLUMN{}", column.oid),
                                table_oid,
                                alias,
                                cte: HashSet::new(),
                                columns: HashSet::from_iter(vec![format!("COLUMN{}", column.oid)])
                            }
                        }
                        TableColumnType::MultiSelect { table_oid, .. } => {
                            Self {
                                join: format!(
                                    "
LEFT JOIN MULTISELECT{} ON t.OID = MULTISELECT{}.TABLE{}_OID 
LEFT JOIN {alias} ON MULTISELECT{}.TABLE{table_oid}_OID = {alias}_OID
                                    ",
                                    column.oid,
                                    column.oid,
                                    self.table_oid,
                                    column.oid
                                ),
                                alias,
                                table_oid,
                                cte: HashSet::new(),
                                columns: HashSet::new()
                            }
                        }
                        _ => {
                            return Err(Error::adhoc(format!("Column of type \"{}\" is not a Record.", column.column_type.name())))
                        }
                    }
                }
                RecordFunc::Cast { table, .. } => {
                    Self {
                        join: format!("LEFT JOIN {alias} ON t.OID = {alias}_MASTER{}_OID", self.table_oid),
                        table_oid: table.oid,
                        alias,
                        cte: HashSet::new(),
                        columns: HashSet::from_iter(vec![format!("MASTER{}_OID", self.table_oid)])
                    }
                }
            }
        })
    }

    /// Adds a column to either this CTE or a child CTE, as appropriate.
    pub fn add_column(&mut self, path: &[&RecordFunc], table_oid: i64, column_name: String) -> Result<(), Error> {
        if path.len() > 0 {
            let next = path[0];
            let path = &path[1..];
            let mut child_cte = self.take_child(next)?;
            child_cte.add_column(path, table_oid, column_name)?;
            self.cte.insert(child_cte);
        } else {
            if table_oid != self.table_oid {
                self.columns.insert(format!("MASTER{table_oid}_OID"));
            }
            self.columns.insert(column_name);
        }
        Ok(())
    }

    /// List all OID filters for the CTE.
    pub fn oid_filters(&self) -> Vec<String> {
        self.cte.iter()
            .flat_map(|cte| cte.oid_filters())
            .chain(vec![format!("{}_OID", self.alias)])
            .collect()
    }

    /// Converts this object into an SQL expression.
    pub fn sql(&self) -> String {
        let sql: String = format!(
            "
{} AS (
    SELECT 
        {}
    {}
)
            ",
            self.alias,
            self.columns.iter().map(|column_name| format!("t.{column_name} AS {}_{column_name}", self.alias))
                .fold(
                    self.cte.iter()
                        .fold(
                            format!("t.OID AS {}_OID", self.alias),
                            |acc, e| format!("{acc}, {}.*", e.alias)
                        ), 
                    |acc, e| format!("{acc}, {e}")
                ),
            self.cte.iter().map(|j| &j.join).fold(format!("FROM TABLE{} t", self.table_oid), |acc, e| format!("{acc} {e}"))
        );
        self.cte.iter().fold(
            sql,
            |acc, e| format!("{acc}, {}", e.sql())
        )
    }
}

#[derive(Clone)]
pub struct RecordFuncQuery {
    /// The child CTE.
    cte: HashSet<RecordFuncCte>
}

impl RecordFuncQuery {
    /// Create a new query.
    pub fn new() -> Self {
        Self { cte: HashSet::new() }
    }

    /// Takes a child CTE from this CTE, or creates it if it does not exist.
    /// The child CTE will need to be added back to this set of child CTEs after manipulation is done to it.
    fn take_child(&mut self, func: &RecordFunc) -> Result<RecordFuncCte, Error> {
        let alias: String = func.alias();
        Ok(match self.cte.take(&alias) {
            Some(child_cte) => child_cte,
            None => match func {
                RecordFunc::Source(table) => {
                    RecordFuncCte {
                        join: String::from(""),
                        alias,
                        table_oid: table.oid.clone(),
                        cte: HashSet::new(),
                        columns: HashSet::new()
                    }
                }
                _ => {
                    // This shouldn't ever happen
                    return Err(Error::adhoc(format!("Something has gone wrong!")));
                }
            }
        })
    }

    /// Adds a column to either this CTE or a child CTE, as appropriate.
    pub fn add_column(&mut self, path: &[&RecordFunc], table_oid: i64, column_name: String) -> Result<(), Error> {
        if path.len() > 0 {
            let next = path[0];
            let path = &path[1..];
            let mut child_cte = self.take_child(next)?;
            child_cte.add_column(path, table_oid, column_name)?;
            self.cte.insert(child_cte);
        } else {
            return Err(Error::adhoc("Column does not belong to a record."));
        }
        Ok(())
    }

    /// List all OID filters for the query.
    pub fn oid_filters(&self) -> Vec<String> {
        self.cte.iter()
            .flat_map(|cte| cte.oid_filters())
            .collect()
    }

    /// Converts this object into an SQL expression of CTEs.
    pub fn sql(&self) -> String {
        let sql: String = format!(
            "
WRAPPER AS (
    SELECT 
        {}
    {}
)
            ",
            match self.cte.iter().map(|e| format!("{}.*", e.alias)).reduce(|acc, e| format!("{acc}, {e}")) {
                Some(columns) => columns,
                None => String::from("NULL")
            },
            match self.cte.iter().map(|j| j.alias.clone()).reduce(|acc, e| format!("{acc}, {e}")) {
                Some(sources) => format!("FROM {sources}"),
                None => String::from("WHERE FALSE")
            }
        );
        self.cte.iter().fold(
            sql,
            |acc, e| format!("{}, {acc}", e.sql())
        )
    }
}
