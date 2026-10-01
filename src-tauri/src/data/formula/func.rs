use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::data::formula::context::Context;
use crate::data::formula::value::{Value, TextValue, RecordValue};
use crate::data::table::TableMetadata;
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::column_type::TableColumnType;
use crate::data::table::row::{TableCellContent, TableCellTextContentFormat};
use crate::util::db::sql_one;
use crate::util::encode::{json_encode_string, sql_encode_string};
use crate::util::error::Error;


#[derive(Clone, PartialEq, Eq)]
pub enum RecordFunc {
    Source(TableMetadata),
    Reference {
        record: Box<RecordFunc>,
        column: TableColumnMetadata,
    },
    Backreference {
        record: Box<RecordFunc>,
        table: TableMetadata,
        column: TableColumnMetadata,
    },
    Cast {
        record: Box<RecordFunc>,
        table: TableMetadata,
    }
}

impl RecordFunc {
    /// Constructs a unique alias representing this table.
    pub fn alias(&self) -> String {
        match self {
            Self::Source(table) => 
                format!("TABLE{}", table.oid),
            Self::Reference { record, column } => 
                format!("{}_REF{}", record.alias(), column.oid),
            Self::Backreference { record, table, column } => 
                format!("{}_BACK{}REF{}", record.alias(), table.oid, column.oid),
            Self::Cast { record, table } => 
                format!("{}_CAST{}", record.alias(), table.oid)
        }
    }

    /// Gets the OID of the record's table.
    pub fn table_oid(&self) -> Result<i64, Error> {
        Ok(match self {
            Self::Source(table) => table.oid.clone(),
            Self::Reference { column, .. } => {
                match column.column_type {
                    TableColumnType::Object { table_oid, .. }
                    | TableColumnType::SingleSelect { table_oid, .. }
                    | TableColumnType::MultiSelect { table_oid, .. } => table_oid.clone(),
                    _ => {
                        return Err(Error::adhoc(format!("Column of type \"{}\" is not a record.", column.column_type.name())));
                    }
                }
            }
            Self::Backreference { table, .. }
            | Self::Cast { table, .. } => table.oid.clone()
        })
    }

    /// Constructs an equivalent formula.
    pub fn formula(&self) -> String {
        match self {
            Self::Source(table) => 
                format!("\"{}\"", json_encode_string(&table.name)),
            Self::Reference { record, column } => 
                format!("{}.\"{}\"", record.formula(), json_encode_string(&column.name)),
            Self::Backreference { record, table, column } => 
                format!(
                    "(\"{}\" WHERE \"{}\" = {})", 
                    json_encode_string(&table.name), 
                    json_encode_string(&column.name), 
                    record.formula()
                ),
            Self::Cast { record, table } => 
                format!("CAST({} AS \"{}\")", record.formula(), json_encode_string(&table.name))
        }
    }

    /// Linearizes the RecordFunc into an array of RecordFunc objects.
    pub fn linearize(&self) -> Vec<&Self> {
        match self {
            Self::Source(_) => vec![self],
            Self::Reference { record, .. }
            | Self::Backreference { record, .. }
            | Self::Cast { record, .. } => {
                let mut path = record.linearize();
                path.push(self);
                path
            }
        }
    }
}



pub struct FuncSql {
    /// Expression for the value returned by the function.
    pub value_expr: String,

    /// Expression to get the table cell referenced by the function.
    pub reference_expr: String 
}

impl FuncSql {
    /// Constructs a new FuncSql object with a null reference.
    pub fn new<S>(value_expr: String, type_expr: S) -> Self where S : std::fmt::Display {
        Self {
            value_expr,
            reference_expr: format!("{type_expr};")
        }
    }
}


pub enum Func {
    Column {
        record: RecordFunc,
        table_oid: i64,
        column: TableColumnMetadata,
    },

    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Text(String),

    Wrap(Box<Func>),

    Not(Box<Func>),
    And(Box<Func>, Box<Func>),
    Or(Box<Func>, Box<Func>),

    If(Box<Func>, Box<Func>, Box<Func>),
    Coalesce(Vec<Func>),
    NullIf(Box<Func>, Box<Func>),

    Abs(Box<Func>),
    Sign(Box<Func>),
    Round(Box<Func>),
    Floor(Box<Func>),
    Ceiling(Box<Func>),
    Add(Box<Func>, Box<Func>),
    Sub(Box<Func>, Box<Func>),
    Mul(Box<Func>, Box<Func>),
    Div(Box<Func>, Box<Func>),
    Mod(Box<Func>, Box<Func>),
    Pow(Box<Func>, Box<Func>),

    Concat(Box<Func>, Box<Func>),
    Upper(Box<Func>),
    Lower(Box<Func>),
    StringLength(Box<Func>),

    ToUnixEpoch(Box<Func>),
    FromUnixEpoch(Box<Func>),

    Sum(Box<Func>),
    Average(Box<Func>),
    Min(Box<Func>),
    Max(Box<Func>),
    Count(Box<Func>),
    Join {
        list: Box<Func>,
        delimiter: Box<Func>
    }
}

impl Func {
    /// Get the columns used in the query that do not belong to the inside of an aggregated function.
    pub fn get_nonaggregated_columns(&self) -> Vec<(RecordFunc, i64, TableColumnMetadata)> {
        match self {
            Self::Column { record, table_oid, column } => 
                vec![(record.clone(), table_oid.clone(), column.clone())],

            Self::Null
            | Self::Boolean(_)
            | Self::Integer(_)
            | Self::Number(_)
            | Self::Text(_) 
            | Self::Average(_)
            | Self::Count(_)
            | Self::Max(_)
            | Self::Min(_)
            | Self::Sum(_) => Vec::new(),

            Self::Wrap(a)
            | Self::Abs(a)
            | Self::Ceiling(a)
            | Self::Floor(a)
            | Self::Lower(a)
            | Self::Not(a)
            | Self::Round(a)
            | Self::Sign(a)
            | Self::StringLength(a)
            | Self::Upper(a) 
            | Self::ToUnixEpoch(a)
            | Self::FromUnixEpoch(a)
            | Self::Join { delimiter: a, .. } => a.get_all_columns(),

            Self::Add(a, b)
            | Self::And(a, b)
            | Self::Concat(a, b)
            | Self::Div(a, b)
            | Self::Mod(a, b)
            | Self::Mul(a, b)
            | Self::NullIf(a, b)
            | Self::Or(a, b)
            | Self::Pow(a, b)
            | Self::Sub(a, b) => 
                a.get_all_columns()
                    .into_iter()
                    .chain(b.get_all_columns())
                    .collect(),
            
            Self::If(a, b, c) => 
                a.get_all_columns()
                    .into_iter()
                    .chain(b.get_all_columns())
                    .chain(c.get_all_columns())
                    .collect(),

            Self::Coalesce(items) => 
                items.iter().flat_map(|a| a.get_all_columns()).collect()
        }
    }

    /// Get the columns used in the query.
    pub fn get_all_columns(&self) -> Vec<(RecordFunc, i64, TableColumnMetadata)> {
        match self {
            Self::Column { record, table_oid, column } => 
                vec![(record.clone(), table_oid.clone(), column.clone())],

            Self::Null
            | Self::Boolean(_)
            | Self::Integer(_)
            | Self::Number(_)
            | Self::Text(_) => Vec::new(),

            Self::Wrap(a)
            | Self::Abs(a)
            | Self::Ceiling(a)
            | Self::Floor(a)
            | Self::Lower(a)
            | Self::Not(a)
            | Self::Round(a)
            | Self::Sign(a)
            | Self::StringLength(a)
            | Self::ToUnixEpoch(a)
            | Self::FromUnixEpoch(a)
            | Self::Upper(a) 
            | Self::Average(a)
            | Self::Count(a)
            | Self::Max(a)
            | Self::Min(a)
            | Self::Sum(a) => a.get_all_columns(),

            Self::Add(a, b)
            | Self::And(a, b)
            | Self::Concat(a, b)
            | Self::Div(a, b)
            | Self::Mod(a, b)
            | Self::Mul(a, b)
            | Self::NullIf(a, b)
            | Self::Or(a, b)
            | Self::Pow(a, b)
            | Self::Sub(a, b)
            | Self::Join { list: a, delimiter: b } => 
                a.get_all_columns()
                    .into_iter()
                    .chain(b.get_all_columns())
                    .collect(),
            
            Self::If(a, b, c) => 
                a.get_all_columns()
                    .into_iter()
                    .chain(b.get_all_columns())
                    .chain(c.get_all_columns())
                    .collect(),

            Self::Coalesce(items) => 
                items.iter().flat_map(|a| a.get_all_columns()).collect()
        }
    }

    /// Constructs the equivalent formula representing the function.
    pub fn formula(&self) -> String {
        match self {
            Self::Abs(inner) => format!("ABS({})", inner.formula()),
            Self::Add(lhs, rhs) => format!("({} + {})", lhs.formula(), rhs.formula()),
            Self::And(lhs, rhs) => format!("({} AND {})", lhs.formula(), rhs.formula()),
            Self::Average(list) => format!("AVG({})", list.formula()),
            Self::Boolean(value) => String::from(if *value { "true" } else { "false" }),
            Self::Ceiling(inner) => format!("CEIL({})", inner.formula()),
            Self::Coalesce(items) => if items.len() == 0 {
                String::from("null")
            } else if items.len() == 1 {
                items[0].formula()
            } else {
                format!("COALESCE({})", items.iter().map(|item| item.formula()).reduce(|acc, e| format!("{acc}, {e}")).unwrap())
            },
            Self::Column { record, column, .. } => format!("{}.\"{}\"", record.formula(), json_encode_string(&column.name)),
            Self::Concat(lhs, rhs) => format!("({} || {})", lhs.formula(), rhs.formula()),
            Self::Count(list) => format!("COUNT({})", list.formula()),
            Self::Div(lhs, rhs) => format!("({} / {})", lhs.formula(), rhs.formula()),
            Self::Floor(inner) => format!("FLOOR({})", inner.formula()),
            Self::FromUnixEpoch(inner) => format!("FROMUNIXTIME({})", inner.formula()),
            Self::If(a, b1, b2) => format!("IF({}, {}, {})", a.formula(), b1.formula(), b2.formula()),
            Self::Integer(value) => format!("{value}"),
            Self::Join { list, delimiter } => format!("JOIN({}, {})", list.formula(), delimiter.formula()),
            Self::Lower(inner) => format!("LOWER({})", inner.formula()),
            Self::Max(list) => format!("MAX({})", list.formula()),
            Self::Min(list) => format!("MIN({})", list.formula()),
            Self::Mod(lhs, rhs) => format!("({} % {})", lhs.formula(), rhs.formula()),
            Self::Mul(lhs, rhs) => format!("({} * {})", lhs.formula(), rhs.formula()),
            Self::Not(inner) => format!("(NOT {})", inner.formula()),
            Self::Null => String::from("null"),
            Self::NullIf(a, b) => format!("NULLIF({}, {})", a.formula(), b.formula()),
            Self::Number(value) => format!("{value}"),
            Self::Or(lhs, rhs) => format!("({} OR {})", lhs.formula(), rhs.formula()),
            Self::Pow(lhs, rhs) => format!("POW({}, {})", lhs.formula(), rhs.formula()),
            Self::Round(inner) => format!("ROUND({})", inner.formula()),
            Self::Sign(inner) => format!("SIGN({})", inner.formula()),
            Self::StringLength(inner) => format!("LENGTH({})", inner.formula()),
            Self::Sub(lhs, rhs) => format!("({} - {})", lhs.formula(), rhs.formula()),
            Self::Sum(list) => format!("SUM({})", list.formula()),
            Self::Text(value) => format!("'{}'", sql_encode_string(value)),
            Self::ToUnixEpoch(inner) => format!("UNIXTIME({})", inner.formula()),
            Self::Upper(inner) => format!("UPPER({})", inner.formula()),
            Self::Wrap(inner) => format!("({})", inner.formula())
        }
    }

    /// Converts the function into an equivalent SQL expression.
    pub fn sql(&self) -> Result<FuncSql, Error> {
        Ok(match self {
            Self::Column { record, table_oid, column } => {
                let alias: String = record.alias();
                FuncSql {
                    value_expr: format!("{alias}_COLUMN{}", column.name),
                    reference_expr: format!(
                        "('{};' || COALESCE('{table_oid}:{}:' || CAST({} AS TEXT), ''))",
                        column.column_type.name(),
                        column.oid,
                        if record.table_oid()? == *table_oid {
                            format!("{alias}_OID")
                        } else {
                            format!("{alias}_MASTER{table_oid}_OID")
                        }
                    )
                }
            }

            Self::Null => FuncSql::new(String::from("NULL"), "Text"),
            Self::Boolean(value) => FuncSql::new(String::from(if *value { "TRUE" } else { "FALSE" }), "Boolean"),
            Self::Integer(value) => FuncSql::new(format!("{value}"), "Text"),
            Self::Number(value) => FuncSql::new(format!("{value}"), "Text"),
            Self::Text(value) => FuncSql::new(format!("'{}'", sql_encode_string(value)), "Text"),

            Self::Wrap(inner) => inner.sql()?,

            Self::Not(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("(NOT {})", inner.value_expr), "Boolean")
            }
            Self::And(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} AND {})", lhs.value_expr, rhs.value_expr), "Boolean")
            }
            Self::Or(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} OR {})", lhs.value_expr, rhs.value_expr), "Boolean")
            }
            
            Self::Abs(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("ABS({})", inner.value_expr), "Text")
            }
            Self::Sign(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("SIGN({})", inner.value_expr), "Text")
            }
            Self::Round(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("ROUND({})", inner.value_expr), "Text")
            }
            Self::Floor(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("FLOOR({})", inner.value_expr), "Text")
            }
            Self::Ceiling(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("CEIL({})", inner.value_expr), "Text")
            }
            Self::Add(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} + {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Sub(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} - {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Mul(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} * {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Div(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} / {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Mod(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} % {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Pow(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("POW({}, {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            
            Self::Concat(lhs, rhs) => {
                let lhs = lhs.sql()?;
                let rhs = rhs.sql()?;
                FuncSql::new(format!("({} || {})", lhs.value_expr, rhs.value_expr), "Text")
            }
            Self::Upper(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("UPPER({})", inner.value_expr), "Text")
            }
            Self::Lower(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("LOWER({})", inner.value_expr), "Text")
            }
            Self::StringLength(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("LENGTH({})", inner.value_expr), "Integer")
            }

            Self::ToUnixEpoch(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("UNIXEPOCH({}, 'julianday')", inner.value_expr), "Integer")
            }
            Self::FromUnixEpoch(inner) => {
                let inner = inner.sql()?;
                FuncSql::new(format!("JULIANDAY({}, 'unixepoch')", inner.value_expr), "Datetime")
            }

            Self::If(a, b1, b2) => {
                let a = a.sql()?;
                let b1 = b1.sql()?;
                let b2 = b2.sql()?;
                FuncSql {
                    value_expr: format!("IIF({}, {}, {})", a.value_expr, b1.value_expr, b2.value_expr),
                    reference_expr: if b1.reference_expr == b2.reference_expr {
                        b1.reference_expr 
                    } else {
                        format!("IIF({}, {}, {})", a.value_expr, b1.reference_expr, b2.reference_expr)
                    }
                }
            }
            Self::NullIf(a, b) => {
                let a = a.sql()?;
                let b = b.sql()?;
                FuncSql {
                    value_expr: format!("NULLIF({}, {})", a.value_expr, b.value_expr),
                    reference_expr: if a.reference_expr == "NULL" {
                        String::from("NULL")
                    } else {
                        format!("IIF({} IS {}, NULL, {})", a.value_expr, b.value_expr, a.reference_expr)
                    }
                }
            }
            Self::Coalesce(items) => {
                let mut sql: Vec<FuncSql> = Vec::new();
                for item in items {
                    sql.push(item.sql()?);
                }
                if sql.len() > 1 {
                    FuncSql {
                        value_expr: format!("COALESCE({})", sql.iter().map(|s| s.value_expr.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()),
                        reference_expr: format!("CASE {} ELSE NULL END", sql.iter().map(|s| format!("WHEN {} IS NOT NULL THEN {}", s.value_expr, s.reference_expr)).reduce(|acc, e| format!("{acc} {e}")).unwrap())
                    }
                } else if sql.len() == 1 {
                    sql.pop().unwrap()
                } else {
                    FuncSql::new(String::from("NULL"), "Text")
                }
            }

            Self::Average(list) => {
                let list = list.sql()?;
                FuncSql::new(format!("AVG({})", list.value_expr), "Text")
            }
            Self::Count(list) => {
                let list = list.sql()?;
                FuncSql::new(format!("COUNT({})", list.value_expr), "Text")
            }
            Self::Join { list, delimiter } => {
                let list = list.sql()?;
                let delimiter = delimiter.sql()?;
                FuncSql::new(format!("GROUP_CONCAT({}, {})", list.value_expr, delimiter.value_expr), "Text")
            }
            Self::Max(list) => {
                let list = list.sql()?;
                FuncSql::new(format!("MAX({})", list.value_expr), "Text")
            }
            Self::Min(list) => {
                let list = list.sql()?;
                FuncSql::new(format!("MIN({})", list.value_expr), "Text")
            }
            Self::Sum(list) => {
                let list = list.sql()?;
                FuncSql::new(format!("SUM({})", list.value_expr), "Text")
            }
        })
    }
}