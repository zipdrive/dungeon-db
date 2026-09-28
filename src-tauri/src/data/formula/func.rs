use std::collections::HashMap;

use rusqlite::{Connection, params};

use crate::data::formula::context::Context;
use crate::data::formula::value::{Value, TextValue, RecordValue};
use crate::data::table::TableMetadata;
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::column_type::TableColumnType;
use crate::data::table::row::{TableCellContent, TableCellTextContentFormat};
use crate::util::db::sql_one;
use crate::util::encode::json_encode_string;
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


pub enum Func {
    Column {
        record: Box<RecordFunc>,
        table_oid: i64,
        column: TableColumnMetadata,
    },

    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Text(String),

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

    FileName(Box<Func>),
    FileSize(Box<Func>),

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
    /// Get the columns that are not part of the grouping.
    fn get_nongrouped_columns(&self) -> Vec<(RecordFunc, i64, TableColumnMetadata)> {
        match self {
            Self::Column { record, table_oid, column } => 
                vec![(*record.clone(), table_oid.clone(), column.clone())],

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

            Self::Abs(a)
            | Self::Ceiling(a)
            | Self::FileName(a)
            | Self::FileSize(a)
            | Self::Floor(a)
            | Self::Lower(a)
            | Self::Not(a)
            | Self::Round(a)
            | Self::Sign(a)
            | Self::StringLength(a)
            | Self::Upper(a) 
            | Self::Join { delimiter: a, .. } => a.get_nongrouped_columns(),

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
                a.get_nongrouped_columns()
                    .into_iter()
                    .chain(b.get_nongrouped_columns())
                    .collect(),
            
            Self::If(a, b, c) => 
                a.get_nongrouped_columns()
                    .into_iter()
                    .chain(b.get_nongrouped_columns())
                    .chain(c.get_nongrouped_columns())
                    .collect(),

            Self::Coalesce(items) => items.iter().flat_map(|a| a.get_nongrouped_columns()).collect()
        }
    }

    fn eval(&self, context: &Context) -> Result<Value, Error> {
        Ok(match self {
            Self::Column { record, table_oid, column } => {
                let cell = context.get_column(record, table_oid.clone(), column)?;
                Value::from_cell(&cell)?
            }

            Self::Null => Value::new_null(),
            Self::Boolean(value) => Value::Boolean {
                value: value.clone(),
                reference: None
            },
            Self::Integer(value) => Value::Integer {
                value: value.clone(),
                reference: None
            },
            Self::Number(value) => Value::Number {
                value: value.clone(),
                reference: None
            },
            Self::Text(value) => Value::Text { 
                value: TextValue {
                    text: value.clone(), 
                    format: TableCellTextContentFormat::Plain 
                },
                reference: None
            },

            Self::Not(inner) => {
                let value = inner.eval(context)?.into_bool("NOT")?;
                Value::Boolean { 
                    value: !value, 
                    reference: None 
                }
            }
            Self::And(lhs, rhs) => {
                let lhs = lhs.eval(context)?.into_bool("AND, argument lhs")?;
                let rhs = rhs.eval(context)?.into_bool("AND, argument rhs")?;
                Value::Boolean { 
                    value: lhs && rhs, 
                    reference: None 
                }
            }
            Self::Or(lhs, rhs) => {
                let lhs = lhs.eval(context)?.into_bool("OR, argument lhs")?;
                let rhs = rhs.eval(context)?.into_bool("OR, argument rhs")?;
                Value::Boolean { 
                    value: lhs || rhs, 
                    reference: None 
                }
            }

            Self::Abs(inner) => {
                let inner = inner.eval(context)?;
                if let Value::Integer { value: inner, .. } = inner {
                    return Ok(Value::Integer { value: inner.abs(), reference: None });
                }
                let Some(inner) = inner.into_f64("ABS")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: inner.abs(),
                    reference: None 
                }
            }
            Self::Sign(inner) => {
                let inner = inner.eval(context)?;
                if let Value::Integer { value: inner, .. } = inner {
                    return Ok(Value::Integer { value: inner.signum(), reference: None });
                }
                let Some(inner) = inner.into_f64("SIGN")? else { return Ok(Value::new_null()); };
                if inner.is_nan() {
                    Value::Number {
                        value: f64::NAN,
                        reference: None
                    }
                } else {
                    Value::Integer { 
                        value: if inner.signum() > 0.0 { 1 } else { -1 },
                        reference: None 
                    }
                }
            }
            Self::Round(inner) => {
                let Some(inner) = inner.eval(context)?.into_f64("ROUND")? else { return Ok(Value::new_null()); };
                if inner.is_nan() {
                    Value::Number {
                        value: f64::NAN,
                        reference: None
                    }
                } else {
                    Value::Integer { 
                        value: inner.round() as i64,
                        reference: None 
                    }
                }
            }
            Self::Floor(inner) => {
                let Some(inner) = inner.eval(context)?.into_f64("ROUND")? else { return Ok(Value::new_null()); };
                if inner.is_nan() {
                    Value::Number {
                        value: f64::NAN,
                        reference: None
                    }
                } else {
                    Value::Integer { 
                        value: inner.floor() as i64,
                        reference: None 
                    }
                }
            }
            Self::Ceiling(inner) => {
                let Some(inner) = inner.eval(context)?.into_f64("CEIL")? else { return Ok(Value::new_null()); };
                if inner.is_nan() {
                    Value::Number {
                        value: f64::NAN,
                        reference: None
                    }
                } else {
                    Value::Integer { 
                        value: inner.ceil() as i64,
                        reference: None 
                    }
                }
            }
            Self::Add(lhs, rhs) => {
                let lhs = lhs.eval(context)?;
                let rhs = rhs.eval(context)?;
                if let Value::Integer { value: lhs, .. } = lhs {
                    if let Value::Integer { value: rhs, .. } = rhs {
                        return Ok(Value::Integer { value: lhs + rhs, reference: None });
                    }
                }
                let Some(lhs) = lhs.into_f64("ADD, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.into_f64("ADD, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs + rhs, 
                    reference: None 
                }
            }
            Self::Sub(lhs, rhs) => {
                let lhs = lhs.eval(context)?;
                let rhs = rhs.eval(context)?;
                if let Value::Integer { value: lhs, .. } = lhs {
                    if let Value::Integer { value: rhs, .. } = rhs {
                        return Ok(Value::Integer { value: lhs - rhs, reference: None });
                    }
                }
                let Some(lhs) = lhs.into_f64("SUB, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.into_f64("SUB, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs - rhs, 
                    reference: None 
                }
            }
            Self::Mul(lhs, rhs) => {
                let lhs = lhs.eval(context)?;
                let rhs = rhs.eval(context)?;
                if let Value::Integer { value: lhs, .. } = lhs {
                    if let Value::Integer { value: rhs, .. } = rhs {
                        return Ok(Value::Integer { value: lhs * rhs, reference: None });
                    }
                }
                let Some(lhs) = lhs.into_f64("MUL, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.into_f64("MUL, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs * rhs, 
                    reference: None 
                }
            }
            Self::Div(lhs, rhs) => {
                let Some(lhs) = lhs.eval(context)?.into_f64("DIV, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.eval(context)?.into_f64("DIV, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs / rhs, 
                    reference: None 
                }
            }
            Self::Mod(lhs, rhs) => {
                let lhs = lhs.eval(context)?;
                let rhs = rhs.eval(context)?;
                if let Value::Integer { value: lhs, .. } = lhs {
                    if let Value::Integer { value: rhs, .. } = rhs {
                        return Ok(Value::Integer { value: lhs % rhs, reference: None });
                    }
                }
                let Some(lhs) = lhs.into_f64("MOD, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.into_f64("MOD, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs % rhs, 
                    reference: None 
                }
            }
            Self::Pow(lhs, rhs) => {
                let lhs = lhs.eval(context)?;
                let rhs = rhs.eval(context)?;
                if let Value::Integer { value: lhs, .. } = lhs {
                    if let Value::Integer { value: rhs, .. } = rhs {
                        if let Ok(rhs) = rhs.try_into() {
                            return Ok(Value::Integer { value: lhs.pow(rhs), reference: None });
                        }
                    }
                }
                let Some(lhs) = lhs.into_f64("MOD, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.into_f64("MOD, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Number { 
                    value: lhs.powf(rhs), 
                    reference: None 
                }
            }

            Self::Concat(lhs, rhs) => {
                let Some(lhs) = lhs.eval(context)?.into_text("CONCAT, argument lhs")? else { return Ok(Value::new_null()); };
                let Some(rhs) = rhs.eval(context)?.into_text("CONCAT, argument rhs")? else { return Ok(Value::new_null()); };
                Value::Text { 
                    value: TextValue {
                        text: lhs.text + &rhs.text,
                        format: TableCellTextContentFormat::Plain
                    }, 
                    reference: None 
                }
            }
            Self::Upper(inner) => {
                let Some(inner) = inner.eval(context)?.into_text("UPPER")? else { return Ok(Value::new_null()); };
                Value::Text { 
                    value: TextValue { 
                        text: inner.text.to_uppercase(), 
                        format: inner.format
                    }, 
                    reference: None 
                }
            }
            Self::Lower(inner) => {
                let Some(inner) = inner.eval(context)?.into_text("LOWER")? else { return Ok(Value::new_null()); };
                Value::Text { 
                    value: TextValue { 
                        text: inner.text.to_lowercase(), 
                        format: inner.format
                    }, 
                    reference: None 
                }
            }
            Self::StringLength(inner) => {
                let Some(inner) = inner.eval(context)?.into_text("LENGTH")? else { return Ok(Value::new_null()); };
                Value::Integer { 
                    value: inner.text.len() as i64, 
                    reference: None 
                }
            }

            Self::FileName(inner) => {
                let Some(inner) = inner.eval(context)?.into_file("NAME")? else { return Ok(Value::new_null()); };
                Value::Text { 
                    value: TextValue { 
                        text: inner.name().clone(), 
                        format: TableCellTextContentFormat::Plain 
                    },
                    reference: None 
                }
            }
            Self::FileSize(inner) => {
                let Some(inner) = inner.eval(context)?.into_file("SIZE")? else { return Ok(Value::new_null()); };
                Value::Integer { 
                    value: inner.get_size()?, 
                    reference: None 
                }
            }

            Self::If(a, b1, b2) => {
                (if a.eval(context)?.into_bool("IF, argument condition")? { b1 } else { b2 }).eval(context)?
            }
            Self::NullIf(a, b) => {

            }
            Self::Coalesce(items) => {

            }

            Self::Average(list) => {

            }
            Self::Count(list) => {

            }
            Self::Join { list, delimiter } => {

            }
            Self::Max(list) => {

            }
            Self::Min(list) => {

            }
            Self::Sum(list) => {
                
            }
        })
    }
}