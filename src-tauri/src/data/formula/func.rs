use std::collections::HashMap;

use rusqlite::Connection;

use crate::data::formula::value::{Value, TextValue, RecordValue};
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::row::TableCellTextContentFormat;
use crate::util::error::Error;

pub struct Context<'a> {
    pub conn: &'a Connection,
    pub table_restrictions: HashMap<i64, Vec<i64>>
}
pub struct ContextRestriction {
    /// OID filters on the table.
    pub oid_filters: Option<Vec<i64>>,

    /// Restrictions on a column of the table.
    pub column_restrictions: HashMap<TableColumnMetadata, ContextRestriction>
}

pub enum Func {
    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Text(String),

    Not(Box<Func>),
    And(Box<Func>, Box<Func>),
    Or(Box<Func>, Box<Func>),

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
}

impl Func {
    fn eval(&self, context: &Context) -> Result<Value, Error> {
        Ok(match self {
            Self::Null => Value::Null {
                reference: None
            },
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
        })
    }
}