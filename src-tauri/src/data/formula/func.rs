use std::collections::HashMap;

use crate::{data::table::column::TableColumnMetadata, util::error::Error};

pub struct Context {
    pub table_restrictions: HashMap<i64, Vec<i64>>
}
pub struct ContextRestriction {
    /// OID filters on the table.
    pub oid_filters: Option<Vec<i64>>,

    /// Restrictions on a column of the table.
    pub column_restrictions: HashMap<TableColumnMetadata, ContextRestriction>
}

pub enum Func {
    Cell {
        path: Vec<String>
    },

    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Text(String),

    Not(Box<Func>),
    And(Box<Func>, Box<Func>),
    Or(Box<Func>, Box<Func>),

    Add(Box<Func>, Box<Func>),
    Sub(Box<Func>, Box<Func>),
    Mul(Box<Func>, Box<Func>),
    Div(Box<Func>, Box<Func>),
    Mod(Box<Func>, Box<Func>),
    Pow(Box<Func>, Box<Func>),
}

impl Func {
    /// Evaluates a boolean value.
    fn eval_bool(&self, context: &Context) -> Result<bool, Error> {
        Ok(match self {
            Self::Not(inner) => {
                !inner.eval_bool(context)?      
            }
            Self::And(lhs, rhs) => {
                lhs.eval_bool(context)? && rhs.eval_bool(context)?
            }
            Self::Or(lhs, rhs) => {
                lhs.eval_bool(context)? || rhs.eval_bool(context)?
            }
            _ => {
                return Err(Error::adhoc(format!("Unable to evaluate as bool.")));
            }
        })
    }

    /// Evaluates an integer value.
    fn eval_i64(&self, context: &Context) -> Result<i64, Error> {

    }

    /// Evaluates a floating-point value
    fn eval_f64(&self, context: &Context) -> Result<f64, Error> {
        
    }
}