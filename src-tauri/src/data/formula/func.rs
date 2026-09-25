use std::collections::HashMap;

use crate::data::table::column::TableColumnMetadata;
use crate::util::error::Error;

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

    Concat(Box<Func>, Box<Func>),
}

impl Func {
    /// Evaluates a boolean value.
    fn eval_bool(&self, context: &Context) -> Result<bool, Error> {
        Ok(match self {
            Self::Boolean(value) => value.clone(),
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
    fn eval_i64(&self, context: &Context) -> Result<Option<i64>, Error> {
        Ok(match self {
            Self::Null => None,
            Self::Integer(value) => Some(value.clone()),
            Self::Add(lhs, rhs) => {
                match lhs.eval_i64(context)? {
                    Some(lhs_eval) => match rhs.eval_i64(context)? {
                        Some(rhs_eval) => Some(lhs_eval + rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Sub(lhs, rhs) => {
                match lhs.eval_i64(context)? {
                    Some(lhs_eval) => match rhs.eval_i64(context)? {
                        Some(rhs_eval) => Some(lhs_eval - rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Mul(lhs, rhs) => {
                match lhs.eval_i64(context)? {
                    Some(lhs_eval) => match rhs.eval_i64(context)? {
                        Some(rhs_eval) => Some(lhs_eval * rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Mod(lhs, rhs) => {
                match lhs.eval_i64(context)? {
                    Some(lhs_eval) => match rhs.eval_i64(context)? {
                        Some(rhs_eval) => Some(lhs_eval % rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Pow(lhs, rhs) => {
                match lhs.eval_i64(context)? {
                    Some(lhs_eval) => match rhs.eval_i64(context)? {
                        Some(rhs_eval) => Some(match u32::try_from(rhs_eval) {
                            Ok(rhs_eval) => lhs_eval.pow(rhs_eval),
                            Err(_) => {
                                return Err(Error::adhoc(format!("Unable to evaluate as int.")));        
                            }
                        }),
                        None => None 
                    }
                    None => None 
                }
            }
            _ => {
                return Err(Error::adhoc(format!("Unable to evaluate as int.")));
            }
        })
    }

    /// Evaluates a floating-point value
    fn eval_f64(&self, context: &Context) -> Result<Option<f64>, Error> {
        Ok(match self {
            Self::Integer(value) => Some(value.clone() as f64),
            Self::Number(value) => Some(value.clone()),
            Self::Add(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval + rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Sub(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval - rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Mul(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval * rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Div(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval / rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Mod(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval % rhs_eval),
                        None => None 
                    }
                    None => None 
                }
            }
            Self::Pow(lhs, rhs) => {
                match lhs.eval_f64(context)? {
                    Some(lhs_eval) => match rhs.eval_f64(context)? {
                        Some(rhs_eval) => Some(lhs_eval.powf(rhs_eval)),
                        None => None 
                    }
                    None => None 
                }
            }
            _ => {
                return Err(Error::adhoc(format!("Unable to evaluate as number.")));
            }
        })
    }

    /// Evaluates a text value.
    fn eval_text(&self, context: &Context) -> Result<String, Error> {
        Ok(match self {
            Self::Text(value) => value.clone(),
            Self::Concat(lhs, rhs) => {
                let rhs_value: String = rhs.eval_text(context)?;
                lhs.eval_text(context)? + &rhs_value
            }
            _ => {
                return Err(Error::adhoc(format!("Unable to evaluate as text.")));
            }
        })
    }
}