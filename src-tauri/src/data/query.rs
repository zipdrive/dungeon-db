use rusqlite::{Connection, Transaction, params};
use crate::data::{column, column_type, datasource, parameter, schema, table};
use crate::util::formula::Formula;
use crate::util::db;
use crate::util::error::Error;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::collections::{HashSet,HashMap};
use bitflags::bitflags;



!bitflags {
    struct ScalarType: u32 {
        const Null          = 0b00000000;
        const Any           = 0b11111111;
        const Boolean       = 0b00000001;
        const Integer       = 0b00000010;
        const Number        = 0b00000110;
        const Date          = 0b00001000;
        const Datetime      = 0b00011000;
        const Text          = 0b00100000;
        const JSON          = 0b01100000;
        const Blob          = 0b10000000;
    }
}

impl ScalarType {
    /// Converts from a scalar type to a string.
    fn to_string(&self) -> String {
        let flags = self.iter().collect();
        // Reduce flags to minimal set
        let mut k: usize = 0;
        while k < flags.len() {
            // Iterate over each other flag, testing if this flag is contained in the other
            let mut j: usize = 0;
            while j < flags.len() {
                if j != k && flags[j].contains(flags[k]) {
                    flags.remove(k.clone());
                    k -= 1; // Decrement to negate the increment
                    break;
                }
                // Increment the index being compared to
                j += 1;
            }

            // Increment index
            k += 1;
        }
        // Concatenate different types together
        flags.iter().map(|flag| match flag {
            Self::Null => String::from("null"),
            Self::Any => String::from("any"),
            Self::Boolean => String::from("boolean"),
            Self::Integer => String::from("integer"),
            Self::Number => String::from("number"),
            Self::Date => String::from("date"),
            Self::Datetime => String::from("timestamp"),
            Self::Text => String::from("text"),
            Self::JSON => String::from("JSON"),
            Self::Blob => String::from("file")
        }).reduce(|acc, e| format!("{acc} | {e}")).unwrap_or(String::from("null"))
    }
}

/// Represents an expression returning a scalar value.
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The SQL expression resulting in a scalar value representing the true value of the parameter.
    /// This will typically be the same as arg_expr, with the exception that Select/Multiselect/Object columns will have their primary keys 
    /// returned by arg_expr and their referenced row OIDs returned by value_expr.
    value_expr: String,

    /// The SQL expression for the label of that scalar value (e.g. primary key of the row referenced by a Select column).
    label_expr: String,

    /// The SQL expression for the parameter returned by the expression, if it returns the value of an unmodified parameter.
    param_expr: String,

    /// The scalar type returned by the expression.
    return_type: ScalarType
}



enum TableOrSubquery {
    Root(datasource::Datasource),
    Array {
        values: Vec<ScalarExpression>,
        alias: String
    },
    PrecompiledJoin {
        datasource: datasource::Datasource,
        join_clause: String
    }
}

impl Hash for TableOrSubquery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Root(datasource)
            | Self::Precompiled { datasource, .. } => {
                datasource.hash(state)
            }
        }
    }
}

impl Borrow<datasource::Datasource> for TableOrSubquery {
    fn borrow(&self) -> &datasource::Datasource {
        match self {
            Self::Root(datasource)
            | Self::Precompiled { datasource, .. } => {
                datasource
            }
        }
    }
}



pub struct QueryBuilder {

}