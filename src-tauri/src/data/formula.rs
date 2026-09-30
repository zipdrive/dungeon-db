use crate::util::error::Error;

pub mod query;
pub mod context;
pub mod func;
pub mod value;

impl func::Func {
    /// Parse a formula.
    pub fn parse(formula: String) -> Result<Self, Error> {
        
    }
}