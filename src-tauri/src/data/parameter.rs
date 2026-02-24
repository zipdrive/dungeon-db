use crate::util::error::Error;
use crate::data::datasource;
use rusqlite::{Transaction, params};

/// Creates a new parameter.
pub fn create(trans: &Transaction) -> Result<i64, Error> {
    // Create datasource
    let parameter_oid: i64 = datasource::create(trans)?;
    // Create parameter
    trans.execute("INSERT INTO METADATA_PARAMETER (OID) VALUES (?1)", params![parameter_oid])?;
    // Return parameter OID
    Ok(parameter_oid)
}