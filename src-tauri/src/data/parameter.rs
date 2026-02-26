use crate::util::error::Error;
use rusqlite::{Transaction};

/// Creates a new parameter.
pub fn create(trans: &Transaction) -> Result<i64, Error> {
    // Create parameter
    trans.execute("INSERT INTO METADATA_PARAMETER DEFAULT VALUES", [])?;
    let parameter_oid = trans.last_insert_rowid();
    // Return parameter OID
    Ok(parameter_oid)
}