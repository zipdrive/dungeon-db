use crate::util::error::Error;
use rusqlite::Transaction;

/// Creates a new datasource.
pub fn create(trans: &Transaction) -> Result<i64, Error> {
    // Create datasource
    trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
    Ok(trans.last_insert_rowid())
}