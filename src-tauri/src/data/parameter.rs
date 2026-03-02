use crate::util::error::Error;
use crate::data::{datasource, column};
use rusqlite::{Transaction};

#[derive(PartialEq, Eq, Hash, Clone)]
pub struct Parameter {
    pub datasource: datasource::Datasource,
    pub column: column::Metadata
}

impl Parameter {
    
}

/// Creates a new parameter.
pub fn create(trans: &Transaction) -> Result<i64, Error> {
    // Create parameter
    trans.execute("INSERT INTO METADATA_PARAMETER DEFAULT VALUES", [])?;
    let parameter_oid = trans.last_insert_rowid();
    // Return parameter OID
    Ok(parameter_oid)
}