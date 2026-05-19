use crate util::error::Error;
use serde_json::{Map, Value};

/// Exports a row of a table, accounting for additional columns of tables inheriting from this one.
fn export_object_row(conn: &Connection, schema_oid: i64, row_oid: i64) -> Result<Value, Error> {
    
}

pub fn export()