use crate::util::error::Error;
use rusqlite::Connection;
use std::path::Path;
use std::fs::{File as FilesystemFile};
use std::io::{BufReader, Read, Write};
use serde_json::{Map, Value};

enum ExportPolymorphism {
    No,
    Yes {
        type_column: Option<String>
    }
}

enum ExportSchema {
    Table {
        schema_oid: i64,
        schema_name: String,
        polymorphism: ExportPolymorphism,
        index_column: Option<String>,
        oid_column: Option<String>
    },
    Report {
        schema_oid: i64,
        schema_name: String,
        index_column: Option<String>
    }
}

/// Exports a row of a table, accounting for additional columns of tables inheriting from this one.
fn export_object_row(conn: &Connection, table_oid: i64, row_oid: i64) -> Result<Value, Error> {
    Err(Error::AdhocError(""))
}

/// Exports the rows of the schema individually, accounting for column/type polymorphism.
fn export_schema_individual(conn: &Connection, schema_oid: i64, index_column: Option<String>, oid_column: Option<String>, type_column: Option<String>) -> Result<Value, Error> {
    for row_result in conn.prepare("SELECT OID, SCHEMA_OID, ROW_OID FROM TABLE{schema_oid}_LABEL_VIEW")
}

/// Exports the rows of the schema in a batch, not accounting for polymorphism.
fn export_schema_batch(conn: &Connection, schema_oid: i64, index_column: Option<String>, oid_column: Option<String>) -> Result<Value, Error> {
    let mut top_level_array: Vec<Value> = Vec::new();
    let mut current_row: Option<Map> = None;
    
    // Run the schema query
    let mut column_mapping: HashMap<i64, column::FullMetadata> = HashMap::new();
    cell::Cell::query_by_schema(
        Sender::Callback(Box::new(
            |column| {
                column_mapping.insert(column.oid, column);
                Ok(())
            }
        )),
        Sender::Callback(Box::new(
            |cell| {
                match cell {
                    cell::Cell::Row { row_identifier, index, .. } => {
                        if let Some(current_row) = current_row {
                            top_level_array.push(Value::Object(current_row));
                        }

                        let mut map: Map = Map::new();
                        if let Some(index_column_name) = index_column {
                            map.insert(index_column_name, Value::I64(index));
                        }
                        if let Some(oid_column_name) = oid_column {
                            if let Some((row_schema_oid, row_oid)) = row_identifier {
                                if row_schema_oid == schema_oid {
                                    map.insert(oid_column_name, Value::I64(row_oid));
                                } else {
                                    map.insert(oid_column_name, Value::Null);
                                }
                            } else {
                                map.insert(oid_column_name, Value::Null);
                            }
                        }
                        current_row = Some(map);
                    }
                    cell::Cell::PrimitiveEntry { cell_oid, label, .. } => {

                    }
                }
                Ok(())
            }
        )),
        schema_oid,
        Vec::new(),
        cell::RetrievalLimit::None
    )?;

    Ok(Value::Array(top_level_array))
}

fn export_schema(conn: &Connection, schema: ExportSchema) -> Result<(String, Value), Error> {
    match schema {
        ExportSchema::Table { schema_oid, schema_name, polymorphism, index_column, oid_column } => {
            match polymorphism {
                ExportPolymorphism::No => {
                    // Export rows of table in batch
                    Ok((schema_name, export_schema_batch(schema_oid, index_column, oid_column)?))
                }
                ExportPolymorphism::Yes { type_column } => {
                    // Export rows of table individually
                    Ok((schema_name, export_schema_individual(schema_oid, index_column, oid_column, type_column)?))
                }
            }
        }
        ExportSchema::Report { schema_oid, schema_name, index_column } => {
            // Export rows of report in batch
            Ok((schema_name, export_schema_batch(schema_oid, index_column, None)?))
        }
    }
}

/// Exports tables in JSON format.
pub fn export(conn: &Connection, filepath: String, schemas: Vec<ExportSchema>) -> Result<(), Error> {
    // Construct the top-level map
    let mut top_level_map: Map = Map::new();
    for schema in schemas.into_iter() {
        let (schema_name, schema_rows) = export_schema(conn, schema)?;
        top_level_map.insert(schema_name, schema_rows);
    }

    // Serialize JSON and export to file
}