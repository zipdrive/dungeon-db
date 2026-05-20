use crate::data::column;
use crate::data::datasource::Datasource;
use crate::util::error::Error;
use rusqlite::{Connection, params};
use std::collections::HashMap;
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
    let mut top_level_array: Vec<Value> = Vec::new();

    // Records the list of columns for each schema
    let mut columns_by_schema: HashMap<i64, Vec<(Datasource, column::FullMetadata)>> = HashMap::new();

    for row_result in conn.prepare("SELECT OID, SCHEMA_OID, ROW_OID FROM TABLE{schema_oid}_POLYMORPHISM_VIEW")?.query_map([], |row| Ok((row.get::<_, i64>("OID")?, row.get::<_, i64>("SCHEMA_OID")?, row.get::<_, i64>("ROW_OID")?)))? {
        let (oid, schema_oid, row_oid) = row_result?;
        
        // Get the columns of the row's schema
        let schema_columns = if columns_by_schema.contains_key(&schema_oid) {
            &columns_by_schema[&schema_oid]
        } else {
            let root_datasource: Datasource = Datasource::get_default_datasource_transact(conn, schema_oid)?;
            let root_datasource_alias: String = root_datasource.get_alias();

            let mut columns: Vec<(Datasource, column::FullMetadata)> = Vec::new();
            for column_result in conn.prepare("SELECT DATASOURCE_PATH, COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED")?.query_map(params![schema_oid], |row| Ok((row.get::<_, String>("DATASOURCE_PATH")?, row.get::<_, i64>("COLUMN_OID")?)))? {
                let (column_datasource_path, column_oid) = column_result?;
                let column_datasource: Datasource = Datasource::from_alias_transact(conn, format!("{root_datasource_alias}{column_datasource_path}"))?;
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(conn, column_oid)?;
                columns.push((column_datasource, column_metadata));
            }
            columns_by_schema.insert(schema_oid, columns);
            &columns_by_schema[&schema_oid]
        };

        // Run query for that particular row in its schema
    }
    Ok(Value::Array(top_level_array))
}

/*

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

    */