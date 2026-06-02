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

/// Get the columns of a schema.
fn get_columns<'a, 'b>(conn: &'a Connection, columns_by_schema: &'b mut HashMap<i64, Vec<column::FullMetadata>>, schema_oid: i64) -> Result<&'b Vec<column::FullMetadata>, Error> {
    match columns_by_schema.get(&table_oid) {
        Some(cols) => cols,
        None => {
            let mut cols: Vec<column::FullMetadata> = Vec::new();
            for row_result in conn.prepare("SELECT COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE IS_REQUIRED AND SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
                let column_oid: i64 = row_result?;
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(conn, column_oid)?;

                // Add to the list of columns
                cols.push(column_metadata);
            }
            columns_by_schema.insert(table_oid, cols);
            columns_by_schema.get(&table_oid).unwrap()
        }
    }
}

fn construct_row_object() -> Result<Value, Error> {
    let mut map: Map = Map::new();

    // Add the row's index, if requested
    if let Some(index_column_name) = &index_column {
        map.insert(index_column_name.clone(), Value::I64(row.get::<_, i64>("ROW_INDEX")?));
    }

    // Add the row's OID, if requested
    if let Some(oid_column_name) = &oid_column {
        map.insert(oid_column_name.clone(), match row.get::<_, Option<i64>>("OID")? {
            Some(oid) => Value::I64(oid),
            None => Value::Null
        });
    }

    // Add each column to the map
    for c in cols.iter() {
        // Ensure the column name isn't duplicated
        if map.contains_key(&c.name) {
            return Err(Error::DuplicateColumnName { column_name: c.name });
        }

        // Insert the column into the map
        map.insert(c.name.clone(), match &c.column_type {
            &column_type::ColumnType::Primitive(prim) => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match prim {
                    column_type::Primitive::Integer => match row.get::<&str, Option<i64>>(&value_ord)? {
                        Some(value) => Value::I64(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Number => match row.get::<&str, Option<f64>>(&value_ord)? {
                        Some(value) => Value::F64(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Text => match row.get::<&str, Option<String>>(&value_ord)? {
                        Some(value) => Value::String(value),
                        None => Value::Null
                    },
                    column_type::Primitive::JSON => match row.get::<&str, Option<String>>(&value_ord)? {
                        Some(value) => {
                            // TODO parse the JSON
                        },
                        None => Value::Null
                    },
                    column_type::Primitive::File
                    | column_type::Primitive::Image => match row.get::<&str, Option<i64>>(&value_ord)? {
                        Some(value) => {
                            // TODO get the file content as base64 string
                        },
                        None => Value::Null
                    },
                    column_type::Primitive::Checkbox => match row.get::<&str, Option<bool>>(&value_ord)? {
                        Some(value) => Value::Bool(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Date
                    | column_type::Primitive::Datetime => {
                        let label_ord: String = format!("COLUMN{}_LABEL", c.oid);
                        match row.get::<&str, Option<String>>(&label_ord)? {
                            Some(value) => Value::I64(value),
                            None => Value::Null
                        }
                    }
                }
            }
            &column_type::ColumnType::Object { table_oid, .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<i64>>(&value_ord)? {
                    Some(row_oid) => {
                        let obj_conn = db::open()?;
                        export_object_row(&obj_conn, columns_by_schema, table_oid, row_oid)?
                    }
                    None => Value::Null
                }
            }
            &column_type::ColumnType::Select { .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<i64>>(&value_ord)? {
                    Some(value) => Value::I64(value),
                    None => Value::Null
                }
            }
            &column_type::ColumnType::Multiselect { .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<String>>(&value_ord)? {
                    Some(value) => Value::Array(
                        value.split(',').filter_map(|s| match i64::parse(s) {
                            Ok(i) => Some(i),
                            Err(_) => None
                        }).collect()
                    ),
                    None => Value::Null
                }
            }
            &column_type::ColumnType::Formula { .. } => {
                let param_ord: String = format!("COLUMN{}_PARAM", c.oid);
                match row.get::<&str, Option<String>>(&param_ord)? {
                    Some(param) => {
                        if param.starts_with("boolean") {
                            let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                            let value: String = row.get::<&str, String>(&value_ord)?;
                            match i64::from_str(&value) {
                                Ok(i) => Value::Bool(i != 0),
                                Err(_) => {
                                    return Err(Error::AdhocError("Expected a boolean value."));
                                }
                            }
                        } else if param.starts_with("integer") {
                            let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                            let value: String = row.get::<&str, String>(&value_ord)?;
                            match i64::from_str(&value) {
                                Ok(i) => Value::I64(i),
                                Err(_) => {
                                    return Err(Error::AdhocError("Expected a boolean value."));
                                }
                            }
                        }
                    },
                    None => Value::Null 
                }
            }
            &column_type::ColumnType::Subreport { report_oid, .. } => {

            }
        });
    }

    // Add the object to the array of the schema rows
    top_level_array.push(Value::Object(map));
}

/// Exports a row of a table, accounting for additional columns of tables inheriting from this one.
fn export_object_row(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, table_oid: i64, row_oid: i64) -> Result<Value, Error> {
    let select_polymorphism_sql: String = format!("SELECT TABLE_OID, ROW_OID FROM TABLE{table_oid}_POLYMORPHISM_VIEW WHERE OID = ?1", table_oid);
    Ok(if let Some((table_oid, row_oid)) = conn.query_one(&select_polymorphism_sql, params![row_oid], |row| Ok((row.get::<_, i64>("TABLE_OID")?, row.get::<_, i64>("ROW_OID")?))).optional()? {
        // Get the columns of the table
        let cols: &Vec<column::FullMetadata> = get_columns(&conn, columns_by_schema, table_oid)?;

        // Query the row from the table

    } else {
        Value::Null 
    })
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

/// Exports the rows of the schema in a batch, not accounting for polymorphism.
fn export_schema_batch(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, schema_oid: i64, index_column: Option<String>, oid_column: Option<String>) -> Result<Value, Error> {
    // Query the columns of the schema
    let cols: &Vec<column::FullMetadata> = get_columns(conn, columns_by_schema, schema_oid)?;

    // Query for the rows of the schema
    let select_sql: String = format!("SELECT * FROM SCHEMA{schema_oid}_VIEW ORDER BY ROW_INDEX");
    let select_stmt = conn.prepare(&select_sql)?;
    let select_rows = select_stmt.query([])?;

    // Build the JSON array
    let mut top_level_array: Vec<Value> = Vec::new();
    loop {
        // Start building the object for the next row of the schema
        let Some(row) = select_rows.next() else { break; };
        
    }
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