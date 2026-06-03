use crate::data::{column, column_type};
use crate::data::datasource::Datasource;
use crate::util::db;
use crate::util::error::Error;
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::collections::HashMap;
use std::path::Path;
use std::fs::{File as FilesystemFile};
use std::io::{BufReader, Read, Write};
use serde_json::{Map, Value, json};

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
fn get_columns<'a, 'b>(conn: &'a Connection, columns_by_schema: &'b mut HashMap<i64, Vec<column::FullMetadata>>, schema_oid: &i64) -> Result<&'b Vec<column::FullMetadata>, Error> {
    if columns_by_schema.contains_key(schema_oid) {
        return Ok(columns_by_schema.get(schema_oid).unwrap());
    }

    let mut cols: Vec<column::FullMetadata> = Vec::new();
    for row_result in conn.prepare("SELECT COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE IS_REQUIRED AND SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
        let column_oid: i64 = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(conn, column_oid)?;

        // Add to the list of columns
        cols.push(column_metadata);
    }
    columns_by_schema.insert(schema_oid.clone(), cols);
    return Ok(columns_by_schema.get(schema_oid).unwrap());
}

fn construct_row_object(conn: &Connection, row: &Row<'_>, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, schema_oid: &i64, index_column: &Option<String>, oid_column: &Option<String>) -> Result<Value, Error> {
    let cols = get_columns(conn, columns_by_schema, schema_oid)?.clone();
    let mut map: Map<String, Value> = Map::new();

    // Add the row's index, if requested
    if let Some(index_column_name) = index_column {
        map.insert(index_column_name.clone(), json!(row.get::<_, i64>("ROW_INDEX")?));
    }

    // Add the row's OID, if requested
    if let Some(oid_column_name) = oid_column {
        map.insert(oid_column_name.clone(), match row.get::<_, Option<i64>>("OID")? {
            Some(oid) => json!(oid),
            None => Value::Null
        });
    }

    // Add each column to the map
    for c in cols.iter() {
        // Ensure the column name isn't duplicated
        if map.contains_key(&c.name) {
            return Err(Error::DuplicateColumnName { column_name: c.name.clone() });
        }

        // Insert the column into the map
        map.insert(c.name.clone(), match &c.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match prim {
                    column_type::Primitive::Integer => match row.get::<&str, Option<i64>>(&value_ord)? {
                        Some(value) => json!(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Number => match row.get::<&str, Option<f64>>(&value_ord)? {
                        Some(value) => json!(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Text => match row.get::<&str, Option<String>>(&value_ord)? {
                        Some(value) => json!(value),
                        None => Value::Null
                    },
                    column_type::Primitive::JSON => match row.get::<&str, Option<String>>(&value_ord)? {
                        Some(value) => {
                            todo!("Parse the JSON string into a Value")
                        },
                        None => Value::Null
                    },
                    column_type::Primitive::File
                    | column_type::Primitive::Image => match row.get::<&str, Option<i64>>(&value_ord)? {
                        Some(value) => {
                            todo!("Get the file content as a base64 string")
                        },
                        None => Value::Null
                    },
                    column_type::Primitive::Checkbox => match row.get::<&str, Option<bool>>(&value_ord)? {
                        Some(value) => json!(value),
                        None => Value::Null
                    },
                    column_type::Primitive::Date
                    | column_type::Primitive::Datetime => {
                        let label_ord: String = format!("COLUMN{}_LABEL", c.oid);
                        match row.get::<&str, Option<String>>(&label_ord)? {
                            Some(value) => json!(value),
                            None => Value::Null
                        }
                    }
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<i64>>(&value_ord)? {
                    Some(row_oid) => {
                        let obj_conn = db::open()?;
                        export_object_row(&obj_conn, columns_by_schema, &table_oid, &row_oid, &None, &None, &Some(String::from("$type")))?
                    }
                    None => Value::Null
                }
            }
            column_type::ColumnType::Select { .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<i64>>(&value_ord)? {
                    Some(value) => json!(value),
                    None => Value::Null
                }
            }
            column_type::ColumnType::Multiselect { .. } => {
                let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                match row.get::<&str, Option<String>>(&value_ord)? {
                    Some(value) => Value::Array(
                        value.split(',').filter_map(|s| match i64::from_str_radix(s, 10) {
                            Ok(i) => Some(json!(i)),
                            Err(_) => None
                        }).collect()
                    ),
                    None => Value::Null
                }
            }
            column_type::ColumnType::Formula { .. } => {
                let param_ord: String = format!("COLUMN{}_PARAM", c.oid);
                match row.get::<&str, Option<String>>(&param_ord)? {
                    Some(param) => {
                        if param.starts_with("boolean") {
                            let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                            let value: String = row.get::<&str, String>(&value_ord)?;
                            match i64::from_str_radix(&value, 10) {
                                Ok(i) => json!(i != 0),
                                Err(_) => {
                                    return Err(Error::AdhocError("Expected a boolean value."));
                                }
                            }
                        } else if param.starts_with("integer") {
                            let value_ord: String = format!("COLUMN{}_VALUE", c.oid);
                            let value: String = row.get::<&str, String>(&value_ord)?;
                            match i64::from_str_radix(&value, 10) {
                                Ok(i) => json!(i),
                                Err(_) => {
                                    return Err(Error::AdhocError("Expected a boolean value."));
                                }
                            }
                        } else {
                            return Err(Error::AdhocError("Unknown formula return type."));
                        }
                    },
                    None => Value::Null 
                }
            }
            column_type::ColumnType::Subreport { report_oid, .. } => { // Export the rows of the report, filtered
                let report_conn: Connection = db::open()?;

                // Query for the rows of the schema
                let select_sql: String = format!(
                    "SELECT * FROM SCHEMA{report_oid}_VIEW {} ORDER BY ROW_INDEX",
                    {
                        if let Some(query_filter) = row.get::<_, Option<String>>("QUERY_FILTER")? {
                            let report_view_def: String = report_conn.query_one("SELECT sql FROM sqlite_schema WHERE tbl_name = ?1", params![format!("SCHEMA{report_oid}_VIEW")], |row| row.get::<_, String>("sql"))?;
                            let mut filters: Vec<String> = Vec::new();
                            for (filtered_column_name, filtered_column_oid) in query_filter.split('&').filter_map(|s| s.split_once('=')) {
                                // Ensure that the filtered column name belongs to the report view
                                if report_view_def.contains(&format!(" AS {filtered_column_name}")) {
                                    filters.push(format!("{filtered_column_name} = {filtered_column_oid}"));
                                }
                            }
                            match filters.into_iter().reduce(|acc, e| format!("{acc} AND {e}")) {
                                Some(combined_filters) => format!("WHERE {combined_filters}"),
                                None => String::from("")
                            }
                        } else {
                            String::from("")
                        }
                    }
                );
                let mut select_stmt = conn.prepare(&select_sql)?;
                let mut select_rows = select_stmt.query([])?;

                // Build the JSON array
                let mut array_rows: Vec<Value> = Vec::new();
                loop {
                    // Start building the object for the next row of the schema
                    let Some(row) = select_rows.next()? else { break; };

                    // Add the object to the array of the schema rows
                    array_rows.push(construct_row_object(conn, row, columns_by_schema, &schema_oid, &index_column, &oid_column)?);
                }
                json!(array_rows)
            }
        });
    }

    Ok(json!(map))
}

/// Exports a single row of a table, accounting for additional columns of tables inheriting from this one.
fn export_object_row(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, table_oid: &i64, row_oid: &i64, index_column: &Option<String>, oid_column: &Option<String>, type_column: &Option<String>) -> Result<Value, Error> {
    let select_polymorphism_sql: String = format!("SELECT t.TABLE_OID, s.TABLE_NAME, t.ROW_OID FROM TABLE{table_oid}_POLYMORPHISM_VIEW t INNER JOIN METADATA_SCHEMA s ON s.OID = t.TABLE_OID WHERE t.OID = ?1");
    Ok(if let Some((table_oid, table_name, row_oid)) = conn.query_one(&select_polymorphism_sql, params![row_oid], |row| Ok((row.get::<_, i64>("TABLE_OID")?, row.get::<_, String>("TABLE_NAME")?, row.get::<_, i64>("ROW_OID")?))).optional()? {
        // Query the row from the table
        let select_sql: String = format!("SELECT * FROM SCHEMA{table_oid}_VIEW WHERE OID = ?1");
        let mut select_stmt = conn.prepare(&select_sql)?;
        let mut select_rows = select_stmt.query(params![row_oid])?;
        if let Some(row) = select_rows.next()? {
            let mut row_value: Value = construct_row_object(conn, row, columns_by_schema, &table_oid, index_column, oid_column)?;
            if let Some(type_column_name) = type_column {
                if let Value::Object(ref mut row_object_map) = row_value {
                    row_object_map.insert(type_column_name.clone(), json!(table_name));
                }
            }
            row_value 
        } else {
            return Err(Error::AdhocError("Expected to find a row that does not exist."));
        }
    } else {
        Value::Null 
    })
}

/// Exports the rows of the schema individually, accounting for column/type polymorphism.
fn export_schema_individual(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, table_oid: i64, index_column: Option<String>, oid_column: Option<String>, type_column: Option<String>) -> Result<Value, Error> {
    let mut array_rows: Vec<Value> = Vec::new();

    for row_result in conn.prepare("SELECT OID FROM TABLE{schema_oid} WHERE NOT TRASH")?.query_map([], |row| row.get::<_, i64>("OID"))? {
        let row_oid = row_result?;
        array_rows.push(export_object_row(conn, columns_by_schema, &table_oid, &row_oid, &index_column, &oid_column, &type_column)?);
    }
    Ok(json!(array_rows))
}

/// Exports the rows of the schema in a batch, not accounting for polymorphism.
fn export_schema_batch(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, schema_oid: i64, index_column: Option<String>, oid_column: Option<String>) -> Result<Value, Error> {
    // Query for the rows of the schema
    let select_sql: String = format!("SELECT * FROM SCHEMA{schema_oid}_VIEW ORDER BY ROW_INDEX");
    let mut select_stmt = conn.prepare(&select_sql)?;
    let mut select_rows = select_stmt.query([])?;

    // Build the JSON array
    let mut array_rows: Vec<Value> = Vec::new();
    loop {
        // Start building the object for the next row of the schema
        let Some(row) = select_rows.next()? else { break; };

        // Add the object to the array of the schema rows
        array_rows.push(construct_row_object(conn, row, columns_by_schema, &schema_oid, &index_column, &oid_column)?);
    }
    Ok(json!(array_rows))
}

fn export_schema(conn: &Connection, columns_by_schema: &mut HashMap<i64, Vec<column::FullMetadata>>, schema: ExportSchema) -> Result<(String, Value), Error> {
    match schema {
        ExportSchema::Table { schema_oid, schema_name, polymorphism, index_column, oid_column } => {
            match polymorphism {
                ExportPolymorphism::No => {
                    // Export rows of table in batch
                    Ok((schema_name, export_schema_batch(conn, columns_by_schema, schema_oid, index_column, oid_column)?))
                }
                ExportPolymorphism::Yes { type_column } => {
                    // Export rows of table individually
                    Ok((schema_name, export_schema_individual(conn, columns_by_schema, schema_oid, index_column, oid_column, type_column)?))
                }
            }
        }
        ExportSchema::Report { schema_oid, schema_name, index_column } => {
            // Export rows of report in batch
            Ok((schema_name, export_schema_batch(conn, columns_by_schema, schema_oid, index_column, None)?))
        }
    }
}

/// Exports schemas in JSON format.
pub fn export(filepath: String, schemas: Vec<ExportSchema>) -> Result<(), Error> {
    // Construct the top-level map
    let conn = db::open()?;
    let mut columns_by_schema: HashMap<i64, Vec<column::FullMetadata>> = HashMap::new();
    let mut map: Map<String, Value> = Map::new();
    for schema in schemas.into_iter() {
        let (schema_name, schema_rows) = export_schema(&conn, &mut columns_by_schema, schema)?;
        map.insert(schema_name, schema_rows);
    }

    // Serialize into JSON document
    let json: String = json!(map).to_string();

    // Create or open the file for writing
    let mut file = match FilesystemFile::create(filepath) {
        Ok(f) => f,
        Err(_) => {
            return Err(Error::AdhocError("Unable to open file."));
        }
    };

    // Write the contents of the JSON document into the file
    match file.write_all(json.as_bytes()) {
        Ok(_) => {},
        Err(_) => {
            return Err(Error::AdhocError("Unable to write to file."));
        }
    }
    Ok(())
}