use crate::data::report::column::ReportColumnMetadata;
use crate::data::report::row::ReportRow;
use crate::data::table::{self, TableMetadata};
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::row::{TableRow, TableCell, TableCellContent, TableCellTextContentFormat};
use crate::util::channel;
use crate::util::db::{sql_zero_or_one, sql_iter, sql_map_then_iter, sql_one};
use crate::util::db;
use crate::util::error::Error;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs::File as FilesystemFile;
use std::io::{BufReader, Read, Write};
use std::path::Path;

#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum ExportPolymorphism {
    No,
    Yes
}

#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum ExportSchema {
    Table {
        table_oid: i64,
        table_name: String,
        polymorphism: ExportPolymorphism,
        index_column: Option<String>,
        oid_column: Option<String>,
    },
    Report {
        report_oid: i64,
        report_name: String,
        index_column: Option<String>
    }
}

impl ExportSchema {
    /// Gets the name of the schema.
    fn name(&self) -> String {
        match self {
            Self::Table { table_name: name, .. }
            | Self::Report { report_name: name, .. } => name.clone()
        }
    }
}

/// Construct a Value corresponding to a TableCellContent object.
fn construct_value(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    content: TableCellContent
) -> Result<Value, Error> {
    Ok(match content {
        TableCellContent::Boolean { value } => json!(value),
        TableCellContent::Date { label, .. }
        | TableCellContent::Datetime { label, .. } => json!(label),
        TableCellContent::File { value } => match value {
            Some(file) => json!(file.oid().clone()),
            None => json!(None::<Value>)
        },
        TableCellContent::Integer { value }
        | TableCellContent::SingleSelectDropdown { value, .. } => json!(value),
        TableCellContent::MultiSelectDropdown { value, .. } => json!(value),
        TableCellContent::Number { value } => json!(value),
        TableCellContent::Object { table_oid, value } => match value {
            Some(row_oid) => {
                export_object_row_json_object(
                    conn, 
                    columns_by_table, 
                    columns_by_report,
                    type_column,
                    &table_oid, 
                    row_oid, 
                    &None,
                    &None
                )?
            }
            None => json!(None::<Value>)
        },
        TableCellContent::Subreport { report_oid, oid_filters } => {
            // Export the rows of the subreport
            export_report(
                conn, 
                columns_by_table, 
                columns_by_report, 
                type_column,
                report_oid, 
                oid_filters, 
                None
            )?
        }
        TableCellContent::Text { value, format } => {
            match format {
                TableCellTextContentFormat::Json => match value {
                    Some(value) => {
                        match serde_json::from_str(&value) {
                            Ok(json_value) => json_value,
                            Err(_) => {
                                return Err(Error::InvalidJson { text: value });
                            }
                        }
                    }
                    None => json!(None::<Value>)
                }
                _ => json!(value)
            }
        }
    })
}

// Construct a JSON object corresponding to the table row.
fn construct_table_row_json_object(
    conn: &Connection,
    row: TableRow,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    table_oid: &i64,
    index_column: &Option<String>,
    oid_column: &Option<String>,
    custom_columns: Vec<(String, Value)>
) -> Result<Value, Error> {
    let cols = columns_by_table.get(table_oid).unwrap().clone();
    let mut map: Map<String, Value> = Map::new();

    // Add the row's index, if requested
    if let Some(index_column_name) = index_column {
        map.insert(
            index_column_name.clone(),
            json!(row.index)
        );
    }

    // Add the row's OID, if requested
    if let Some(oid_column_name) = oid_column {
        map.insert(
            oid_column_name.clone(),
            json!(row.oid)
        );
    }

    // Add each column to the map
    for cell in row.cells {
        // Get the column name
        let column_name: String = {
            let Some((_, column_metadata)) = cols.iter().find(|(_, col)| col.oid == cell.column_oid) else {
                return Err(Error::MissingMetadata);
            };
            column_metadata.name.clone()
        };

        // Ensure that the column name isn't duplicated
        if map.contains_key(&column_name) {
            return Err(Error::DuplicateColumnName {
                column_name
            });
        }

        // Insert value for cell into map
        map.insert(column_name, construct_value(conn, columns_by_table, columns_by_report, type_column, cell.content)?);
    }

    // Add any custom columns
    for (key, value) in custom_columns {
        map.insert(key, value);
    }

    // Return the serialized map
    Ok(json!(map))
}

// Construct a JSON object corresponding to the report row.
fn construct_report_row_json_object(
    conn: &Connection,
    row: ReportRow,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    report_oid: &i64,
    index_column: &Option<String>
) -> Result<Value, Error> {
    let cols = columns_by_report.get(report_oid).unwrap().clone();
    let mut map: Map<String, Value> = Map::new();

    // Add the row's index, if requested
    if let Some(index_column_name) = index_column {
        map.insert(
            index_column_name.clone(),
            json!(row.index)
        );
    }

    // Add each column to the map
    for cell in row.cells {
        // Get the column name
        let column_name: String = {
            let Some(column_metadata) = cols.iter().find(|col| col.oid == cell.column_oid) else {
                return Err(Error::MissingMetadata);
            };
            column_metadata.name.clone()
        };

        // Ensure that the column name isn't duplicated
        if map.contains_key(&column_name) {
            return Err(Error::DuplicateColumnName {
                column_name
            });
        }

        // Insert value for cell into map
        map.insert(column_name, construct_value(conn, columns_by_table, columns_by_report, type_column, cell.content)?);
    }

    // Return the serialized map
    Ok(json!(map))
}

/// Exports a single row of a table, accounting for additional columns of tables inheriting from this one.
fn export_object_row_json_object(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    table_oid: &i64,
    row_oid: i64,
    index_column: &Option<String>,
    oid_column: &Option<String>,
) -> Result<Value, Error> {
    let (table_oid, column_tuples, row) = TableRow::conn_get_object(conn, table_oid.clone(), row_oid)?;

    // Add columns to storage
    if !columns_by_table.contains_key(&table_oid) {
        columns_by_table.insert(
            table_oid.clone(), 
            column_tuples
        );
    }

    // Construct a JSON object for the polymorphic row
    construct_table_row_json_object(
        conn, 
        row, 
        columns_by_table, 
        columns_by_report,
        type_column,
        &table_oid, 
        index_column, 
        oid_column,
        match type_column {
            Some(type_column) => {
                let table_metadata: TableMetadata = TableMetadata::conn_get(conn, table_oid.clone())?;
                vec![(type_column.clone(), json!(table_metadata.name))]
            }
            None => Vec::new()
        }
    )
}

/// Exports the rows of the schema individually, accounting for column/type polymorphism.
fn export_table_by_row(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    table_oid: i64,
    index_column: Option<String>,
    oid_column: Option<String>
) -> Result<Value, Error> {
    let mut array_rows: Vec<Value> = Vec::new();

    // Iterate over all OIDs of the table
    sql_map_then_iter(
        conn,
        format!("SELECT OID FROM TABLE{table_oid}"),
        [],
        |row| row.get::<_, i64>("OID"),
        |row_oid| {
            // Export JSON object for each row, accounting for polymorphism
            array_rows.push(export_object_row_json_object(
                conn,
                columns_by_table,
                columns_by_report,
                type_column,
                &table_oid,
                row_oid,
                &index_column,
                &oid_column
            )?);
            Ok(None::<()>)
        }
    )?;
    Ok(json!(array_rows))
}

/// Exports the rows of the schema in a batch, not accounting for polymorphism.
fn export_table_batch(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    table_oid: i64,
    index_column: Option<String>,
    oid_column: Option<String>,
) -> Result<Value, Error> {
    let mut array_rows: Vec<Value> = Vec::new();

    // Query for columns of table
    let columns: Vec<(i64, TableColumnMetadata)> = match columns_by_table.get(&table_oid) {
        Some(columns) => columns.clone(),
        None => {
            let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all(conn, table_oid)?
                .into_iter()
                .map(|(owning_table_oid, _, column)| (owning_table_oid, column))
                .collect();
            columns_by_table.insert(table_oid, columns.clone());
            columns 
        }
    };

    // Query for the rows of the table
    TableRow::conn_send(
        conn,
        channel::Sender::Callback(Box::new(|row| {
            array_rows.push(
                construct_table_row_json_object(
                    conn, 
                    row, 
                    columns_by_table, 
                    columns_by_report,
                    type_column,
                    &table_oid, 
                    &index_column, 
                    &oid_column, 
                    Vec::new()
                )?
            );
            Ok(())
        })), 
        table_oid,
        columns
    )?;
    Ok(json!(array_rows))
}

/// Export a table according to the specified rules for handling polymorphism.
fn export_table(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    table_oid: i64,
    polymorphism: ExportPolymorphism,
    index_column: Option<String>,
    oid_column: Option<String>
) -> Result<Value, Error> {
    match polymorphism {
        ExportPolymorphism::No => {
            // Export rows of table in batch
            export_table_batch(
                conn,
                columns_by_table,
                columns_by_report,
                type_column,
                table_oid,
                index_column,
                oid_column,
            )
        }
        ExportPolymorphism::Yes => {
            // Export rows of table individually
            export_table_by_row(
                conn,
                columns_by_table,
                columns_by_report,
                type_column,
                table_oid,
                index_column,
                oid_column,
            )
        }
    }
}

fn export_report(
    conn: &Connection,
    columns_by_table: &mut HashMap<i64, Vec<(i64, TableColumnMetadata)>>,
    columns_by_report: &mut HashMap<i64, Vec<ReportColumnMetadata>>,
    type_column: &Option<String>,
    report_oid: i64,
    oid_filters: Vec<(String, Vec<i64>)>,
    index_column: Option<String>
) -> Result<Value, Error> {
    let mut array_rows: Vec<Value> = Vec::new();

    // Query for columns of table
    let columns: Vec<ReportColumnMetadata> = match columns_by_report.get(&report_oid) {
        Some(columns) => columns.clone(),
        None => {
            let columns: Vec<ReportColumnMetadata> = ReportColumnMetadata::conn_query_all(conn, report_oid)?;
            columns_by_report.insert(report_oid, columns.clone());
            columns 
        }
    };

    // Query for rows of the report
    ReportRow::conn_send(
        conn, 
        channel::Sender::Callback(Box::new(|row| {
            array_rows.push(
                construct_report_row_json_object(
                    conn, 
                    row, 
                    columns_by_table,
                    columns_by_report, 
                    type_column,
                    &report_oid, 
                    &index_column
                )?
            );
            Ok(())
        })), 
        report_oid, 
        oid_filters, 
        columns
    )?;
    Ok(json!(array_rows))
}

/// Exports schemas in JSON format.
pub fn export(mut channel: channel::Sender<(f64, String)>, filepath: String, schemas: Vec<ExportSchema>, type_column: Option<String>) -> Result<(), Error> {
    let progress_by_item: f64 = 1.0 / (schemas.len() as f64);
    let mut columns_by_table: HashMap<i64, Vec<(i64, TableColumnMetadata)>> = HashMap::new();
    let mut columns_by_report: HashMap<i64, Vec<ReportColumnMetadata>> = HashMap::new();

    // Construct the top-level map
    let conn = db::open()?;
    let mut map: Map<String, Value> = Map::new();
    let mut cur_progress: f64 = 0.0;
    for schema in schemas.into_iter() {
        // Send progress report
        let schema_name: String = schema.name();
        channel.send((cur_progress, format!("Serializing {schema_name}...")))?;

        // Do the serialization
        let schema_rows = match schema {
            ExportSchema::Table { table_oid, polymorphism, index_column, oid_column, .. } => {
                export_table(&conn, &mut columns_by_table, &mut columns_by_report, &type_column, table_oid, polymorphism, index_column, oid_column)?
            }
            ExportSchema::Report { report_oid, index_column, .. } => {
                export_report(&conn, &mut columns_by_table, &mut columns_by_report, &type_column, report_oid, Vec::new(), index_column)?
            }
        };
        map.insert(schema_name, schema_rows);

        // Update progress percentage
        cur_progress += progress_by_item;
    }

    // Serialize into JSON document
    channel.send((cur_progress, format!("Saving to file...")))?;
    let json: String = json!(map).to_string();

    // Create or open the file for writing
    let mut file = match FilesystemFile::create(filepath) {
        Ok(f) => f,
        Err(_) => {
            return Err(Error::adhoc("Unable to open file."));
        }
    };

    // Write the contents of the JSON document into the file
    match file.write_all(json.as_bytes()) {
        Ok(_) => {}
        Err(_) => {
            return Err(Error::adhoc("Unable to write to file."));
        }
    }
    Ok(())
}
