use base64::{Engine, prelude::{BASE64_STANDARD as base64standard}};
use rusqlite::types::Value;
use rusqlite::vtab::array::Array;
use rusqlite::{Connection, Params, Transaction, params};
use serde::{Deserialize, Serialize};
use std::cell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use crate::data::query::QueryBuilder;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::{column, column_type, datasource, query, schema, table};

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct FailedValidation {
    message: String
}

#[derive(Deserialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum RetrievalLimit {
    Page {
        num: i64,
        size: i64     
    },
    SingleRow
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CellOid {
    schema_oid: i64,
    row_oid: i64,
    column_oid: i64
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Cell {
    Row {
        schema_oid: i64,
        row_oid: Option<i64>,
        index: i64,
        validation_failures: Vec<FailedValidation>
    },
    Readonly {
        cell_oid: CellOid,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    Subreport {
        cell_oid: CellOid,
        schema_query_string: String,
        validation_failures: Vec<FailedValidation>
    },
    PrimitiveEntry {
        cell_oid: CellOid,
        value_oid: CellOid,
        label: Option<String>,
        primitive_type: column_type::Primitive,
        validation_failures: Vec<FailedValidation>
    },
    Object {
        cell_oid: CellOid,
        value_oid: CellOid,
        object_schema_oid: i64,
        object_row_oid: Option<i64>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    SelectEntry {
        cell_oid: CellOid,
        value_oid: CellOid,
        select_schema_oid: i64,
        select_row_oid: Option<i64>,
        validation_failures: Vec<FailedValidation>
    },
    MultiselectEntry {
        cell_oid: CellOid,
        value_oid: CellOid,
        multiselect_schema_oid: i64,
        multiselect_row_oid: Vec<i64>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    }
}



impl Cell {
    /// Retrieves the value of a cell.
    pub fn get(value_oid: CellOid) -> Result<Self, Error> {
        let column_metadata: column::FullMetadata = column::FullMetadata::get(value_oid.column_oid)?;
        match column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let conn = db::open()?;
                let sql_select: String = match prim {
                    column_type::Primitive::Text
                    | column_type::Primitive::JSON => 
                        format!("SELECT COLUMN{} FROM TABLE{} WHERE OID = ?1", value_oid.column_oid, value_oid.schema_oid),
                    column_type::Primitive::Integer
                    | column_type::Primitive::Number
                    | column_type::Primitive::Checkbox => 
                        format!("SELECT CAST(COLUMN{} AS TEXT) FROM TABLE{} WHERE OID = ?1", value_oid.column_oid, value_oid.schema_oid),
                    column_type::Primitive::Date => 
                        format!("SELECT DATE(COLUMN{}, 'julianday') FROM TABLE{} WHERE OID = ?1", value_oid.column_oid, value_oid.schema_oid),
                    column_type::Primitive::Datetime =>
                        format!("SELECT STRFTIME('%FT%TZ', COLUMN{}, 'julianday') FROM TABLE{} WHERE OID = ?1", value_oid.column_oid, value_oid.schema_oid),
                    column_type::Primitive::File
                    | column_type::Primitive::Image => {
                        let column_name: String = format!("COLUMN{}", value_oid.column_oid);
                        format!(
                            "
                            SELECT 
                                CASE 
                                    WHEN {column_name} IS NULL THEN NULL
                                    WHEN LENGTH({column_name}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({column_name}) * 0.000000001)
                                    WHEN LENGTH({column_name}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({column_name}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({column_name}) * 0.001)
                                END 
                            FROM TABLE{} 
                            WHERE OID = ?1
                            ",
                            value_oid.schema_oid
                        )
                    }
                };
                let label: Option<String> = conn.query_one(&sql_select, params![value_oid.row_oid], |row| row.get(0))?;
                Ok(Self::PrimitiveEntry { 
                    cell_oid: value_oid.clone(), 
                    value_oid, 
                    label, 
                    primitive_type: prim, 
                    validation_failures: Vec::new() 
                })
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                let conn = db::open()?;

                let column_name: String = format!("COLUMN{}", value_oid.column_oid);
                let table_name: String = format!("TABLE{}", value_oid.schema_oid);
                let sql_select: String = format!(
                    "
                    SELECT 
                        t.{column_name} AS OBJECT_ROW_OID, 
                        obj_surr.LABEL
                    FROM {table_name} t
                    INNER JOIN TABLE{table_oid}_SURROGATE obj_surr ON obj_surr.OID = t.{column_name}
                    WHERE t.OID = ?1
                    "
                );
                let (object_row_oid, label) = conn.query_one(
                    &sql_select, 
                    params![value_oid.row_oid], 
                    |row| Ok::<(Option<i64>, Option<String>), rusqlite::Error>((row.get("OBJECT_ROW_OID")?, row.get("LABEL")?))
                )?;

                Ok(Self::Object { 
                    cell_oid: value_oid.clone(), 
                    value_oid, 
                    object_schema_oid: table_oid, 
                    object_row_oid, 
                    label, 
                    validation_failures: Vec::new() 
                })
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                let conn = db::open()?;

                let sql_select: String = format!(
                    "
                    SELECT 
                        COLUMN{} AS SELECT_ROW_OID
                    FROM TABLE{}
                    WHERE t.OID = ?1
                    ", 
                    value_oid.column_oid, 
                    value_oid.schema_oid
                );
                let select_row_oid: Option<i64> = conn.query_one(
                    &sql_select, 
                    params![value_oid.row_oid], 
                    |row| row.get("SELECT_ROW_OID")
                )?;

                Ok(Self::SelectEntry { 
                    cell_oid: value_oid.clone(), 
                    value_oid, 
                    select_schema_oid: table_oid, 
                    select_row_oid, 
                    validation_failures: Vec::new() 
                })
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                let conn = db::open()?;

                let multiselect_name: String = format!("MULTISELECT{}", value_oid.column_oid);
                let table_name: String = format!("TABLE{}", value_oid.schema_oid);
                let sql_select: String = format!(
                    "
                    SELECT 
                        GROUP_CONCAT(CAST(m.TABLE{table_oid}_OID AS TEXT)) AS VALUE, 
                        '[' || GROUP_CONCAT(a.JSON_STRINGIFY) || ']' AS LABEL
                    FROM {multiselect_name} m
                    INNER JOIN TABLE{table_oid}_SURROGATE a ON a.OID = m.TABLE{table_oid}_OID
                    WHERE m.{table_name}_OID = ?1
                    "
                );
                let (multiselect_row_oid_str, label) = conn.query_one(
                    &sql_select, 
                    params![value_oid.row_oid], 
                    |row| Ok::<(Option<String>, Option<String>), rusqlite::Error>((row.get("VALUE")?, row.get("LABEL")?))
                )?;
                let multiselect_row_oid: Vec<i64> = match multiselect_row_oid_str {
                    Some(s) => s.split(',').filter_map(|i| match i.parse::<i64>() { Ok(i) => Some(i), Err(_) => None }).collect(),
                    None => Vec::new()
                };

                Ok(Self::MultiselectEntry { 
                    cell_oid: value_oid.clone(), 
                    value_oid, 
                    multiselect_schema_oid: table_oid, 
                    multiselect_row_oid, 
                    label, 
                    validation_failures: Vec::new() 
                })
            }
            column_type::ColumnType::Formula { .. }
            | column_type::ColumnType::Subreport { .. } => {
                todo!("These branches shouldn't really be necessary, based on the places that this program calls Cell::get from.")
            }
        }
    }

    /// Gets the OIDs pointing to the value of the cell.
    pub fn get_value_oid(&self) -> Result<CellOid, Error> {
        match self {
            Self::PrimitiveEntry { value_oid, .. }
            | Self::Object { value_oid, .. }
            | Self::SelectEntry { value_oid, .. }
            | Self::MultiselectEntry { value_oid, .. } => Ok(value_oid.clone()),
            Self::Readonly { .. } => Err(Error::AdhocError("A readonly cell does not read from a value.")),
            Self::Subreport { .. } => Err(Error::AdhocError("A subreport cell does not read from a value.")),
            Self::Row { .. } => Err(Error::AdhocError("A row does not read from a value."))
        }
    }

    /// Recursively build mapping from schema to default datasource by traversing up the inheritance hierarchy.
    fn build_schema_to_datasource_mapping(trans: &Transaction, schema_to_datasource: &mut HashMap<schema::FullMetadata, datasource::Datasource>, table_metadata: table::FullMetadata) -> Result<(), Error> {
        for master_schema in table_metadata.schema.master_schemas.iter() {
            if let schema::Schema::Table(master_table) = master_schema {
                if !schema_to_datasource.contains_key(&master_table.schema) {
                    let datasource: datasource::Datasource = datasource::Datasource::Inheritance { 
                        oid: 0, 
                        parent_datasource: Box::new(schema_to_datasource[&table_metadata.schema].clone()), 
                        table: master_table.clone() 
                    }.find(trans, Vec::new())?;
                    schema_to_datasource.insert(master_table.schema.clone(), datasource);
                }
                Self::build_schema_to_datasource_mapping(trans, schema_to_datasource, master_table.clone())?;
            }
        }
        Ok(())
    }

    /// Builds a basic query to get all columns associated with the given schema.
    /// Also sends the column information through the provided Sender object.
    fn build_query(mut column_sender: Sender<column::FullMetadata>, schema_oid: i64, initial_datasources: Vec<datasource::Datasource>) -> Result<query::QueryBuilder, Error> {
        // Construct mapping from schema to default datasource
        let mut schema_to_datasource: HashMap<schema::FullMetadata, datasource::Datasource> = HashMap::new();
        {
            let mut conn = db::open()?;
            let trans = conn.transaction()?;

            for datasource in initial_datasources.iter() {
                schema_to_datasource.insert(datasource.get_schema(), datasource.clone());

                // Make sure all master tables of a root table are also included as a datasource
                if let datasource::Datasource::Table { table, .. } = datasource {
                    Self::build_schema_to_datasource_mapping(&trans, &mut schema_to_datasource, table.clone())?;
                }
            }

            trans.commit()?;
        }
        
        // Build query to get data for each column in the schema
        let mut query: query::QueryBuilder = query::QueryBuilder::new(initial_datasources);
        column::FullMetadata::query_by_schema(
            Sender::Callback(Box::new(|col: column::FullMetadata| -> Result<(), Error> {
                // Add column to query
                let datasource: datasource::Datasource = schema_to_datasource[&col.schema].clone();
                query.insert_column(datasource, col.clone())?;

                // Send column metadata over the provided Sender object
                column_sender.send(col)?;
                Ok(())
            })), 
            schema_oid
        )?;

        let conn: Connection = db::open()?;

        // Filter rows in the query based on the METADATA_REPORT_FILTER table
        let mut stmt_filter = conn.prepare("SELECT FORMULA FROM METADATA_REPORT_FILTER WHERE REPORT_OID = ?1 AND TRASH = 0")?;
        for row_result in stmt_filter.query_and_then(params![schema_oid], |row| row.get::<_, String>("FORMULA"))? {
            let filter_formula = row_result?;
            // Insert WHERE clause
            query.insert_filter(filter_formula)?;
        }

        // Group rows in the query based on the METADATA_REPORT_GROUPBY table
        let mut stmt_groupby = conn.prepare("SELECT COLUMN_OID FROM METADATA_REPORT_GROUPBY WHERE REPORT_OID = ?1 AND TRASH = 0")?;
        for row_result in stmt_groupby.query_and_then(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
            let column_oid = row_result?;
            // Insert GROUP BY clause
            query.insert_grouping(column_oid)?;
        }

        // Order the query based on the METADATA_SCHEMA_ORDERBY table
        let mut stmt_orderby = conn.prepare("SELECT COLUMN_OID, SORT_ASCENDING FROM METADATA_SCHEMA_ORDERBY WHERE SCHEMA_OID = ?1 AND TRASH = 0 ORDER BY ORDERING")?;
        for row_result in stmt_orderby.query_and_then(params![schema_oid], |row| { Ok::<(i64, bool), rusqlite::Error>((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)) })? {
            let (column_oid, sort_ascending) = row_result?;
            // Insert ORDER BY clause
            query.insert_ordering(column_oid, sort_ascending)?;
        }

        Ok(query)
    }

    fn run_query(mut cell_sender: Sender<Self>, schema_oid: i64, query: query::QueryBuilder, filters: Vec<(String, i64)>, limit: RetrievalLimit) -> Result<(), Error> {
        // Compile and run the query
        let conn: Connection = db::open()?;
        if let Some((cmd_query, cols, datasource_aliases)) = query.compile()? {

            // Add extra filters and row limits
            let cmd_query: String = if filters.len() > 0 {
                format!(
                    "SELECT * FROM ({cmd_query}) WHERE {}",
                    filters.into_iter().map(|(table_or_subquery_alias, table_or_subquery_row_oid)| format!("{table_or_subquery_alias}_OID = {table_or_subquery_row_oid}"))
                        .reduce(|acc, e| format!("{acc} AND {e}"))
                        .unwrap()
                )
            } else {
                cmd_query
            };
            let cmd_query: String = match limit {
                RetrievalLimit::SingleRow => format!("{cmd_query} LIMIT 1"),
                RetrievalLimit::Page { num, size } => format!("{cmd_query} LIMIT {size} OFFSET {}", size * (num - 1))
            };

            // Run the query
            let mut stmt_query = conn.prepare(&cmd_query)?;
            let mut rows_query = stmt_query.query([])?;
            loop {
                let Some(row) = rows_query.next()? else { return Ok(()); };

                // First, send a header for the row
                cell_sender.send(Cell::Row { 
                    schema_oid, 
                    row_oid: match row.get::<_, i64>("OID") {
                        Ok(o) => Some(o),
                        Err(rusqlite::Error::InvalidColumnName(_)) => None,
                        Err(e) => {
                            return Err(e.into());
                        }
                    }, 
                    index: row.get("ROW_INDEX")?, 
                    validation_failures: Vec::new() 
                })?;
                
                // Then, send a cell for each column
                for c in cols.iter() {
                    match c {
                        query::QueryBuilderColumn::Primitive { schema_oid, schema_row_ord, column_oid, label_ord, primitive_type, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let value_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::PrimitiveEntry { 
                                cell_oid: value_oid.clone(), 
                                value_oid,
                                label, 
                                primitive_type: primitive_type.clone(),
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Object { schema_oid, schema_row_ord, column_oid, label_ord, object_schema_oid, object_row_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let value_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::Object { 
                                cell_oid: value_oid.clone(), 
                                value_oid,
                                object_schema_oid: object_schema_oid.clone(),
                                object_row_oid: row.get::<&str, Option<i64>>(object_row_ord)?,
                                label, 
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Select { schema_oid, schema_row_ord, column_oid, select_schema_oid, select_row_ord, .. } => {
                            let value_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::SelectEntry { 
                                cell_oid: value_oid.clone(), 
                                value_oid,
                                select_schema_oid: select_schema_oid.clone(),
                                select_row_oid: row.get::<&str, Option<i64>>(select_row_ord)?,
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Multiselect { schema_oid, schema_row_ord, column_oid, label_ord, select_schema_oid, select_row_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let value_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };

                            let multiselect_row_oid: Vec<i64> = match row.get::<&str, Option<String>>(select_row_ord)? {
                                Some(s) => s.split(',').filter_map(|n| match n.parse::<i64>() { Ok(num) => Some(num), Err(_) => None }).collect(),
                                None => Vec::new()
                            };

                            cell_sender.send(Cell::MultiselectEntry { 
                                cell_oid: value_oid.clone(), 
                                value_oid,
                                multiselect_schema_oid: select_schema_oid.clone(),
                                multiselect_row_oid,
                                label,
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Formula { schema_oid, schema_row_ord, column_oid, param_ord, label_ord, value_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let cell_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };

                            if let Some(param) = row.get::<&str, Option<String>>(param_ord)? {
                                let Some((value_datasource_oid_str, value_column_oid_str)) = param.split_once(':') else {
                                    return Err(Error::AdhocError("Formula returned nonempty parameter, but in nonstandard format."));
                                };
                                let Ok(value_datasource_oid) = value_datasource_oid_str.parse::<i64>() else {
                                    return Err(Error::AdhocError("Formula returned datasource OID that was not an integer."));
                                };
                                let value_datasource = datasource::Datasource::get(value_datasource_oid)?;
                                let Ok(value_column_oid) = value_column_oid_str.parse::<i64>() else {
                                    return Err(Error::AdhocError("Formula returned column OID that was not an integer."));
                                };
                                let value_column = column::FullMetadata::get(value_column_oid)?;
                                
                                let value_row_ord: String = format!("d{}_OID", value_datasource.get_oid());
                                let value_oid: CellOid = CellOid {
                                    schema_oid: value_datasource.get_schema().oid,
                                    row_oid: row.get::<&str, i64>(&value_row_ord)?,
                                    column_oid: value_column.oid.clone()
                                };

                                cell_sender.send(match value_column.column_type {
                                    column_type::ColumnType::Primitive(primitive_type) => {
                                        Cell::PrimitiveEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            label, 
                                            primitive_type,
                                            validation_failures: Vec::new() 
                                        }
                                    }
                                    column_type::ColumnType::Object { table_oid, .. } => {
                                        Cell::Object { 
                                            cell_oid, 
                                            value_oid, 
                                            object_schema_oid: table_oid.clone(), 
                                            object_row_oid: row.get::<&str, Option<i64>>(value_ord)?, 
                                            label, 
                                            validation_failures: Vec::new() 
                                        }
                                    }
                                    column_type::ColumnType::Select { table_oid, .. } => {
                                        Cell::SelectEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            select_schema_oid: table_oid.clone(), 
                                            select_row_oid: row.get::<&str, Option<i64>>(value_ord)?, 
                                            validation_failures: Vec::new() 
                                        }
                                    }
                                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                                        let multiselect_row_oid: Vec<i64> = match row.get::<&str, Option<String>>(value_ord)? {
                                            Some(s) => s.split(',').filter_map(|n| match n.parse::<i64>() { Ok(num) => Some(num), Err(_) => None }).collect(),
                                            None => Vec::new()
                                        };

                                        Cell::MultiselectEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            multiselect_schema_oid: if value_datasource.get_schema().oid == value_column.schema.oid {
                                                // If the multiselect column belongs to the schema of the datasource, do not invert
                                                table_oid.clone() 
                                            } else {
                                                // If the multiselect column does not belong to the schema of the datasource, 
                                                // then this multiselect is inverted and pointing back at the schema holding the multiselect column
                                                value_column.schema.oid
                                            }, 
                                            multiselect_row_oid, 
                                            label, 
                                            validation_failures: Vec::new() 
                                        }
                                    }
                                    _ => {
                                        return Err(Error::AdhocError("Formula returned an invalid column."));
                                    }
                                })?;
                            } else {
                                // If the value of the cell is not directly linked to the value of another cell, send as a readonly value
                                cell_sender.send(Cell::Readonly { 
                                    cell_oid, 
                                    label, 
                                    validation_failures: Vec::new() 
                                });
                            }
                        }
                        query::QueryBuilderColumn::Subreport { schema_oid, schema_row_ord, column_oid, subreport_schema_oid } => {
                            let cell_oid: CellOid = CellOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row.get::<&str, i64>(schema_row_ord)?, 
                                column_oid: column_oid.clone()
                            };

                            cell_sender.send(Cell::Subreport { 
                                cell_oid, 
                                schema_query_string: {
                                    // Compile the query string for the subreport

                                    // First key is "schema_oid", which determines the schema that's pulled up when the subreport is opened
                                    let mut qstr: String = format!("schema_oid={subreport_schema_oid}");

                                    // Then add a key for each non-null datasource
                                    for datasource_alias in datasource_aliases.iter() {
                                        let datasource_row_alias: String = format!("{datasource_alias}_OID");
                                        if let Some(datasource_row) = row.get::<&str, Option<i64>>(&datasource_row_alias)? {
                                            qstr = format!("{qstr}&{datasource_alias}={datasource_row}")
                                        }
                                    }
                                    qstr
                                }, 
                                validation_failures: Vec::new() 
                            })?;
                        }
                    }
                }
            }
        } else {
            return Ok(()); // If the report doesn't have any datasources, just don't run it
        }
    }

    /// Sends all cells on a page in a schema.
    pub fn query_by_schema(column_sender: Sender<column::FullMetadata>, cell_sender: Sender<Self>, schema_oid: i64, filters: Vec<(String, i64)>, limit: RetrievalLimit) -> Result<(), Error> {
        let schema: schema::Schema = schema::Schema::get(schema_oid)?;

        // Build the base query and retrieve columns
        let query = Self::build_query(column_sender, schema_oid, 
            match schema {
                schema::Schema::Table(table_metadata) => {
                    // Find the default datasource for the table
                    let mut conn = db::open()?;
                    let trans = conn.transaction()?;
                    let table_datasource: datasource::Datasource = datasource::Datasource::Table { 
                        oid: 0, 
                        label: table_metadata.schema.name.clone(), 
                        table: table_metadata
                    }.find(&trans, Vec::new())?;
                    trans.commit()?;
                    vec![table_datasource]
                }
                schema::Schema::Report(_) => Vec::new() // Reports have no default datasource
            }
        )?;

        // Compile and run the query
        Self::run_query(cell_sender, schema_oid, query, filters, limit)?;
        Ok(())
    }

    /// Overwrites the previous value of the cell.
    pub fn set(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        match self {
            Self::PrimitiveEntry { value_oid, label, primitive_type, .. } => {
                match primitive_type {
                    column_type::Primitive::File
                    | column_type::Primitive::Image => {
                        // Do nothing for BLOB types
                        // BLOB content will need to be uploaded separately
                    }
                    _ => {
                        let sql_update: String = format!("UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2", value_oid.schema_oid, value_oid.column_oid);
                        trans.execute(&sql_update, params![label, value_oid.row_oid])?;
                    }
                }
            }
            Self::Object { value_oid, object_schema_oid, object_row_oid, .. } => {
                // Trash the previous object
                let sql_trash_previous: String = format!(
                    "UPDATE TABLE{} AS o SET o.TRASH = TRUE FROM (SELECT COLUMN{} AS O_OID FROM TABLE{} WHERE OID = ?1) AS t WHERE o.OID = t.O_OID",
                    object_schema_oid,
                    value_oid.column_oid,
                    value_oid.schema_oid
                );
                trans.execute(&sql_trash_previous, params![value_oid.row_oid])?;

                match object_row_oid {
                    Some(_) => {
                        // Create a new Object row
                        let object_table: table::FullMetadata = table::FullMetadata::get(object_schema_oid.clone())?;
                        let object_row_oid: i64 = object_table.insert_row(None)?;

                        // Assign the Object row to the cell's value
                        let sql_update: String = format!("UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2", value_oid.schema_oid, value_oid.column_oid);
                        trans.execute(&sql_update, params![object_row_oid, value_oid.row_oid])?;
                    }
                    _ => {
                        // Set the cell's value to NULL
                        let sql_update: String = format!("UPDATE TABLE{} SET COLUMN{} = NULL WHERE OID = ?1", value_oid.schema_oid, value_oid.column_oid);
                        trans.execute(&sql_update, params![value_oid.row_oid])?;
                    }
                }
            }
            Self::SelectEntry { value_oid, select_row_oid, .. } => {
                let sql_update: String = format!("UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2", value_oid.schema_oid, value_oid.column_oid);
                trans.execute(&sql_update, params![select_row_oid, value_oid.row_oid])?;
            }
            Self::MultiselectEntry { value_oid, multiselect_schema_oid, multiselect_row_oid, .. } => {
                // Delete the rows selected in the database that were deselected
                let sql_delete: String = format!(
                    "DELETE FROM MULTISELECT{} WHERE TABLE{}_OID = ?1 AND TABLE{}_OID NOT IN ?2",
                    value_oid.column_oid,
                    value_oid.schema_oid,
                    multiselect_schema_oid
                );
                trans.execute(&sql_delete, 
                    params![
                        value_oid.row_oid, 
                        Array::new(multiselect_row_oid.iter().map(|i| Value::Integer(i.clone())).collect())
                    ]
                )?;

                // Insert the selected rows
                let sql_insert: String = format!(
                    "INSERT OR IGNORE INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) VALUES (?1, ?2)",
                    value_oid.column_oid,
                    value_oid.schema_oid,
                    multiselect_schema_oid
                );
                for selected_oid in multiselect_row_oid.iter() {
                    trans.execute(&sql_insert, params![value_oid.row_oid, selected_oid]);
                }
            }
            _ => {
                // All other types of columns cannot be updated, so ignore
            }
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}



#[derive(Deserialize)]
pub struct Blob {
    pub blob_oid: CellOid
}

impl Blob {
    pub fn into_base64(self) -> Result<String, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Construct a BLOB IO object
        let table_name: String = format!("TABLE{}", self.blob_oid.schema_oid);
        let column_name: String = format!("COLUMN{}", self.blob_oid.column_oid);
        let blob = trans.blob_open("main", &*table_name, &*column_name, self.blob_oid.row_oid, true)?;

        // Read the BLOB into a buffer
        let mut buf_reader = BufReader::new(blob);
        let mut buf: Vec<u8> = Vec::new();
        match buf_reader.read_to_end(&mut buf) {
            Ok(_) => {},
            Err(_) => {
                return Err(Error::AdhocError("Unable to read stored file."));
            }
        }

        // Encode in base64
        return Ok(base64standard.encode(&buf));
    }

    /// Downloads data from the BLOB to the filepath.
    pub fn download(self, filepath: String) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Load the file from the filesystem
        let mut file = match File::create(filepath) {
            Ok(f) => f,
            Err(_) => {
                return Err(Error::AdhocError("Unable to open file."));
            }
        };

        // Construct a BLOB IO object
        let table_name: String = format!("TABLE{}", self.blob_oid.schema_oid);
        let column_name: String = format!("COLUMN{}", self.blob_oid.column_oid);
        let blob = trans.blob_open("main", &*table_name, &*column_name, self.blob_oid.row_oid, true)?;

        // Read the BLOB into a buffer
        let mut buf_reader = BufReader::new(blob);
        let mut buf: Vec<u8> = Vec::new();
        match buf_reader.read_to_end(&mut buf) {
            Ok(_) => {},
            Err(_) => {
                return Err(Error::AdhocError("Unable to read stored file."));
            }
        }

        // Write the contents of the buffer into the file
        match file.write_all(&buf) {
            Ok(_) => {},
            Err(_) => {
                return Err(Error::AdhocError("Unable to write to file."));
            }
        }

        return Ok(());
    }

    /// Uploads data from the filepath to the BLOB.
    pub fn upload(self, filepath: String) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Load the file from the filesystem
        let buf = match std::fs::read(filepath) {
            Ok(read_buf) => read_buf,
            Err(_) => {
                return Err(Error::AdhocError("Unable to open file."));
            }
        };
        let cropped_file_len: i64 = match i64::try_from(buf.len()) {
            Ok(len) => len,
            Err(_) => {
                return Err(Error::AdhocError("File size is greater than 9,223,372,036,854,775,807 bytes."));
            }
        };

        // Update the value with an empty blob
        let update_cmd = format!("UPDATE TABLE{} SET COLUMN{} = ZEROBLOB(?1) WHERE OID = ?2;", self.blob_oid.schema_oid, self.blob_oid.column_oid);
        trans.execute(&update_cmd, params![cropped_file_len, self.blob_oid.row_oid])?;

        // Fill the empty blob with the data from the file
        {
            let table_name: String = format!("TABLE{}", self.blob_oid.schema_oid);
            let column_name: String = format!("COLUMN{}", self.blob_oid.column_oid);
            let mut blob = trans.blob_open("main", &*table_name, &*column_name, self.blob_oid.row_oid, false)?;
            match blob.write_all(&buf) {
                Ok(_) => {},
                Err(_) => {
                    return Err(Error::AdhocError("Unable to upload file contents to database."));
                }
            }
        }

        // Commit the transaction
        trans.commit()?;
        return Ok(());
    }
}