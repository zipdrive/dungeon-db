use rusqlite::{Connection, Params, Transaction, params};
use serde::{Deserialize, Serialize};
use std::cell;
use std::collections::HashMap;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::{column, column_type, datasource, parameter, query, schema, table};

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct FailedValidation {
    message: String
}

#[derive(Deserialize)]
#[serde(rename_all="camelCase")]
pub struct Page {
    num: i64,
    size: i64 
}

#[derive(Serialize, Clone)]
pub struct CellOid {
    schema_oid: i64,
    row_oid: i64,
    column_oid: i64
}

#[derive(Serialize, Clone)]
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


enum Column {
    Formula {
        
    },
    Multiselect {

    }
}


#[derive(Clone)]
enum Relationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
    }
}

impl Cell {
    /// Recursively build mapping from schema to default datasource by traversing up the inheritance hierarchy.
    fn build_schema_to_datasource_mapping(trans: &Transaction, schema_to_datasource: &mut HashMap<schema::Metadata, datasource::Datasource>, table_metadata: table::Metadata) -> Result<(), Error> {
        for master_table in table_metadata.master_tables.iter() {
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
        Ok(())
    }

    /// Builds a basic query to get all columns associated with the given schema.
    /// Also sends the column information through the provided Sender object.
    fn build_query(mut column_sender: Sender<column::Metadata>, schema_oid: i64, initial_datasources: Vec<datasource::Datasource>) -> Result<query::QueryBuilder, Error> {
        // Construct mapping from schema to default datasource
        let mut schema_to_datasource: HashMap<schema::Metadata, datasource::Datasource> = HashMap::new();
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
        column::Metadata::query_by_schema(
            Sender::Callback(Box::new(|col: column::Metadata| -> Result<(), Error> {
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

    fn run_query<P: Params>(mut cell_sender: Sender<Self>, schema_oid: i64, query: query::QueryBuilder, params: P, query_cols: Vec<query::QueryBuilderColumn>) -> Result<(), Error> {
        // Compile and run the query
        let conn: Connection = db::open()?;
        if let Some((cmd_query, cols)) = query.compile()? {
            let mut stmt_query = conn.prepare(&cmd_query)?;
            let mut rows_query = stmt_query.query(params)?;
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
                        query::QueryBuilderColumn::Primitive { schema_oid, schema_row_ord, column_oid, primitive_type, label_ord, label_expr, value_expr } => {

                        }
                    }
                }
            }
        } else {
            return Ok(()); // If the report doesn't have any datasources, just don't run it
        }
        
                match c {
                    query::QueryBuilderColumn::Primitive { column, label_ord, row_ord } => {
                        let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                        let value_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };
                        cell_sender.send(Cell::PrimitiveEntry { 
                            cell_oid: value_oid.clone(), 
                            value_oid, 
                            label, 
                            validation_failures: Vec::new()
                        })?;
                    }
                    query::QueryBuilderColumn::Object { column, value_ord, label_ord, row_ord } => {
                        let object_row_oid: Option<i64> = row.get::<&str, Option<i64>>(value_ord)?;
                        let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                        let value_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };

                        let object_schema_oid = if let column_type::ColumnType::Object { table_oid, .. } = &column.column_type { table_oid.clone() } else {
                            return Err(Error::AdhocError("Expected an Object column."));
                        };

                        cell_sender.send(Cell::Object { 
                            cell_oid: value_oid.clone(), 
                            value_oid, 
                            object_schema_oid, 
                            object_row_oid, 
                            label, 
                            validation_failures: Vec::new() 
                        })?;
                    }
                    query::QueryBuilderColumn::Select { column, value_ord, row_ord } => {
                        let select_row_oid: Option<i64> = row.get::<&str, Option<i64>>(value_ord)?;
                        let value_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };

                        let select_schema_oid = if let column_type::ColumnType::Select { table_oid, .. } = &column.column_type { table_oid.clone() } else {
                            return Err(Error::AdhocError("Expected a Select column."));
                        };

                        cell_sender.send(Cell::SelectEntry { 
                            cell_oid: value_oid.clone(), 
                            value_oid, 
                            select_schema_oid, 
                            select_row_oid, 
                            validation_failures: Vec::new() 
                        })?;
                    }
                    query::QueryBuilderColumn::Multiselect { column, value_ord, label_ord, row_ord } => {
                        let multiselect_row_oid: Vec<i64> = match row.get::<&str, Option<String>>(value_ord)? {
                            Some(value) => {
                                value.split(',').filter_map(|o| if let Ok(oid) = o.parse::<i64>() { Some(oid) } else { None }).collect()
                            }
                            None => Vec::new()
                        };

                        let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                        let value_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };

                        let multiselect_schema_oid = if let column_type::ColumnType::Multiselect { table_oid, .. } = &column.column_type { table_oid.clone() } else {
                            return Err(Error::AdhocError("Expected a Multiselect column."));
                        };

                        cell_sender.send(Cell::MultiselectEntry { 
                            cell_oid: value_oid.clone(), 
                            value_oid, 
                            multiselect_schema_oid, 
                            multiselect_row_oid, 
                            label, 
                            validation_failures: Vec::new() 
                        })?;
                    }
                    query::QueryBuilderColumn::Formula { column, value_ord, label_ord, param_ord, row_ord } => {
                        let cell_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };
                        let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;

                        match row.get::<&str, Option<String>>(param_ord)? {
                            Some(param_string) => {
                                // If the formula result is associated with a parameter, send as a cell for the associated column

                                // Param string is expected to be of the form {datasource_row_ord}:{column_oid}
                                let Some((datasource_oid_string, column_oid_string)) = param_string.split_once(':') else {
                                    return Err(Error::AdhocError("Expected nonempty parameter string to be of the form \"{datasource_oid}:{column_oid}\"."));
                                };
                                let Ok(datasource_oid) = datasource_oid_string.parse::<i64>() else {
                                    return Err(Error::AdhocError("Expected nonempty parameter string to be of the form \"{datasource_oid}:{column_oid}\"."));
                                };
                                let Ok(column_oid) = column_oid_string.parse::<i64>() else {
                                    return Err(Error::AdhocError("Expected nonempty parameter string to be of the form \"{datasource_oid}:{column_oid}\"."));
                                };

                                let datasource: datasource::Datasource = datasource::Datasource::get(datasource_oid)?;
                                let datasource_row_ord: String = format!("d{datasource_oid}_OID");
                                let value_oid: CellOid = CellOid {
                                    schema_oid: datasource.get_schema().oid,
                                    row_oid: row.get::<&str, i64>(&datasource_row_ord)?,
                                    column_oid
                                };

                                // Send different kinds of cells depending on column type
                                let value_column_metadata: column::Metadata = column::Metadata::get(column_oid)?;
                                match value_column_metadata.column_type {
                                    column_type::ColumnType::Primitive(_) => {
                                        cell_sender.send(Cell::PrimitiveEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            label, 
                                            validation_failures: Vec::new() 
                                        })?;
                                    }
                                    column_type::ColumnType::Object { table_oid, .. } => {
                                        cell_sender.send(Cell::Object { 
                                            cell_oid, 
                                            value_oid, 
                                            object_schema_oid: table_oid, 
                                            object_row_oid: row.get::<&str, Option<i64>>(value_ord)?, 
                                            label, 
                                            validation_failures: Vec::new() 
                                        })?;
                                    }
                                    column_type::ColumnType::Select { table_oid, .. } => {
                                        cell_sender.send(Cell::SelectEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            select_schema_oid: table_oid, 
                                            select_row_oid: row.get::<&str, Option<i64>>(value_ord)?, 
                                            validation_failures: Vec::new() 
                                        })?;
                                    }
                                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                                        let multiselect_row_oid: Vec<i64> = match row.get::<&str, Option<String>>(value_ord)? {
                                            Some(value) => {
                                                value.split(',').filter_map(|o| if let Ok(oid) = o.parse::<i64>() { Some(oid) } else { None }).collect()
                                            }
                                            None => Vec::new()
                                        };
                                        cell_sender.send(Cell::MultiselectEntry { 
                                            cell_oid, 
                                            value_oid, 
                                            multiselect_schema_oid: table_oid, 
                                            multiselect_row_oid, 
                                            label, 
                                            validation_failures: Vec::new() 
                                        })?;
                                    }
                                    column_type::ColumnType::Formula { .. }
                                    | column_type::ColumnType::Subreport { .. } => {
                                        return Err(Error::AdhocError("Invalid return type for a formula!"));
                                    }
                                }
                            }
                            None => {
                                // If the formula result is not associated with a parameter, send as a readonly label
                                cell_sender.send(Cell::Readonly { 
                                    cell_oid, 
                                    label, 
                                    validation_failures: Vec::new() 
                                })?;
                            }
                        }
                    }
                    query::QueryBuilderColumn::Subreport { column, report_oid, datasource_ords, row_ord } => {
                        let cell_oid: CellOid = CellOid { 
                            schema_oid: column.schema.oid, 
                            row_oid: row.get::<&str, i64>(row_ord)?, 
                            column_oid: column.oid 
                        };

                        // Build the query string that is then passed through the URL of the subreport page
                        let mut schema_query_string: String = format!("schema_oid={report_oid}");
                        for (datasource, datasource_row_ord) in datasource_ords {
                            schema_query_string = format!(
                                "{schema_query_string}&d{}={}",
                                datasource.get_oid(),
                                row.get::<&str, i64>(datasource_row_ord)?
                            );
                        }

                        cell_sender.send(Cell::Subreport { 
                            cell_oid, 
                            schema_query_string, 
                            validation_failures: Vec::new() 
                        })?;
                    }
                }
            }
        }
    }

    /// Sends all cells on a page in a schema.
    pub fn query_by_schema_page(column_sender: Sender<column::Metadata>, cell_sender: Sender<Self>, schema_oid: i64, page: Page) -> Result<(), Error> {
        let schema: schema::Schema = schema::Schema::get(schema_oid)?;

        // Build the base query and retrieve columns
        let (query, query_cols) = Self::build_query(column_sender, schema_oid, 
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

        // Add filters to the query

        // Compile and run the query
        Self::run_query(cell_sender, schema_oid, query, params![page.num, page.size], query_cols)?;
        Ok(())
    }

    /// Sends all cells belonging to a particular row in a schema.
    pub fn query_by_schema_row(cell_sender: Sender<Self>, schema_oid: i64, row_oid: i64) -> Result<(), Error> {
        let schema: schema::Schema = schema::Schema::get(schema_oid)?;

        // Build the base query and retrieve columns
        let (query, query_cols) = Self::build_query(Sender::Dummy, schema_oid, 
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

        // Add filters to the query
        todo!("Implement filters for row query.");

        // Compile and run the query
        Self::run_query(cell_sender, schema_oid, query, params![row_oid], query_cols)?;
        Ok(())
    }
}