use base64::{Engine, prelude::{BASE64_STANDARD as base64standard}};
use rusqlite::{AndThenRows, OptionalExtension, ffi::FTS5_TOKENIZE_QUERY, types::Value};
use rusqlite::vtab::array::Array;
use rusqlite::{Connection, Params, Transaction, params};
use serde::{Deserialize, Serialize, de::value};
use tauri::{AppHandle, Emitter};
use std::{cell, collections::HashSet};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use crate::data::{datasource::Datasource, row};
use crate::data::query::{QueryBuilder};
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
    SingleRow,
    None
}

impl RetrievalLimit {
    /// Retrieves the LIMIT of the query.
    pub fn get_size(&self) -> i64 {
        match self {
            Self::Page { size, .. } => size.clone(),
            Self::SingleRow => 1,
            Self::None => i64::MAX
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase", untagged)]
pub enum CellOid {
    TableCell {
        schema_oid: i64,
        column_oid: i64,
        row_oid: i64
    },
    ReportCell {
        column_oid: i64,
        filters: Vec<(String, i64)>
    }
}

impl CellOid {
    /// Retrieves the column OID associated with this cell.
    pub fn get_column_oid(&self) -> i64 {
        match self {
            Self::TableCell { column_oid, .. }
            | Self::ReportCell { column_oid, .. } => column_oid.clone()
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct ValueOid {
    schema_oid: i64,
    row_oid: i64,
    column_oid: i64
}

const UPDATE_CELL_SIGNAL: &'static str = "cell";

impl ValueOid {
    /// Sends a signal to update any affected cells.
    pub fn query_affected_cells(&self, app: &AppHandle) -> Result<(), Error> {
        // Update the cell on the table
        app.emit(UPDATE_CELL_SIGNAL, CellOid::TableCell {
            schema_oid: self.schema_oid.clone(),
            column_oid: self.column_oid.clone(),
            row_oid: self.row_oid.clone()
        })?;

        // Query for cells with a formula that depends on it, send signal to update those cells
        let conn = db::open()?;
        let mut affected_schema_oid: HashSet<i64> = HashSet::new();
        for affected_column_results in conn
            .prepare(
                "
                WITH RECURSIVE AFFECTED_FORMULAE (FORMULA_OID, FORMULA) AS (
                    SELECT
                        OID AS FORMULA_OID,
                        FORMULA
                    FROM METADATA_COLUMN_TYPE__FORMULA
                    WHERE FORMULA LIKE '%_COLUMN' || FORMAT('%d', ?1) || '%'

                    UNION

                    SELECT
                        fdep.OID AS FORMULA_OID,
                        fdep.FORMULA
                    FROM AFFECTED_FORMULAE f
                    INNER JOIN METADATA_COLUMN c ON c.TYPE_OID = f.FORMULA_OID
                    INNER JOIN METADATA_COLUMN_TYPE__FORMULA fdep ON fdep.FORMULA LIKE '%_COLUMN' || FORMAT('%d', c.OID) || '%'
                )
                SELECT 
                    c.OID AS COLUMN_OID,
                    c.SCHEMA_OID,
                    f.FORMULA
                FROM AFFECTED_FORMULAE f
                INNER JOIN METADATA_COLUMN c ON c.TYPE_OID = f.FORMULA_OID
                "
            )?
            .query_map(params![self.column_oid], |row| Ok((
                row.get::<_, i64>("COLUMN_OID")?,
                row.get::<_, i64>("SCHEMA_OID")?,
                row.get::<_, String>("FORMULA")?
            )))? {
            
            let (affected_column_oid, affected_column_schema_oid, affected_column_formula) = affected_column_results?;
            
            // Update the entire schema containing the affected column, just in case
            affected_schema_oid.insert(affected_column_schema_oid);
        }
        schema::FullMetadata::query_affected_schema(app, affected_schema_oid.into_iter().collect())?;

        Ok(())
    }
}



#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Cell {
    MaxIndex(i64),
    Row {
        row_identifier: Option<(i64, i64)>,
        index: i64,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
        validation_failures: Vec<FailedValidation>
    },
    Readonly {
        cell_oid: CellOid,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    Subreport {
        cell_oid: CellOid,
        label: String,
        schema_query_string: String,
        validation_failures: Vec<FailedValidation>
    },
    PrimitiveEntry {
        cell_oid: CellOid,
        value_oid: ValueOid,
        label: Option<String>,
        primitive_type: column_type::Primitive,
        validation_failures: Vec<FailedValidation>
    },
    FileEntry {
        cell_oid: CellOid,
        value_oid: ValueOid,
        file_oid: Option<i64>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    Object {
        cell_oid: CellOid,
        value_oid: ValueOid,
        object_schema_oid: i64,
        object_query_string: Option<String>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    SelectEntry {
        cell_oid: CellOid,
        value_oid: ValueOid,
        select_schema_oid: i64,
        select_row_oid: Option<i64>,
        validation_failures: Vec<FailedValidation>
    },
    MultiselectEntry {
        cell_oid: CellOid,
        value_oid: ValueOid,
        multiselect_schema_oid: i64,
        multiselect_row_oid: Vec<i64>,
        label: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    AddNewRowButton {
        table_oid: i64,
        fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
        column_span: usize
    }
}



impl Cell {
    /// Retrieves the value of a cell.
    pub fn get(cell_oid: CellOid) -> Result<Self, Error> {
        let conn = db::open()?;

        // Build the query
        let (query, filters) = match cell_oid {
            CellOid::TableCell { schema_oid, column_oid, row_oid } => {
                // Get the table datasource
                let table_datasource: Datasource = Datasource::get_default_datasource_transact(&conn, schema_oid)?;
                // Get the column metadata
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;
                
                // Build query to get data for each column in the schema
                let mut query: query::QueryBuilder = query::QueryBuilder::new(vec![table_datasource.clone()])?;
                query.insert_column(Some(&table_datasource), column_metadata)?;

                // Filter based on the particular row in the table
                query.insert_row_filter(table_datasource.get_alias(), row_oid);

                // Return the built query
                (query, vec![(table_datasource.get_alias(), row_oid)])
            }
            CellOid::ReportCell { column_oid, filters } => {
                // Get the column metadata
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;
                
                // Build query to get data for each column in the schema
                let mut query: query::QueryBuilder = query::QueryBuilder::new(Vec::new())?;
                query.insert_column(None, column_metadata)?;

                // Filter based on the particular row in the table
                for (filter_datasource_alias, filter_datasource_row_oid) in filters.clone() {
                    query.insert_row_filter(filter_datasource_alias, filter_datasource_row_oid);
                }
                
                // Return the built query
                (query, filters)
            }
        };

        // Run the query, return the last cell (only one cell should be returned)
        let mut last_cell: Option<Self> = None;
        Self::run_query(Sender::Callback(
            Box::new(
                |cell| {
                    last_cell = Some(cell);
                    Ok(())
                }
            )), 
            query, 
            filters, 
            RetrievalLimit::SingleRow
        )?;
        match last_cell {
            Some(cell) => Ok(cell),
            None => Err(Error::AdhocError("Cell does not exist."))
        }
    }

    /// Gets the OIDs pointing to the value of the cell.
    pub fn get_cell_oid(&self) -> Result<CellOid, Error> {
        match self {
            Self::PrimitiveEntry { cell_oid, .. }
            | Self::FileEntry { cell_oid, .. }
            | Self::Object { cell_oid, .. }
            | Self::SelectEntry { cell_oid, .. }
            | Self::MultiselectEntry { cell_oid, .. }
            | Self::Readonly { cell_oid, .. }
            | Self::Subreport { cell_oid, .. } => Ok(cell_oid.clone()),
            Self::Row { .. } => Err(Error::AdhocError("A row does not have an associated cell.")),
            Self::AddNewRowButton { .. } => Err(Error::AdhocError("A button to add a new row does not have an associated cell.")),
            Self::MaxIndex(_) => Err(Error::AdhocError("The maximum index does not have an associated cell."))
        }
    }

    /// Gets the OIDs pointing to the value of the cell.
    pub fn get_value_oid(&self) -> Result<ValueOid, Error> {
        match self {
            Self::PrimitiveEntry { value_oid, .. }
            | Self::FileEntry { value_oid, .. }
            | Self::Object { value_oid, .. }
            | Self::SelectEntry { value_oid, .. }
            | Self::MultiselectEntry { value_oid, .. } => Ok(value_oid.clone()),
            Self::Readonly { .. } => Err(Error::AdhocError("A readonly cell does not read from a value.")),
            Self::Subreport { .. } => Err(Error::AdhocError("A subreport cell does not read from a value.")),
            Self::Row { .. } => Err(Error::AdhocError("A row does not read from a value.")),
            Self::AddNewRowButton { .. } => Err(Error::AdhocError("A button to add a new row does not read from a value.")),
            Self::MaxIndex(_) => Err(Error::AdhocError("The maximum index does not read from a value."))
        }
    }

    /// Recursively build mapping from schema to default datasource by traversing up the inheritance hierarchy.
    fn build_schema_to_datasource_mapping(trans: &Transaction, schema_to_datasource: &mut HashMap<i64, datasource::Datasource>, table_metadata: table::FullMetadata) -> Result<(), Error> {
        for master_schema_oid in table_metadata.schema.master_schema_oids.iter() {
            if !schema_to_datasource.contains_key(master_schema_oid) {
                if let schema::Schema::Table(master_table) = schema::Schema::get(master_schema_oid.clone())? {
                    let datasource: datasource::Datasource = datasource::Datasource::MasterTable { 
                        parent_datasource: Box::new(schema_to_datasource[&table_metadata.schema.oid].clone()), 
                        table_oid: master_table.schema.oid
                    };
                    schema_to_datasource.insert(master_table.schema.oid, datasource);

                    Self::build_schema_to_datasource_mapping(trans, schema_to_datasource, master_table)?;
                }
            }
        }
        Ok(())
    }

    /// Builds a basic query to get all columns associated with the given schema.
    /// Also sends the column information through the provided Sender object.
    fn build_query(mut column_sender: Sender<column::FullMetadata>, schema_oid: i64, initial_datasources: Vec<datasource::Datasource>, filters: Vec<(String, i64)>) -> Result<query::QueryBuilder, Error> {
        // Construct mapping from schema to default datasource
        let mut schema_to_datasource: HashMap<i64, datasource::Datasource> = HashMap::new();
        {
            let mut conn = db::open()?;
            let trans = conn.transaction()?;

            for datasource in initial_datasources.iter() {
                schema_to_datasource.insert(datasource.get_schema_oid()?, datasource.clone());

                // Make sure all master tables of a root table are also included as a datasource
                if let datasource::Datasource::Table { table_oid, .. } = datasource {
                    let table: table::FullMetadata = table::FullMetadata::get(table_oid.clone())?;
                    Self::build_schema_to_datasource_mapping(&trans, &mut schema_to_datasource, table)?;
                }
            }

            trans.commit()?;
        }
        
        // Build query to get data for each column in the schema
        let mut query: query::QueryBuilder = query::QueryBuilder::new(initial_datasources)?;
        column::FullMetadata::query_by_schema(
            Sender::Callback(Box::new(|col: column::FullMetadata| -> Result<(), Error> {
                // Add column to query
                query.insert_column(schema_to_datasource.get(&col.schema.oid), col.clone())?;

                // Send column metadata over the provided Sender object
                column_sender.send(col)?;
                Ok(())
            })), 
            schema_oid
        )?;

        let conn: Connection = db::open()?;

        // Filter rows in the query based on the METADATA_REPORT.FILTER_FORMULA formula
        println!("Now applying filter formula...");
        if let Some(Some(filter_formula)) = conn.query_one("SELECT FILTER_FORMULA FROM METADATA_REPORT WHERE OID = ?1", params![schema_oid], |row| row.get::<_, Option<String>>("FILTER_FORMULA")).optional()? {
            // Insert WHERE clause
            query.insert_filter(filter_formula)?;
        }

        // Additionally filter rows in the query based on the provided filters
        println!("Now applying row filters {:?}", filters);
        for (filter_datasource_alias, filter_datasource_row_oid) in filters {
            query.insert_row_filter(filter_datasource_alias, filter_datasource_row_oid);
        }

        // Group rows in the query based on the METADATA_REPORT_GROUPBY table
        println!("Now applying GROUP BY...");
        let mut stmt_groupby = conn.prepare(
            "
            SELECT 
                COLUMN_OID 
            FROM METADATA_REPORT_GROUPBY_VIEW
            WHERE REPORT_OID = ?1 
            "
        )?;
        for row_result in stmt_groupby.query_and_then(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
            let column_oid = row_result?;
            // Insert GROUP BY clause
            query.insert_grouping(column_oid)?;
        }

        // Order the query based on the METADATA_SCHEMA_ORDERBY table
        println!("Now applying ORDER BY...");
        let mut stmt_orderby = conn.prepare(
            "
            SELECT 
                DATASOURCE_ALIAS,
                COLUMN_OID, 
                SORT_ASCENDING 
            FROM METADATA_SCHEMA_ORDERBY_VIEW
            WHERE SCHEMA_OID = ?1 
            "
        )?;
        for row_result in stmt_orderby.query_and_then(params![schema_oid], |row| { Ok::<(String, i64, bool), rusqlite::Error>((row.get::<_, String>("DATASOURCE_ALIAS")?, row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)) })? {
            let (datasource_alias, column_oid, sort_ascending) = row_result?;
            let column_datasource: datasource::Datasource = datasource::Datasource::from_alias_transact(&conn, datasource_alias)?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;

            // Insert ORDER BY clause
            query.insert_ordering(&column_datasource, column_metadata, sort_ascending)?;
        }

        Ok(query)
    }

    fn run_query(mut cell_sender: Sender<Self>, query: query::QueryBuilder, filters: Vec<(String, i64)>, limit: RetrievalLimit) -> Result<(), Error> {
        // Compile and run the query
        let conn: Connection = db::open()?;
        println!("Now compiling query...");
        if let Some((cmd_query, cols, datasource_aliases)) = query.compile()? {
            println!("Query compiled successfully.\n{cmd_query}");

            // First, check for which datasources in the query are unfixed
            let mut unfixed_datasources: HashSet<Datasource> = HashSet::new();
            for datasource_alias in datasource_aliases.iter() {
                let datasource: Datasource = Datasource::from_alias(datasource_alias.clone())?;
                let base_datasource: Datasource = datasource.seek_basis()?;
                let base_datasource_alias: String = base_datasource.get_alias();

                // A datasource is fixed if it or a datasource that branches from it is filtered
                if !filters.iter().any(|(fixed_datasource_alias, _)| fixed_datasource_alias.starts_with(&base_datasource_alias)) {
                    unfixed_datasources.insert(base_datasource);
                }
            }

            // Also, determine which datasources should be fixed for the purposes of creating new roes
            let fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)> = if unfixed_datasources.len() == 1 {
                match unfixed_datasources.iter().next().unwrap() {
                    Datasource::Table { .. } => None,
                    Datasource::Column { parent_datasource, column } => {
                        let parent_datasource_alias: String = parent_datasource.get_alias();
                        let parent_datasource_table_oid: i64 = parent_datasource.get_schema_oid()?;
                        filters.iter().find_map(|(fixed_datasource_alias, fixed_datasource_row_oid)| if *fixed_datasource_alias == parent_datasource_alias {
                            Some((parent_datasource_table_oid, fixed_datasource_row_oid.clone(), column.clone()))
                        } else {
                            None 
                        })
                    }
                    Datasource::MasterTable { .. } 
                    | Datasource::InheritorTable { .. } => {
                        // Neither of these cases should ever occur, so throw an error if it does
                        return Err(Error::AdhocError("The only found unfixed base datasource has a strict 1-to-1 relationship with its parent datasource, which is not allowed."));
                    }
                }
            } else {
                None
            };

            // First, get the maximum index
            let cmd_max_index_query = format!("SELECT ROW_INDEX FROM ({cmd_query}) ORDER BY ROW_INDEX DESC LIMIT 1");
            cell_sender.send(
                Cell::MaxIndex(
                    conn.query_one(&cmd_max_index_query, [], |row| row.get::<_, i64>("ROW_INDEX")).optional()?.unwrap_or(0)
                )
            )?;

            // Then, start working on the actual query
            // Add row limits
            let cmd_query: String = match limit {
                RetrievalLimit::SingleRow => format!("{cmd_query} LIMIT 1"),
                RetrievalLimit::Page { num, size } => format!("{cmd_query} LIMIT {size} OFFSET {}", size * (num - 1)),
                RetrievalLimit::None => cmd_query
            };

            // Run the query
            let mut stmt_query = conn.prepare(&cmd_query)?;
            let mut rows_query = stmt_query.query([])?;
            let mut row_count: i64 = 0;
            loop {
                let Some(row) = rows_query.next()? else { break; };
                row_count += 1;

                // Load all filters used to identify the row
                let mut filters: Vec<(String, i64)> = Vec::new();
                for datasource_alias in datasource_aliases.iter() {
                    let datasource_row_alias: String = format!("{datasource_alias}_OID");
                    if let Some(datasource_row_oid) = row.get::<&str, Option<i64>>(&datasource_row_alias)? {
                        filters.push((datasource_alias.clone(), datasource_row_oid));
                    }
                }

                // Determine if there is a specific schema that can be used to identify the row
                let row_identifier: Option<(i64, i64)> = if unfixed_datasources.len() == 1 {
                    let datasource = unfixed_datasources.iter().next().unwrap();
                    let datasource_schema_oid: i64 = datasource.get_schema_oid()?;
                    let datasource_row_alias: String = format!("{}_OID", datasource.get_alias());
                    if let Some(datasource_row_oid) = row.get::<&str, Option<i64>>(&datasource_row_alias)? {
                        Some((datasource_schema_oid, datasource_row_oid))
                    } else {
                        None
                    }
                } else {
                    None
                };

                // First, send a header for the row
                cell_sender.send(Cell::Row { 
                    row_identifier,
                    index: row.get("ROW_INDEX")?, 
                    fixed_parent_datasource: fixed_parent_datasource.clone(),
                    validation_failures: Vec::new() 
                })?;
                
                // Then, send a cell for each column
                for c in cols.iter() {
                    match c {
                        query::QueryBuilderColumn::Primitive { schema_oid, schema_row_ord, column_oid, label_ord, primitive_type, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let row_oid: i64 = row.get::<&str, i64>(schema_row_ord)?;
                            let cell_oid: CellOid = CellOid::TableCell { 
                                schema_oid: schema_oid.clone(), 
                                column_oid: column_oid.clone(), 
                                row_oid: row_oid.clone()
                            };
                            let value_oid: ValueOid = ValueOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row_oid.clone(), 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::PrimitiveEntry { 
                                cell_oid, 
                                value_oid,
                                label, 
                                primitive_type: primitive_type.clone(),
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::File { schema_oid, schema_row_ord, column_oid, label_ord, file_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let file_oid: Option<i64> = row.get::<&str, Option<i64>>(file_ord)?;
                            let row_oid: i64 = row.get::<&str, i64>(schema_row_ord)?;
                            let cell_oid: CellOid = CellOid::TableCell { 
                                schema_oid: schema_oid.clone(), 
                                column_oid: column_oid.clone(), 
                                row_oid: row_oid.clone()
                            };
                            let value_oid: ValueOid = ValueOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row_oid.clone(), 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::FileEntry { 
                                cell_oid, 
                                value_oid, 
                                file_oid, 
                                label, 
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Object { schema_oid, schema_row_ord, column_oid, label_ord, object_schema_oid, object_query_string_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let row_oid: i64 = row.get::<&str, i64>(schema_row_ord)?;
                            let cell_oid: CellOid = CellOid::TableCell { 
                                schema_oid: schema_oid.clone(), 
                                column_oid: column_oid.clone(), 
                                row_oid: row_oid.clone()
                            };
                            let value_oid: ValueOid = ValueOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row_oid.clone(), 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::Object { 
                                cell_oid, 
                                value_oid,
                                object_schema_oid: object_schema_oid.clone(),
                                object_query_string: row.get::<&str, Option<String>>(object_query_string_ord)?,
                                label, 
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Select { schema_oid, schema_row_ord, column_oid, select_schema_oid, select_row_ord, .. } => {
                            let row_oid: i64 = row.get::<&str, i64>(schema_row_ord)?;
                            let cell_oid: CellOid = CellOid::TableCell { 
                                schema_oid: schema_oid.clone(), 
                                column_oid: column_oid.clone(), 
                                row_oid: row_oid.clone()
                            };
                            let value_oid: ValueOid = ValueOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row_oid.clone(), 
                                column_oid: column_oid.clone()
                            };
                            cell_sender.send(Cell::SelectEntry { 
                                cell_oid, 
                                value_oid,
                                select_schema_oid: select_schema_oid.clone(),
                                select_row_oid: row.get::<&str, Option<i64>>(select_row_ord)?,
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Multiselect { schema_oid, schema_row_ord, column_oid, label_ord, select_schema_oid, select_row_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let row_oid: i64 = row.get::<&str, i64>(schema_row_ord)?;
                            let cell_oid: CellOid = CellOid::TableCell { 
                                schema_oid: schema_oid.clone(), 
                                column_oid: column_oid.clone(), 
                                row_oid: row_oid.clone()
                            };
                            let value_oid: ValueOid = ValueOid { 
                                schema_oid: schema_oid.clone(), 
                                row_oid: row_oid.clone(), 
                                column_oid: column_oid.clone()
                            };

                            let multiselect_row_oid: Vec<i64> = match row.get::<&str, Option<String>>(select_row_ord)? {
                                Some(s) => s.split(',').filter_map(|n| match n.parse::<i64>() { Ok(num) => Some(num), Err(_) => None }).collect(),
                                None => Vec::new()
                            };

                            cell_sender.send(Cell::MultiselectEntry { 
                                cell_oid, 
                                value_oid,
                                multiselect_schema_oid: select_schema_oid.clone(),
                                multiselect_row_oid,
                                label,
                                validation_failures: Vec::new() 
                            })?;
                        }
                        query::QueryBuilderColumn::Formula { schema_oid, schema_row_ord, column_oid, param_ord, label_ord, value_ord, .. } => {
                            let label: Option<String> = row.get::<&str, Option<String>>(label_ord)?;
                            let cell_oid: CellOid = match schema_row_ord {
                                Some(schema_row_ord) => CellOid::TableCell { 
                                    schema_oid: schema_oid.clone(), 
                                    column_oid: column_oid.clone(), 
                                    row_oid: row.get::<&str, i64>(schema_row_ord)?
                                },
                                None => CellOid::ReportCell { 
                                    column_oid: column_oid.clone(), 
                                    filters: filters.clone() 
                                }
                            };

                            if let Some(param) = row.get::<&str, Option<String>>(param_ord)? {
                                let Some((value_datasource_alias, value_column_oid_str)) = param.split_once(':') else {
                                    return Err(Error::AdhocError("Formula returned nonempty parameter, but in nonstandard format."));
                                };
                                let Ok(value_column_oid) = value_column_oid_str.parse::<i64>() else {
                                    return Err(Error::AdhocError("Formula returned column OID that was not an integer."));
                                };
                                let value_column = column::FullMetadata::get(value_column_oid)?;
                                
                                let value_row_ord: String = format!("{value_datasource_alias}_OID");
                                if let Some(value_row_oid) = row.get::<&str, Option<i64>>(&value_row_ord)? {
                                    let value_oid: ValueOid = ValueOid {
                                        schema_oid: value_column.schema.oid,
                                        row_oid: value_row_oid,
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
                                                object_query_string: row.get::<&str, Option<String>>(value_ord)?,
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
                                                multiselect_schema_oid: if {
                                                    let value_datasource: Datasource = Datasource::from_alias(String::from(value_datasource_alias))?;
                                                    value_datasource.get_schema_oid()?
                                                } == value_column.schema.oid {
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
                                    cell_sender.send(Cell::Readonly { 
                                        cell_oid, 
                                        label: None, 
                                        validation_failures: Vec::new()
                                    })?;
                                }
                            } else {
                                // If the value of the cell is not directly linked to the value of another cell, send as a readonly value
                                cell_sender.send(Cell::Readonly { 
                                    cell_oid, 
                                    label, 
                                    validation_failures: Vec::new() 
                                })?;
                            }
                        }
                        query::QueryBuilderColumn::Subreport { schema_oid, schema_row_ord, column_oid, subreport_metadata } => {
                            let cell_oid: CellOid = match schema_row_ord {
                                Some(schema_row_ord) => CellOid::TableCell { 
                                    schema_oid: schema_oid.clone(), 
                                    column_oid: column_oid.clone(), 
                                    row_oid: row.get::<&str, i64>(schema_row_ord)?
                                },
                                None => CellOid::ReportCell { 
                                    column_oid: column_oid.clone(), 
                                    filters: filters.clone() 
                                }
                            };

                            cell_sender.send(Cell::Subreport { 
                                schema_query_string: match &cell_oid {
                                    CellOid::TableCell { schema_oid, row_oid, .. } => {
                                        let table_datasource: Datasource = Datasource::get_default_datasource_transact(&conn, schema_oid.clone())?;
                                        format!(
                                            "schema_oid={}&{}={row_oid}", 
                                            subreport_metadata.schema.oid,
                                            table_datasource.get_alias()
                                        )
                                    },
                                    CellOid::ReportCell { filters, .. } => {
                                        filters.iter()
                                            .fold(
                                                // First key is "schema_oid", which determines the schema that's pulled up when the subreport is opened
                                                format!("schema_oid={}", subreport_metadata.schema.oid),
                                                // Other keys identify the filters on the subreport
                                                |acc, (datasource_alias, datasource_row_oid)| format!("{acc}&{datasource_alias}={datasource_row_oid}")
                                            )
                                    }
                                },
                                cell_oid, 
                                label: subreport_metadata.schema.name.clone(),
                                validation_failures: Vec::new() 
                            })?;
                        }
                    }
                }
            }

            // Send over an Add New Row button at the bottom, if there is room and it is applicable to the schema
            if row_count < limit.get_size() {
                // Assuming there is space for an Add New Row button, it is allowed to create a new row 
                // if there is only a single unfixed root or 1-to-* datasource.

                // According to the above rule, check that there is only one unfixed root and/or 1-to-* datasource.
                if unfixed_datasources.len() == 1 {
                    let unfixed_datasource: &Datasource = unfixed_datasources.iter().next().unwrap();
                    let table_oid: i64 = unfixed_datasource.get_schema_oid()?;
                    cell_sender.send(Cell::AddNewRowButton { 
                        table_oid, 
                        fixed_parent_datasource,
                        column_span: cols.len()
                    })?;
                }
            }
        } // If the report doesn't have any datasources, just don't run it
        return Ok(()); 
    }

    /// Sends all cells on a page in a schema.
    pub fn query_by_schema(column_sender: Sender<column::FullMetadata>, cell_sender: Sender<Self>, schema_oid: i64, filters: Vec<(String, i64)>, limit: RetrievalLimit) -> Result<(), Error> {
        let schema: schema::Schema = schema::Schema::get(schema_oid)?;

        let table_datasource: Option<Datasource> = match &schema {
            schema::Schema::Table(table_metadata) => {
                let conn = db::open()?;
                Some(Datasource::get_default_datasource_transact(&conn, table_metadata.schema.oid)?)
            }
            schema::Schema::Report(_) => None 
        };

        // Build the base query and retrieve columns
        let query = Self::build_query(column_sender, schema_oid, 
            match &table_datasource {
                Some(table_datasource) => vec![table_datasource.clone()],
                None => Vec::new()
            },
            filters.clone()
        )?;

        // Compile and run the query
        Self::run_query(cell_sender, query, filters, limit)?;
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
            Self::Object { value_oid, object_schema_oid, object_query_string, .. } => {
                // Trash the previous object
                let sql_trash_previous: String = format!(
                    "UPDATE TABLE{} SET TRASH = TRUE WHERE OID = (SELECT COLUMN{} AS O_OID FROM TABLE{} WHERE OID = ?1)",
                    object_schema_oid,
                    value_oid.column_oid,
                    value_oid.schema_oid
                );
                trans.execute(&sql_trash_previous, params![value_oid.row_oid])?;

                match object_query_string {
                    Some(_) => {
                        // Create a new Object row
                        let mut object_master_rows: HashMap<i64, i64> = HashMap::new();
                        let object_row_oid: i64 = row::insert_transact(&trans, object_schema_oid.clone(), None, &mut object_master_rows)?;

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
                    "DELETE FROM MULTISELECT{} WHERE TABLE{}_OID = ?1 AND TABLE{}_OID NOT IN rarray(?2)",
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
                    trans.execute(&sql_insert, params![value_oid.row_oid, selected_oid])?;
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
    pub blob_oid: ValueOid
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