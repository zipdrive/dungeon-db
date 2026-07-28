use std::collections::{
    HashMap
};
use rusqlite::{
    Transaction,
    params
};
use crate::util::error::Error;
use crate::util::db::{sql_map_then_iter};
use crate::data::datasource::Datasource;
use crate::data::column;
use crate::data::column_type;
use crate::data::view::datasource_cte::{
    DatasourceCteConstructor
};
use crate::data::view::formula::Formula;


pub struct WrapperCteConstructor {
    /// The number of random values.
    random_values: usize,

    /// The CTEs pulling data from a datasource.
    cte_datasource: HashMap<String, DatasourceCteConstructor>
}

impl WrapperCteConstructor {
    /// Constructs a new wrapper CTE.
    pub fn new() -> Self {
        Self {
            random_values: 0,
            cte_datasource: HashMap::new()
        }
    }

    /// Adds all columns in a schema to the wrapper CTE.
    /// If is_label is false, then inheritor columns are not added, and the return value is all columns in the schema (preceded by datasource alias).
    /// If is_label is true, then inheritor columns are added, and the return value is all primary key columns in the schema (preceded by datasource alias).
    pub fn set_schema(&mut self, trans: &Transaction, schema_oid: i64, is_label: bool) -> Result<Vec<(Option<String>, column::FullMetadata)>, Error> {
        match Datasource::check_default_datasource_transact(trans, schema_oid)? {
            Some(root_datasource) => {
                self.add_all_table_parameters(
                    trans, 
                    &root_datasource, 
                    false, 
                    is_label.clone(), 
                    is_label
                )
            }
            None => {
                self.add_all_report_parameters(
                    trans, 
                    schema_oid, 
                    false, 
                    is_label
                )
            }
        }
    }

    /// Adds all parameters for a table to the wrapper CTE.
    fn add_all_table_parameters(&mut self, trans: &Transaction, root_datasource: &Datasource, is_collection: bool, include_object_columns: bool, is_label: bool) -> Result<Vec<(Option<String>, column::FullMetadata)>, Error> {
        let mut columns: Vec<(Option<String>, column::FullMetadata)> = Vec::new();

        // Add all columns to the wrapper
        sql_map_then_iter(
            trans, 
            format!(
                "
                SELECT
                    DATASOURCE_PATH,
                    COLUMN_OID 
                FROM METADATA_SCHEMA_COLUMN_VIEW
                WHERE SCHEMA_OID = ?1
                    {}
                ",
                if include_object_columns {
                    ""
                } else {
                    "AND IS_REQUIRED"
                }
            ), 
            params![root_datasource.get_table_oid()?], 
            |row| {
                Ok((
                    row.get::<_, String>("DATASOURCE_PATH")?,
                    row.get::<_, i64>("COLUMN_OID")?
                ))
            },
            |(datasource_path, column_oid)| {
                // Get the metadata for the column
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                // Schema is a table, so utilize the datasource_path to determine the path to the datasource
                let datasource: Datasource = root_datasource.append_path(datasource_path)?;

                // Cut off infinite recursion in labels
                if is_label && datasource.get_alias().contains(&format!("_COLUMN{}", column_metadata.oid)) {
                    return Ok(None::<()>);
                }
                
                // Check if the column is one of the parameters that need to be added
                if !is_label || column_metadata.is_primary_key {
                    // Add the column to the list of columns to select in the top-level view
                    columns.push((
                        Some(datasource.get_alias()), 
                        column_metadata.clone()
                    ));
                }
                    
                // Add the parameters of the concrete column
                self.add_concrete_parameter(trans, &datasource, column_metadata, is_collection, is_label)?;
                Ok(None::<()>)
            }
        )?;

        Ok(columns)
    }

    /// Adds all parameters for a report to the wrapper CTE.
    fn add_all_report_parameters(&mut self, trans: &Transaction, report_oid: i64, is_collection: bool, is_label: bool) -> Result<Vec<(Option<String>, column::FullMetadata)>, Error> {
        let mut columns: Vec<(Option<String>, column::FullMetadata)> = Vec::new();

        // Add all columns to the wrapper
        sql_map_then_iter(
            trans, 
            format!(
                "
                SELECT
                    COLUMN_OID 
                FROM METADATA_SCHEMA_COLUMN_VIEW
                WHERE SCHEMA_OID = ?1
                    AND IS_REQUIRED
                "
            ), 
            params![report_oid], 
            |row| row.get::<_, i64>("COLUMN_OID"),
            |column_oid| {
                // Get the metadata for the column
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                // Check if the column is one of the parameters that need to be added
                if !is_label || column_metadata.is_primary_key {
                    // Add the column to the list of columns to select in the top-level view
                    columns.push((
                        None, 
                        column_metadata.clone()
                    ));
                }

                // Add the parameters of the virtual column
                self.add_virtual_parameter(trans, column_metadata, is_collection, is_label)?;
                Ok(None::<()>)
            }
        )?;

        Ok(columns)
    }

    /// Adds a column on a datasource as a parameter selected by the wrapper CTE.
    fn add_concrete_parameter(&mut self, trans: &Transaction, datasource: &Datasource, column: column::FullMetadata, is_collection: bool, is_label: bool) -> Result<(), Error> {
        // Add in any datasources that the parameter is dependent on
        self.add_datasource(datasource.get_datasource(), is_collection);
        
        // 
        match column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_primitive_column(column.oid);
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_object_column(column.oid);

                    // If part of a label, add all parameters for key columns to the wrapper
                    if is_label {
                        self.add_all_table_parameters(
                            trans, 
                            &datasource.append_path(format!("_COLUMN{}", column.oid))?, 
                            is_collection, 
                            true, 
                            is_label
                        )?;
                    }
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_select_column(column.oid);

                    // If part of a label, add all parameters for key columns to the wrapper
                    if is_label {
                        self.add_all_table_parameters(
                            trans, 
                            &datasource.append_path(format!("_COLUMN{}", column.oid))?, 
                            is_collection, 
                            false, 
                            is_label
                        )?;
                    }
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                // Add the datasource for the OIDs of the Multiselect column
                let multiselect_datasource = datasource.append_path(format!("_COLUMN{}", column.oid))?;
                let multiselect_datasource_oid: String = format!("{}_OID", multiselect_datasource.get_alias());
                self.add_datasource(&multiselect_datasource, true);

                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_multiselect_column(column.oid);

                    // If part of a label, add all parameters for key columns to the wrapper
                    if is_label {
                        self.add_all_table_parameters(
                            trans, 
                            &multiselect_datasource, 
                            true, 
                            false, 
                            is_label
                        )?;
                    }
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let formula = Formula::parse(formula.clone())?;
                
                // Add each parameter to the formula
                for (param_datasource_alias, param_column_oid, param_is_collection) in formula.get_all_params(is_collection).into_iter() {
                    let param_datasource: Datasource = Datasource::from_alias_transact(trans, param_datasource_alias)?
                        .substitute_root(
                            Datasource::get_default_datasource_oid_transact(trans, datasource.get_table_oid()?)?, 
                            datasource.clone()
                        );
                    let param_column: column::FullMetadata = column::FullMetadata::get_transact(trans, param_column_oid)?;
                    self.add_concrete_parameter(trans, &param_datasource, param_column, param_is_collection, is_label)?;
                }
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                // If part of a label, add all parameters for key columns to the wrapper
                if is_label {
                    self.add_all_report_parameters(
                        trans, 
                        report_oid, 
                        is_collection, 
                        is_label
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Adds a column on a report as a parameter selected by the wrapper CTE.
    fn add_virtual_parameter(&mut self, trans: &Transaction, column: column::FullMetadata, is_collection: bool, is_label: bool) -> Result<(), Error> {
        match column.column_type {
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let formula = Formula::parse(formula.clone())?;
                
                // Add each parameter to the formula
                for (param_datasource_alias, param_column_oid, param_is_collection) in formula.get_all_params(is_collection).into_iter() {
                    let param_datasource: Datasource = Datasource::from_alias_transact(trans, param_datasource_alias)?;
                    let param_column: column::FullMetadata = column::FullMetadata::get_transact(trans, param_column_oid)?;
                    self.add_concrete_parameter(trans, &param_datasource, param_column, param_is_collection, is_label)?;
                }
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                // If part of a label, add all parameters for key columns to the wrapper
                if is_label {
                    self.add_all_report_parameters(
                        trans, 
                        report_oid, 
                        is_collection, 
                        is_label
                    )?;
                }
            }
            _ => {
                return Err(Error::adhoc(format!("Columns of type {} need to belong to a table, not a report!", column.column_type.to_str())));
            }
        }
        Ok(())
    }

    /// Adds a CTE for a datasource.
    fn add_datasource(&mut self, datasource: &Datasource, is_collection: bool) {
        if let Some(parent_datasource) = datasource.get_parent() {
            let parent_datasource_alias: String = parent_datasource.get_alias();
            self.add_datasource(&parent_datasource, is_collection);
            if let Some(parent_datasource_cte) = self.cte_datasource.get_mut(&parent_datasource_alias) {
                parent_datasource_cte.add_child_datasource(datasource);
            }
        }

        let datasource_alias: String = datasource.get_alias();
        if !self.cte_datasource.contains_key(&datasource_alias) {
            self.cte_datasource.insert(datasource_alias, DatasourceCteConstructor::new(datasource.clone(), is_collection));
        } else {
            if let Some(datasource_cte) = self.cte_datasource.get_mut(&datasource_alias) {
                datasource_cte.is_always_collection = datasource_cte.is_always_collection && is_collection;
            }
        }
    }


    /// Gets the CTE for a datasource, if one has been generated for that datasource.
    pub fn get_datasource_cte(&self, datasource: &Datasource) -> Option<&DatasourceCteConstructor> {
        self.cte_datasource.get(datasource)
    }

    /// Gets the OIDs selected from the wrapper.
    /// The first item of each tuple is the alias of the OID.
    /// The second item of each tuple is the expression to get the OID.
    pub fn get_oids(&self) -> Vec<(String, String)> {
        let mut oid_aliases_and_exprs: Vec<(String, String)> = Vec::new();
        for (cte_name, cte) in self.cte_datasource.iter() {
            if !cte.is_always_collection {
                oid_aliases_and_exprs.push((format!("{cte_name}_OID"), format!("w.{cte_name}_OID")));
            }
        }
        oid_aliases_and_exprs
    }


    /// Adds a new random value to the wrapper CTE.
    pub fn add_random_value(&mut self) -> usize {
        self.random_values += 1;
        self.random_values - 1
    }


    /// Constructs the datasource CTEs and the wrapper CTE.
    pub fn build(&self) -> Result<String, Error> {
        let mut cte_list: Vec<String> = Vec::new();
        let mut root_datasource_aliases: Vec<String> = Vec::new();

        // Compile each CTE representing a datasource
        for (cte_name, cte) in self.cte_datasource.iter() {
            cte_list.push(format!("{cte_name} AS ({})", cte.build()?));
            if cte.is_root_datasource() {
                root_datasource_aliases.push(cte_name.clone());
            }
        }

        // Compile the wrapper CTE
        cte_list.push(format!(
            "WRAPPER AS ({})",
            if root_datasource_aliases.len() > 0 {
                format!(
                    "SELECT {} {} FROM {}",

                    // All columns from each datasource
                    root_datasource_aliases.iter()
                        .map(|datasource_alias| format!("{datasource_alias}.*"))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap(),

                    // RANDOM() calls
                    // Done in the wrapper CTE so that the value/label/cell will be aligned
                    (0..self.random_values)
                        .map(|n| format!("RANDOM() AS RANDOM{n}"))
                        .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

                    // FROM/JOIN clauses
                    root_datasource_aliases.into_iter().reduce(|acc, e| format!("{acc} INNER JOIN {e}")).unwrap()
                )
            } else {
                format!(
                    "SELECT {} WHERE FALSE",
                    if self.random_values > 0 {
                        (0..self.random_values)
                            .map(|n| format!("RANDOM() AS RANDOM{n}"))
                            .reduce(|acc, e| format!("{acc}, {e}"))
                            .unwrap()
                    } else {
                        String::from("NULL")
                    }
                )
            }
        ));

        Ok(cte_list.into_iter().reduce(|acc, e| format!("{acc}, {e}")).unwrap())
    }
}