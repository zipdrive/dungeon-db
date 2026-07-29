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


pub struct WrapperCteTableColumn {
    /// The alias for the datasource where the column lies.
    pub datasource_alias: String,

    /// The metadata for the column.
    pub column_metadata: column::FullMetadata,

    /// For labels, these are the keys of the JSON object for this column
    pub child_columns: Option<WrapperCteColumns>,

    /// True if the column is required for the schema. False if it belongs to an inheritor table.
    pub is_required: bool,

    /// If recursive, this is the alias of the datasource that it recurses back to.
    pub recurses_back_to: Option<String>
}

pub struct WrapperCteReportColumn {
    /// The metadata for the column.
    pub column_metadata: column::FullMetadata,

    /// For labels, these are the keys of the JSON object for this column
    pub child_columns: Option<WrapperCteColumns>
}

pub enum WrapperCteColumns {
    TableColumns {
        columns: Vec<WrapperCteTableColumn>
    },
    ReportColumns {
        columns: Vec<WrapperCteReportColumn>
    }
}

impl WrapperCteColumns {
    /// True if there is recursion at some point. False if there is no recursion.
    pub fn is_recursive(&self) -> bool {
        match self {
            Self::TableColumns { columns } => {
                columns.iter().any(|table_column| {
                    if let Some(_) = table_column.recurses_back_to {
                        true 
                    } else if let Some(child_columns) = table_column.child_columns {
                        child_columns.is_recursive()
                    } else {
                        false
                    }
                })
            }
            Self::ReportColumns { columns } => {
                columns.iter().any(|report_column| {
                    if let Some(child_columns) = report_column.child_columns {
                        child_columns.is_recursive()
                    } else {
                        false 
                    }
                })
            }
        }
    }
}


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
    pub fn set_schema(&mut self, trans: &Transaction, schema_oid: i64, is_label: bool) -> Result<WrapperCteColumns, Error> {
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
    fn add_all_table_parameters(&mut self, trans: &Transaction, root_datasource: &Datasource, is_collection: bool, include_object_columns: bool, is_label: bool) -> Result<WrapperCteColumns, Error> {
        let mut columns: Vec<WrapperCteTableColumn> = Vec::new();

        // Add all columns to the wrapper
        sql_map_then_iter(
            trans, 
            format!(
                "
                SELECT
                    DATASOURCE_PATH,
                    COLUMN_OID,
                    IS_REQUIRED
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
                    row.get::<_, i64>("COLUMN_OID")?,
                    row.get::<_, bool>("IS_REQUIRED")?
                ))
            },
            |(datasource_path, column_oid, is_required)| {
                // Get the metadata for the column
                let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                // Schema is a table, so utilize the datasource_path to determine the path to the datasource
                let datasource: Datasource = root_datasource.append_path(datasource_path)?;

                // Cut off infinite recursion in labels
                if is_label {
                    for parent_datasource in datasource.linearize() {
                        if let Datasource::Column { column: parent_column_metadata, .. } = parent_datasource {
                            if parent_column_metadata.oid == column_metadata.oid {
                                columns.push(WrapperCteTableColumn {
                                    datasource_alias: datasource.get_alias(),
                                    column_metadata,
                                    child_columns: None,
                                    is_required,
                                    recurses_back_to: Some(parent_datasource.get_alias())
                                });
                                return Ok(None::<()>);
                            }
                        }
                    }
                }
                    
                // Add the parameters of the concrete column
                let column: WrapperCteTableColumn = self.add_concrete_parameter(trans, &datasource, column_metadata, is_collection, is_label, is_required)?;
                
                // Check if the column is one of the parameters that need to be added
                if !is_label || column_metadata.is_primary_key {
                    // Add the column to the list of columns to select in the top-level view
                    columns.push(column);
                }
                Ok(None::<()>)
            }
        )?;

        Ok(WrapperCteColumns::TableColumns { columns })
    }

    /// Adds all parameters for a report to the wrapper CTE.
    fn add_all_report_parameters(&mut self, trans: &Transaction, report_oid: i64, is_collection: bool, is_label: bool) -> Result<WrapperCteColumns, Error> {
        let mut columns: Vec<WrapperCteReportColumn> = Vec::new();

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

                // Add the parameters of the virtual column
                let column: WrapperCteReportColumn = self.add_virtual_parameter(trans, column_metadata, is_collection, is_label)?;

                // Check if the column is one of the parameters that need to be added
                if !is_label || column_metadata.is_primary_key {
                    // Add the column to the list of columns to select in the top-level view
                    columns.push(column);
                }
                Ok(None::<()>)
            }
        )?;

        Ok(WrapperCteColumns::ReportColumns { columns })
    }

    /// Adds a column on a datasource as a parameter selected by the wrapper CTE.
    fn add_concrete_parameter(&mut self, trans: &Transaction, datasource: &Datasource, column: column::FullMetadata, is_collection: bool, is_label: bool, is_required: bool) -> Result<WrapperCteTableColumn, Error> {
        // Add in any datasources that the parameter is dependent on
        self.add_datasource(datasource, is_collection);
        
        // 
        match &column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_primitive_column(column.oid);
                    return Ok(WrapperCteTableColumn {
                        datasource_alias: datasource.get_alias(),
                        column_metadata: column,
                        child_columns: None,
                        is_required,
                        recurses_back_to: None
                    });
                } else {
                    return Err(Error::adhoc(format!("Datasource {} has not been added as a CTE!", datasource.get_alias())));
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_object_column(column.oid);

                    return Ok(WrapperCteTableColumn {
                        datasource_alias: datasource.get_alias(),
                        column_metadata: column,
                        child_columns: 
                            // If part of a label, add all parameters for key columns to the wrapper
                            if is_label {
                                Some(self.add_all_table_parameters(
                                    trans, 
                                    &datasource.append_path(format!("_COLUMN{}", column.oid))?, 
                                    is_collection, 
                                    true, 
                                    is_label
                                )?)
                            } else {
                                None
                            },
                        is_required,
                        recurses_back_to: None
                    });
                } else {
                    return Err(Error::adhoc(format!("Datasource {} has not been added as a CTE!", datasource.get_alias())));
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_select_column(column.oid);

                    return Ok(WrapperCteTableColumn {
                        datasource_alias: datasource.get_alias(),
                        column_metadata: column,
                        child_columns:
                            // If part of a label, add all parameters for key columns to the wrapper
                            if is_label {
                                Some(self.add_all_table_parameters(
                                    trans, 
                                    &datasource.append_path(format!("_COLUMN{}", column.oid))?, 
                                    is_collection, 
                                    false, 
                                    is_label
                                )?)
                            } else {
                                None 
                            },
                        is_required,
                        recurses_back_to: None
                    });
                } else {
                    return Err(Error::adhoc(format!("Datasource {} has not been added as a CTE!", datasource.get_alias())));
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                // Add the datasource for the OIDs of the Multiselect column
                let multiselect_datasource = datasource.append_path(format!("_COLUMN{}", column.oid))?;
                let multiselect_datasource_oid: String = format!("{}_OID", multiselect_datasource.get_alias());
                self.add_datasource(&multiselect_datasource, true);

                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    cte.add_multiselect_column(column.oid);

                    return Ok(WrapperCteTableColumn {
                        datasource_alias: datasource.get_alias(),
                        column_metadata: column,
                        child_columns:
                            // If part of a label, add all parameters for key columns to the wrapper
                            if is_label {
                                Some(self.add_all_table_parameters(
                                    trans, 
                                    &multiselect_datasource, 
                                    true, 
                                    false, 
                                    is_label
                                )?)
                            } else {
                                None 
                            },
                        is_required,
                        recurses_back_to: None
                    });
                } else {
                    return Err(Error::adhoc(format!("Datasource {} has not been added as a CTE!", datasource.get_alias())));
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let formula = Formula::parse(formula.clone())?;
                
                // Add each parameter to the formula
                let mut params: Vec<WrapperCteTableColumn> = Vec::new();
                for (param_datasource_alias, param_column_oid, param_is_collection) in formula.get_all_params(is_collection).into_iter() {
                    let param_datasource: Datasource = Datasource::from_alias_transact(trans, param_datasource_alias)?
                        .substitute_root(
                            Datasource::get_default_datasource_oid_transact(trans, datasource.get_table_oid()?)?, 
                            datasource.clone()
                        );
                    let param_column: column::FullMetadata = column::FullMetadata::get_transact(trans, param_column_oid)?;
                    params.push(
                        self.add_concrete_parameter(
                            trans, 
                            &param_datasource, 
                            param_column, 
                            param_is_collection, 
                            is_label, 
                            true
                        )?
                    );
                }

                return Ok(WrapperCteTableColumn { 
                    datasource_alias: datasource.get_alias(),
                    column_metadata: column, 
                    child_columns: Some(WrapperCteColumns::TableColumns { columns: params }),
                    is_required,
                    recurses_back_to: None
                });
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                return Ok(WrapperCteTableColumn {
                    datasource_alias: datasource.get_alias(),
                    column_metadata: column,
                    child_columns:
                        // If part of a label, add all parameters for key columns to the wrapper
                        if is_label {
                            Some(self.add_all_report_parameters(
                                trans, 
                                report_oid.clone(), 
                                is_collection, 
                                is_label
                            )?)
                        } else {
                            None 
                        },
                    is_required,
                    recurses_back_to: None
                });
            }
        }
    }

    /// Adds a column on a report as a parameter selected by the wrapper CTE.
    fn add_virtual_parameter(&mut self, trans: &Transaction, column: column::FullMetadata, is_collection: bool, is_label: bool) -> Result<WrapperCteReportColumn, Error> {
        match &column.column_type {
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let formula = Formula::parse(formula.clone())?;
                
                // Add each parameter to the formula
                let mut params: Vec<WrapperCteTableColumn> = Vec::new();
                for (param_datasource_alias, param_column_oid, param_is_collection) in formula.get_all_params(is_collection).into_iter() {
                    let param_datasource: Datasource = Datasource::from_alias_transact(trans, param_datasource_alias)?;
                    let param_column: column::FullMetadata = column::FullMetadata::get_transact(trans, param_column_oid)?;
                    params.push(
                        self.add_concrete_parameter(
                            trans, 
                            &param_datasource, 
                            param_column, 
                            param_is_collection, 
                            is_label,
                            true
                        )?
                    );
                }

                return Ok(WrapperCteReportColumn { 
                    column_metadata: column, 
                    child_columns: Some(WrapperCteColumns::TableColumns { columns: params }) 
                });
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                return Ok(WrapperCteReportColumn { 
                    column_metadata: column, 
                    child_columns: 
                        // If part of a label, add all parameters for key columns to the wrapper
                        if is_label {
                            Some(self.add_all_report_parameters(
                                trans, 
                                report_oid.clone(), 
                                is_collection, 
                                is_label
                            )?)
                        } else {
                            None 
                        }
                });
            }
            _ => {
                return Err(Error::adhoc(format!("Columns of type {} need to belong to a table, not a report!", column.column_type.to_str())));
            }
        }
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