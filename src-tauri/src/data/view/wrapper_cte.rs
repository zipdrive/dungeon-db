use std::collections::{
    HashMap,
    HashSet
};
use rocket::response::content;
use rusqlite::{
    Transaction,
    params
};
use crate::data::view::parameter::SelectExpressions;
use crate::util::error::Error;
use crate::util::formula::Formula;
use crate::data::datasource::Datasource;
use crate::data::column;
use crate::data::column_type;
use crate::data::view::datasource_cte::{
    DatasourceCteConstructor
};
use crate::data::view::parameter::{
    SelectParameterDatasource,
    SelectParameterSlice,
    SelectParameterContext,
    SelectParameterType,
    SelectParameter
};


struct WrapperCteConstructor {
    /// The number of random values.
    random_values: usize,

    /// The CTEs pulling data from a datasource.
    cte_datasource: HashMap<String, DatasourceCteConstructor>
}

impl WrapperCteConstructor {
    /// Adds a CTE for a datasource to the SELECT statement.
    fn add_datasource(&mut self, datasource: &Datasource, is_collection: bool) {
        if let Some(parent_datasource) = datasource.get_parent() {
            let parent_datasource_alias: String = parent_datasource.get_alias();
            self.add_datasource(&parent_datasource, is_collection);
            if let Some(parent_datasource_cte) = self.cte_datasource.get_mut(&parent_datasource_alias) {
                parent_datasource_cte.child_datasources.insert(datasource.clone());
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

    /// Adds a parameter to the wrapper.
    pub fn add_parameter(&mut self, trans: &Transaction, mut context: SelectParameterContext, column: column::FullMetadata) -> Result<(), Error> {
        // Add in any datasources that the parameter is dependent on
        // Also, modify Collection contexts to record how the expression should be partitioned
        match &mut context {
            SelectParameterContext::Scalar { datasource, .. } => {
                match datasource {
                    Some(datasource) => { // Concrete parameter
                        // Add the datasource
                        self.add_datasource(datasource.get_datasource(), false);
                    }
                    None => { // Virtual parameter

                    }
                }
            }
            SelectParameterContext::Collection { datasource, min_depth, .. } => {
                match datasource {
                    Some(datasource) => { // Concrete parameter
                        let datasource: &Datasource = datasource.get_datasource();
                        self.add_datasource(datasource, true);
                        
                        // Check if the minimum depth has changed
                        let root_oid: i64 = datasource.get_root_datasource_oid();
                        if let Some(former_min_depth) = min_depth.get_mut(&root_oid) {
                            if let Some(former_min_depth_inner) = former_min_depth {
                                *former_min_depth = datasource.find_commonality(former_min_depth_inner);
                            }
                        } else {
                            min_depth.insert(root_oid, datasource.get_parent());
                        }
                    }
                    None => { // Virtual parameter

                    }
                }
            }
        }
        Ok(())
    }

    /// Adds a column on a datasource as a parameter to this SELECT statement.
    /// Make sure to add Subreport columns after all other columns.
    fn add_concrete_parameter<P>(&mut self, trans: &Transaction, mut context: SelectParameterContext, column: column::FullMetadata) -> Result<P, Error> where P : SelectParameter {
        // Add in any datasources that the parameter is dependent on
        // Also, modify Collection contexts to record how the expression should be partitioned
        match &mut context {
            SelectParameterContext::Scalar { datasource, .. } => {
                self.add_datasource(datasource.get_datasource(), false);
            }
            SelectParameterContext::Collection { min_depth, .. } => {
                let datasource: &Datasource = datasource.get_datasource();
                self.add_datasource(datasource, true);
                
                // Check if the minimum depth has changed
                let root_oid: i64 = datasource.get_root_datasource_oid();
                if let Some(former_min_depth) = min_depth.get_mut(&root_oid) {
                    if let Some(former_min_depth_inner) = former_min_depth {
                        *former_min_depth = datasource.find_commonality(former_min_depth_inner);
                    }
                } else {
                    min_depth.insert(root_oid, datasource.get_parent());
                }
            }
        }
        
        // First item in tuple: The type of the parameter
        // Second item in tuple: The table OID of the associated cell
        // Third item in tuple: The column OID of the associated cell
        // Fourth item in tuple: The row OID of the associated cell

        
        
        match column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_datasource().get_alias()) {
                    let cte_column = cte.add_primitive_column(column.oid);
                    let scalar_type = SelectParameterType::from(prim);
                    
                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let plain_label_expr: String = scalar_type.construct_plain_label_expr(&value_expr);
                    let json_label_expr: String = scalar_type.construct_json_label_expr(&value_expr);

                    // Determine the hot-reload and full-reload dependencies of the parameter
                    let (isolated_dependency_exprs, full_reload_dependency_exprs): (HashSet<String>, HashSet<String>) = {
                        match context {
                            SelectParameterContext::Scalar => {
                                let mut isolated_dependency_exprs: HashSet<String> = HashSet::new();
                                let mut full_reload_dependency_exprs: HashSet<String> = HashSet::new();
                                let dependent_basis_datasource_alias = datasource.get_datasource().seek_basis()?.get_alias();
                                for dependent_datasource in datasource.get_datasource().linearize() {
                                    let dependent_datasource_alias: String = dependent_datasource.get_alias();
                                    let dependent_datasource_table_oid: i64 = dependent_datasource.get_table_oid()?;
                                    if let Datasource::Column { parent_datasource, column: dependent_column, .. } = dependent_datasource {
                                        let dependent_cell_expr: String = format!(
                                            "('{}:{}:' || w.{}_OID)",
                                            dependent_column.schema.oid,
                                            dependent_column.oid,
                                            if dependent_column.schema.oid == dependent_datasource_table_oid {
                                                // Row OID is on this datasource
                                                dependent_datasource_alias.clone()
                                            } else {
                                                // Row OID is on parent datasource
                                                parent_datasource.get_alias()
                                            }
                                        );
                                        if dependent_basis_datasource_alias.starts_with(&dependent_datasource_alias) && dependent_datasource_alias != dependent_basis_datasource_alias {
                                            // A change to the cell won't affect the cardinality of the schema
                                            isolated_dependency_exprs.insert(dependent_cell_expr);
                                        } else {
                                            // A change to the cell will affect the cardinality of the schema
                                            full_reload_dependency_exprs.insert(dependent_cell_expr);
                                        }
                                    }
                                }
                                (
                                    isolated_dependency_exprs,
                                    full_reload_dependency_exprs
                                )
                            }
                            SelectParameterContext::Collection { .. } => {
                                let mut isolated_dependency_exprs: HashSet<String> = HashSet::new();
                                for dependent_datasource in datasource.get_datasource().linearize() {
                                    if let Datasource::Column { column: dependent_column, .. } = dependent_datasource {
                                        let dependent_cell_expr: String = format!(
                                            "('{}:{}:*')",
                                            dependent_column.schema.oid,
                                            dependent_column.oid
                                        );
                                        isolated_dependency_exprs.insert(dependent_cell_expr);
                                    }
                                }
                                (
                                    isolated_dependency_exprs,
                                    HashSet::new()
                                )
                            }
                        }
                    };

                    return Ok(P::new(
                        SelectExpressions {
                            value_expr,
                            plain_label_expr,
                            json_label_expr,
                            cell_expr,
                            isolated_dependency_exprs,
                            full_reload_dependency_exprs
                        },
                        scalar_type, 
                        context
                    ));
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.datasource.get_alias()) {
                    let cte_column = cte.add_object_column(column.oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    println!("    Now constructing Object label...");
                    let (
                        plain_label_expr_norecursion,
                        plain_label_expr_recursion,
                        json_label_expr_norecursion,
                        json_label_expr_recursion
                    ) = self.construct_object_label(trans, datasource, column.oid, table_oid, &value_expr, match context { SelectParameterContext::Scalar => true, SelectParameterContext::Collection { .. } => false })?;
                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion, 
                        plain_label_expr_recursion, 
                        json_label_expr_norecursion, 
                        json_label_expr_recursion, 
                        value_expr_norecursion: value_expr.clone(), 
                        value_expr_recursion: value_expr, 
                        cell_expr, 
                        isolated_dependency_exprs,
                        full_reload_dependency_exprs,
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.datasource.get_alias()) {
                    let cte_column = cte.add_select_column(column.oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let (
                        plain_label_expr_norecursion,
                        plain_label_expr_recursion,
                        json_label_expr_norecursion,
                        json_label_expr_recursion
                    ) = self.construct_select_label(trans, datasource, column.oid, table_oid, &value_expr, match context { SelectParameterContext::Scalar => true, SelectParameterContext::Collection { .. } => false })?;
                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion, 
                        plain_label_expr_recursion, 
                        json_label_expr_norecursion, 
                        json_label_expr_recursion, 
                        value_expr_norecursion: value_expr.clone(), 
                        value_expr_recursion: value_expr, 
                        cell_expr, 
                        isolated_dependency_exprs,
                        full_reload_dependency_exprs,
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                // Add the datasource for the OIDs of the Multiselect column
                let multiselect_datasource = datasource.datasource.append_path(format!("_COLUMN{}", column.oid))?;
                let multiselect_datasource_oid: String = format!("{}_OID", multiselect_datasource.get_alias());
                self.add_datasource(multiselect_datasource, true);

                if let Some(cte) = self.cte_datasource.get_mut(&datasource.datasource.get_alias()) {
                    let cte_column = cte.add_multiselect_column(column.oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let (
                        _, _, // We don't care about the plain labels, because a Multiselect label is always JSON
                        item_json_label_expr_norecursion,
                        item_json_label_expr_recursion
                    ) = self.construct_select_label(trans, datasource, column.oid, table_oid, &value_expr, match context { SelectParameterContext::Scalar => true, SelectParameterContext::Collection { .. } => false })?;

                    let plain_label_expr_norecursion: String = String::from("NULL");
                    let plain_label_expr_recursion: String = String::from("NULL");
                    let json_label_expr_norecursion: String = format!("('[ ' || GROUP_CONCAT({item_json_label_expr_norecursion}, ', ') || ' ]')");
                    let json_label_expr_recursion: String = format!("('[ ' || GROUP_CONCAT({item_json_label_expr_recursion}, ', ') || ' ]')");

                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion, 
                        plain_label_expr_recursion, 
                        json_label_expr_norecursion, 
                        json_label_expr_recursion, 
                        value_expr_norecursion: value_expr.clone(), 
                        value_expr_recursion: value_expr, 
                        cell_expr, 
                        isolated_dependency_exprs,
                        full_reload_dependency_exprs,
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                return self.construct_formula(
                    trans,
                    Some(datasource),
                    parsed_formula,
                    context
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                match &self.constructor_type {
                    SelectConstructorType::SelectMainConstructor { .. } => {
                        // Examine the schema of SCHEMA{report_oid}_LABEL_VIEW to see what filters are applicable to the report
                        let mut filtered_columns: Vec<(String, String)> = Vec::new();
                        let oid_regex = Regex::new(r"ROOT\d+(?:_MASTER\d+|_INHERITOR\d+|_COLUMN\d+)*_OID").unwrap();
                        let pragma_sql: String = format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)");

                        sql_map_then_iter(
                            trans, 
                            &pragma_sql, 
                            [], 
                            |row| row.get::<_, String>("NAME"), 
                            |oid_column_name| {
                                if oid_regex.is_match(&oid_column_name) {
                                    // Test if the OID is being selected in this view
                                    let filtered_datasource_alias: String = oid_column_name.replace("_OID", "");
                                    let modified_datasource: Datasource = Datasource::from_alias_transact(trans, filtered_datasource_alias)?
                                        .substitute_root(datasource.replace_root, datasource.datasource.clone());
                                    let modified_datasource_alias: String = modified_datasource.get_alias();

                                    if self.cte_datasource.contains_key(&modified_datasource_alias)
                                        && !self.cte_datasource[&modified_datasource_alias].is_always_collection {
                                        filtered_columns.push((
                                            oid_column_name,
                                            format!("w.{modified_datasource_alias}_OID")
                                        ));
                                    }
                                }
                                Ok(None::<()>)
                            }
                        )?;

                        // Construct the parameter
                        let value_expr: String = filtered_columns.iter()
                            .map(|(filtered_oid_ord, filtered_oid_value)| format!(
                                "'{}=' || CAST({} AS TEXT)",
                                sql_encode_string(&filtered_oid_ord),
                                filtered_oid_value
                            ))
                            .reduce(|acc, e| format!("({acc} || '&' || {e})"))
                            .unwrap_or(String::from("''"));
                        let json_label_expr: String = format!(
                            "NULLIF('[ ' || GROUP_CONCAT((SELECT l.JSON_LABEL FROM SCHEMA{report_oid}_LABEL_VIEW l {}), ', ') OVER ({}) || ' ]', '[  ]')",
                            if filtered_columns.len() > 0 {
                                format!(
                                    "WHERE {}",
                                    filtered_columns.iter().map(|(filtered_oid_ord, filtered_oid_value)| format!("l.{filtered_oid_ord} IS {filtered_oid_value}"))
                                        .reduce(|acc, e| format!("{acc} AND {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("")
                            },
                            if filtered_columns.len() > 0 {
                                format!(
                                    "PARTITION BY {}",
                                    filtered_columns.iter().map(|(_, filtered_oid_value)| filtered_oid_value.clone())
                                        .reduce(|acc, e| format!("{acc}, {e}"))
                                        .unwrap()
                                )
                            } else {
                                String::from("")
                            }
                        );
                        return Ok(SelectParameter {
                            plain_label_expr_norecursion: String::from("NULL"),
                            plain_label_expr_recursion: String::from("NULL"),
                            json_label_expr_norecursion: json_label_expr.clone(),
                            json_label_expr_recursion: json_label_expr,
                            value_expr_norecursion: value_expr.clone(),
                            value_expr_recursion: value_expr,
                            cell_expr,
                            isolated_dependency_exprs,
                            full_reload_dependency_exprs,
                            scalar_type: SelectParameterType::new(),
                            context
                        });
                    }
                    SelectConstructorType::SelectLabelConstructor { recursions, .. } => {
                        // Construct datasource for the columns of the subreport
                        let subreport_datasource: SelectParameterDatasource = datasource.clone();

                        // Insert all columns of the report as concrete parameters
                        let mut param_context: SelectParameterContext = SelectParameterContext::Collection { 
                            slice_norecursion: SelectParameterSlice::None, 
                            slice_recursion: SelectParameterSlice::None, 
                            filter_expr_norecursion: None, 
                            filter_expr_recursion: None, 
                            order_exprs_norecursion: Vec::new(), 
                            order_exprs_recursion: Vec::new(), 
                            min_depth: HashMap::new(), 
                            window_changes_disabled: true 
                        };
                        let param_context_ref: &mut SelectParameterContext = &mut param_context;

                        let mut params: HashMap<column::FullMetadata, SelectParameter> = HashMap::new();
                        sql_map_then_iter(
                            trans,
                            "
                            SELECT 
                                COLUMN_OID 
                            FROM METADATA_SCHEMA_COLUMN_VIEW 
                            WHERE SCHEMA_OID = ?1 
                                AND IS_REQUIRED 
                            ORDER BY IS_SUBREPORT ASC
                            ",
                            params![report_oid],
                            |row| row.get::<_, i64>("COLUMN_OID"),
                            |column_oid| {
                                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                                // Insert the parameter with no datasource
                                let param: SelectParameter = self.add_concrete_parameter(trans, subreport_datasource.clone(), column.clone(), param_context_ref.clone())?;
                                *param_context_ref = param.context.clone();
                                params.insert(column, param);
                                Ok(None::<()>)
                            }
                        )?;

                        // Order the columns by ordering
                        let mut ordered_params: Vec<(String, SelectParameter, i64)> = params.into_iter()
                            .filter(|(column_metadata, _)| column_metadata.is_primary_key)
                            .map(|(column_metadata, column_param)| (json_encode_string(&column_metadata.name), column_param, column_metadata.ordering))
                            .collect();
                        ordered_params.sort_by_key(|(_, _, ordering)| *ordering);

                        // Compile the label expressions
                        if ordered_params.len() == 0 {
                            return Ok(SelectParameter { 
                                plain_label_expr_norecursion: String::from("NULL"), 
                                plain_label_expr_recursion: String::from("NULL"), 
                                json_label_expr_norecursion: String::from("NULL"),  
                                json_label_expr_recursion: String::from("NULL"),
                                value_expr_norecursion: String::from("NULL"), 
                                value_expr_recursion: String::from("NULL"), 
                                cell_expr: String::from("NULL"), 
                                isolated_dependency_exprs: HashSet::new(),
                                full_reload_dependency_exprs: HashSet::new(), 
                                scalar_type: SelectParameterType::new(), 
                                context 
                            });
                        } else if ordered_params.len() == 1 {
                            let (agg_expr_norecursion, agg_expr_recursion) = param_context.wrap(
                                format!(
                                    "GROUP_CONCAT({}, ', ')",
                                    ordered_params.iter().map(|(_, param, _)| param.json_label_expr_norecursion.clone()).next().unwrap()
                                ),
                                format!(
                                    "GROUP_CONCAT({}, ', ')",
                                    ordered_params.iter().map(|(_, param, _)| param.json_label_expr_recursion.clone()).next().unwrap()
                                )
                            );
                            return Ok(SelectParameter { 
                                plain_label_expr_norecursion: String::from("NULL"), 
                                plain_label_expr_recursion: String::from("NULL"), 
                                json_label_expr_norecursion: format!("('[ ' || {agg_expr_norecursion} || ' ]')"),  
                                json_label_expr_recursion: format!("('[ ' || {agg_expr_recursion} || ' ]')"), 
                                value_expr_norecursion: String::from("NULL"), 
                                value_expr_recursion: String::from("NULL"), 
                                cell_expr: String::from("NULL"), 
                                isolated_dependency_exprs,
                                full_reload_dependency_exprs, 
                                scalar_type: SelectParameterType::new(), 
                                context 
                            });
                        } else {
                            let (agg_expr_norecursion, agg_expr_recursion) = param_context.wrap(
                                format!(
                                    "GROUP_CONCAT('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', ', ')",
                                    ordered_params.iter()
                                        .map(|(param_key, param, _)| format!("SELECT '\"{param_key}\": ' || {}", param.json_label_expr_norecursion))
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                ),
                                format!(
                                    "GROUP_CONCAT('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', ', ')",
                                    ordered_params.iter()
                                        .map(|(param_key, param, _)| format!("SELECT '\"{param_key}\": ' || {}", param.json_label_expr_recursion))
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                )
                            );
                            return Ok(SelectParameter { 
                                plain_label_expr_norecursion: String::from("NULL"), 
                                plain_label_expr_recursion: String::from("NULL"), 
                                json_label_expr_norecursion: format!("('[ ' || {agg_expr_norecursion} || ' ]')"),  
                                json_label_expr_recursion: format!("('[ ' || {agg_expr_recursion} || ' ]')"), 
                                value_expr_norecursion: String::from("NULL"), 
                                value_expr_recursion: String::from("NULL"), 
                                cell_expr: String::from("NULL"), 
                                isolated_dependency_exprs,
                                full_reload_dependency_exprs, 
                                scalar_type: SelectParameterType::new(), 
                                context 
                            });
                        }
                    }
                }
            }
        }
        return Err(Error::adhoc("Unable to add parameter."));
    }

    /// Adds a column on a report as a parameter to this SELECT statement.
    fn add_virtual_parameter(&mut self, trans: &Transaction, column: column::FullMetadata, context: SelectParameterContext) -> Result<SelectParameter, Error> {
        match column.column_type {
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                return self.construct_formula(
                    trans,
                    None,
                    parsed_formula,
                    context
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                // Insert all columns of the report as virtual parameters
                let mut param_context: SelectParameterContext = SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None, 
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None, 
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(), 
                    min_depth: HashMap::new(), 
                    window_changes_disabled: true 
                };
                let param_context_ref: &mut SelectParameterContext = &mut param_context;
                let mut params: HashMap<column::FullMetadata, SelectParameter> = HashMap::new();
                sql_map_then_iter(
                    trans, 
                    "
                    SELECT 
                        COLUMN_OID 
                    FROM METADATA_SCHEMA_COLUMN_VIEW 
                    WHERE SCHEMA_OID = ?1 
                        AND IS_REQUIRED 
                    ORDER BY IS_SUBREPORT ASC
                    ", 
                    params![report_oid], 
                    |row| row.get::<_, i64>("COLUMN_OID"), 
                    |column_oid| {
                        let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                        // Insert the parameter with no datasource
                        let param: SelectParameter = self.add_virtual_parameter(trans, column.clone(), param_context_ref.clone())?;
                        *param_context_ref = param.context.clone();
                        params.insert(column, param);
                        Ok(None::<()>)
                    }
                )?;

                // Order the columns by ordering
                let mut ordered_params: Vec<(String, SelectParameter, i64)> = params.into_iter()
                    .filter(|(column_metadata, _)| column_metadata.is_primary_key)
                    .map(|(column_metadata, column_param)| (json_encode_string(&column_metadata.name), column_param, column_metadata.ordering))
                    .collect();
                ordered_params.sort_by_key(|(_, _, ordering)| *ordering);

                // Compile the label expressions
                if ordered_params.len() == 0 {
                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"), 
                        plain_label_expr_recursion: String::from("NULL"), 
                        json_label_expr_norecursion: String::from("NULL"),  
                        json_label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"), 
                        value_expr_recursion: String::from("NULL"), 
                        cell_expr: String::from("NULL"), 
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                } else if ordered_params.len() == 1 {
                    let (agg_expr_norecursion, agg_expr_recursion) = param_context.wrap(
                        format!(
                            "GROUP_CONCAT({}, ', ')",
                            ordered_params.iter().map(|(_, param, _)| param.json_label_expr_norecursion.clone()).next().unwrap()
                        ),
                        format!(
                            "GROUP_CONCAT({}, ', ')",
                            ordered_params.iter().map(|(_, param, _)| param.json_label_expr_recursion.clone()).next().unwrap()
                        )
                    );
                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"), 
                        plain_label_expr_recursion: String::from("NULL"), 
                        json_label_expr_norecursion: format!("('[ ' || {agg_expr_norecursion} || ' ]')"),  
                        json_label_expr_recursion: format!("('[ ' || {agg_expr_recursion} || ' ]')"), 
                        value_expr_norecursion: String::from("NULL"), 
                        value_expr_recursion: String::from("NULL"), 
                        cell_expr: String::from("NULL"), 
                        isolated_dependency_exprs: ordered_params.iter()
                            .fold(HashSet::new(), |acc, (_, param, _)| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()), 
                        full_reload_dependency_exprs: ordered_params.iter()
                            .fold(HashSet::new(), |acc, (_, param, _)| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()), 
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                } else {
                    let (agg_expr_norecursion, agg_expr_recursion) = param_context.wrap(
                        format!(
                            "GROUP_CONCAT('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', ', ')",
                            ordered_params.iter()
                                .map(|(param_key, param, _)| format!("SELECT '\"{param_key}\": ' || {}", param.json_label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        ),
                        format!(
                            "GROUP_CONCAT('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', ', ')",
                            ordered_params.iter()
                                .map(|(param_key, param, _)| format!("SELECT '\"{param_key}\": ' || {}", param.json_label_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    );
                    return Ok(SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"), 
                        plain_label_expr_recursion: String::from("NULL"), 
                        json_label_expr_norecursion: format!("('[ ' || {agg_expr_norecursion} || ' ]')"),  
                        json_label_expr_recursion: format!("('[ ' || {agg_expr_recursion} || ' ]')"), 
                        value_expr_norecursion: String::from("NULL"), 
                        value_expr_recursion: String::from("NULL"), 
                        cell_expr: String::from("NULL"), 
                        isolated_dependency_exprs: ordered_params.iter()
                            .fold(HashSet::new(), |acc, (_, param, _)| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()), 
                        full_reload_dependency_exprs: ordered_params.iter()
                            .fold(HashSet::new(), |acc, (_, param, _)| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()), 
                        scalar_type: SelectParameterType::new(), 
                        context 
                    });
                }
            }
            _ => {
                return Err(Error::adhoc("Unable to add virtual parameter - column type belongs to a table."));
            }
        }
    }


    /// Constructs a label for an Object column.
    /// The first item of the returned tuple is the non-recursive plain label. (Always NULL, since the label for an Object is always JSON.)
    /// The second item of the returned tuple is the recursive plain label. (Always NULL, since the label for an Object is always JSON.)
    /// The third item of the returned tuple is the non-recursive JSON label.
    /// The fourth item of the returned tuple is the recursive JSON label.
    fn construct_object_label(&mut self, trans: &Transaction, datasource: SelectParameterDatasource, object_column_oid: i64, object_table_oid: i64, value_expr: &String, is_collection: bool) -> Result<(String, String, String, String), Error> {
        match &mut self.constructor_type {
            SelectConstructorType::SelectMainConstructor { .. } => {
                // MAIN views are allowed to select the label from the LABEL view
                return Ok((
                    String::from("NULL"),
                    String::from("NULL"),
                    format!("(SELECT l.OBJECT_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})"),
                    format!("(SELECT l.OBJECT_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                ));
            }

            SelectConstructorType::SelectLabelConstructor { recursions, .. } => {
                //
                // First, we need to check if the label for this Object column would induce recursion
                // We do this by checking each parent datasource to see if it has the same table_oid as the table the Object column points to
                //
                
                for looped_datasource in datasource.datasource.linearize() {
                    let looped_datasource_schema_oid: i64 = looped_datasource.get_table_oid()?;
                    if looped_datasource_schema_oid == object_table_oid {
                        println!("    Located recursion in Object label at {}...", looped_datasource.get_alias());
                        //
                        // This meets the condition set above, so we have confirmed the Object column induces recursion in the label
                        // 

                        // First, we note where the recursion occurred, and where it should loop backwards to
                        let recursive_datasource: SelectParameterDatasource = SelectParameterDatasource::new_recursion(
                            looped_datasource, 
                            if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, object_table_oid)? {
                                oid
                            } else {
                                return Err(Error::adhoc("No default datasource for table."));
                            }, 
                            datasource.datasource.get_alias()
                        );
                        recursions.push((value_expr.clone(), recursive_datasource.get_oid_expr()));
                        println!("    Constructed recursion for {value_expr} = {}", recursive_datasource.get_oid_expr());
                        
                        // Add datasource for each inheritor table
                        sql_map_then_iter(
                            trans, 
                            "
                            SELECT 
                                INHERITOR_DATASOURCE_PATH 
                            FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW 
                            WHERE MASTER_SCHEMA_OID = ?1
                            ", 
                            params![object_table_oid], 
                            |row| row.get("INHERITOR_DATASOURCE_PATH"), 
                            |inheritor_datasource_path| {
                                println!("      Now adding datasource {}{inheritor_datasource_path}", recursive_datasource.datasource.get_alias());
                                self.add_datasource(
                                    recursive_datasource.datasource.append_path(inheritor_datasource_path)?, 
                                    is_collection
                                );
                                Ok(None::<()>)
                            }
                        )?;

                        // Construct labels for each key column on table referenced by Object, including non-required columns
                        let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                        sql_map_then_iter(
                            trans, 
                            "
                            SELECT 
                                COLUMN_OID, 
                                ORDERING 
                            FROM METADATA_SCHEMA_COLUMN_VIEW 
                            WHERE SCHEMA_OID = ?1 
                                AND IS_PRIMARY_KEY 
                            ORDER BY IS_SUBREPORT ASC
                            ", 
                            params![object_table_oid], 
                            |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)), 
                            |(column_oid, ordering)| {
                                println!("    Now adding {}.{}_COLUMN{column_oid} to the query...", recursive_datasource.alias, recursive_datasource.datasource.get_alias());
                                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                                let json_safe_column_name: String = json_encode_string(&column.name);
                                
                                let param = self.add_concrete_parameter(trans, recursive_datasource.clone(), column, SelectParameterContext::Scalar)?;
                                key_columns.push((json_safe_column_name, param, ordering));
                                Ok(None::<()>)
                            }
                        )?;
                        key_columns.sort_by_key(|(_, _, ordering)| *ordering);

                        // Construct the Object label
                        return Ok((
                            String::from("NULL"),
                            String::from("NULL"),
                            
                            // Non-recursive JSON label
                            if key_columns.len() == 1 {
                                format!(
                                    "IF({value_expr} IS NOT NULL, 'null', NULL)"
                                )
                            } else {
                                format!(
                                    "IF({value_expr} IS NOT NULL, '{{ }}', NULL)"
                                )
                            },

                            // Recursive JSON label
                            if key_columns.len() == 1 {
                                format!(
                                    "
                                    ('{{ \"' 
                                        || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                        || '\": ' 
                                        || COALESCE({}, 'null') 
                                        || ' }}')
                                    ",

                                    // The OID of the schema
                                    recursive_datasource.get_schema_expr(),

                                    // The key columns of the schema
                                    key_columns.iter()
                                        .map(|(_, param, _)| param.json_label_expr_recursion.clone())
                                        .next()
                                        .unwrap()
                                )
                            } else {
                                format!(
                                    "
                                    ('{{ \"' 
                                        || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                        || '\": ' 
                                        || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                                        || ' }}')
                                    ",

                                    // The OID of the schema
                                    recursive_datasource.get_schema_expr(),

                                    // The key columns of the schema
                                    key_columns.iter()
                                        .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_recursion))
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                )
                            }
                        ));
                    }
                }

                //
                // We have now confirmed that the Object column does not induce recursion.
                // To construct the label for the Object column, we follow a similar procedure to the above
                //

                println!("    Object label determined to be non-recursive.");
                let object_datasource = SelectParameterDatasource {
                    datasource: datasource.datasource.append_path(format!("_COLUMN{object_column_oid}"))?,
                    replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, object_table_oid)? {
                        oid
                    } else {
                        return Err(Error::adhoc("No default datasource for table."));
                    },
                    alias: datasource.alias 
                };

                // Add datasource for each inheritor table
                sql_map_then_iter(
                    trans, 
                    "
                    SELECT 
                        INHERITOR_DATASOURCE_PATH 
                    FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW 
                    WHERE MASTER_SCHEMA_OID = ?1
                    ", 
                    params![object_table_oid], 
                    |row| row.get("INHERITOR_DATASOURCE_PATH"), 
                    |inheritor_datasource_path| {
                        println!("      Now adding datasource {}{inheritor_datasource_path}", object_datasource.datasource.get_alias());
                        self.add_datasource(
                            datasource.datasource.append_path(inheritor_datasource_path)?, 
                            is_collection
                        );
                        Ok(None::<()>)
                    }
                )?;

                // Construct labels for each key column on table referenced by Object, including non-required columns
                let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                sql_map_then_iter(
                    trans, 
                    "
                    SELECT 
                        COLUMN_OID, 
                        ORDERING 
                    FROM METADATA_SCHEMA_COLUMN_VIEW 
                    WHERE SCHEMA_OID = ?1 
                        AND IS_PRIMARY_KEY 
                    ORDER BY IS_SUBREPORT ASC
                    ", 
                    params![object_table_oid], 
                    |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)), 
                    |(column_oid, ordering)| {
                        println!("    Now adding {}.{}_COLUMN{column_oid} to the query...", object_datasource.alias, object_datasource.datasource.get_alias());
                        let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                        let json_safe_column_name: String = json_encode_string(&column.name);
                                
                        let param = self.add_concrete_parameter(trans, object_datasource.clone(), column, SelectParameterContext::Scalar)?;
                        key_columns.push((json_safe_column_name, param, ordering));
                        Ok(None::<()>)
                    }
                )?;
                key_columns.sort_by_key(|(_, _, ordering)| *ordering);

                // Construct the Object label
                return Ok((
                    String::from("NULL"),
                    String::from("NULL"),
                            
                    // Non-recursive JSON label
                    if key_columns.len() == 1 {
                        format!(
                            "
                            ('{{ \"' 
                                || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                || '\": ' 
                                || COALESCE({}, 'null') 
                                || ' }}')
                            ",

                            // The OID of the schema
                            object_datasource.get_schema_expr(),

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(_, param, _)| param.json_label_expr_norecursion.clone())
                                .next()
                                .unwrap()
                        )
                    } else {
                        format!(
                            "
                            ('{{ \"' 
                                || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                || '\": ' 
                                || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                                || ' }}')
                            ",

                            // The OID of the schema
                            object_datasource.get_schema_expr(),

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    },

                    // Recursive JSON label
                    if key_columns.len() == 1 {
                        format!(
                            "
                            ('{{ \"' 
                                || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                || '\": ' 
                                || COALESCE({}, 'null') 
                                || ' }}')
                            ",

                            // The OID of the schema
                            object_datasource.get_schema_expr(),

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(_, param, _)| param.json_label_expr_recursion.clone())
                                .next()
                                .unwrap()
                        )
                    } else {
                        format!(
                            "
                            ('{{ \"' 
                                || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) 
                                || '\": ' 
                                || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                                || ' }}')
                            ",

                            // The OID of the schema
                            object_datasource.get_schema_expr(),

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    }
                ));
            }
        }
    }

    /// Constructs a label for a Select or Multiselect column.
    /// The first item of the returned tuple is the non-recursive plain label.
    /// The second item of the returned tuple is the recursive plain label.
    /// The third item of the returned tuple is the non-recursive JSON label.
    /// The fourth item of the returned tuple is the recursive JSON label.
    fn construct_select_label(&mut self, trans: &Transaction, datasource: SelectParameterDatasource, object_column_oid: i64, object_table_oid: i64, value_expr: &String, is_collection: bool) -> Result<(String, String, String, String), Error> {
        match &mut self.constructor_type {
            SelectConstructorType::SelectMainConstructor { .. } => {
                // MAIN views are allowed to select the label from the LABEL view
                return Ok((
                    format!("(SELECT l.PLAIN_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})"),
                    format!("(SELECT l.PLAIN_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})"),
                    format!("(SELECT l.JSON_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})"),
                    format!("(SELECT l.JSON_LABEL FROM SCHEMA{object_table_oid}_LABEL_VIEW l WHERE l.OID = {value_expr})")
                ));
            }

            SelectConstructorType::SelectLabelConstructor { recursions, .. } => {
                //
                // First, we need to check if the label for this Object column would induce recursion
                // We do this by checking each parent datasource to see if it has the same table_oid as the table the Object column points to
                //
                
                for looped_datasource in datasource.datasource.linearize() {
                    let looped_datasource_schema_oid: i64 = looped_datasource.get_table_oid()?;
                    if looped_datasource_schema_oid == object_table_oid {
                        //
                        // This meets the condition set above, so we have confirmed the Select/Multiselect column induces recursion in the label
                        // 

                        // First, we note where the recursion occurred, and where it should loop backwards to
                        let recursive_datasource: SelectParameterDatasource = SelectParameterDatasource::new_recursion(
                            looped_datasource, 
                            if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, object_table_oid)? {
                                oid
                            } else {
                                return Err(Error::adhoc("No default datasource for table."));
                            }, 
                            datasource.datasource.get_alias()
                        );
                        recursions.push((value_expr.clone(), recursive_datasource.get_oid_expr()));

                        // Construct labels for each key column on table referenced by Object, including non-required columns
                        let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                        sql_map_then_iter(
                            trans,
                            "
                            SELECT 
                                COLUMN_OID, 
                                ORDERING 
                            FROM METADATA_SCHEMA_COLUMN_VIEW 
                            WHERE SCHEMA_OID = ?1 
                                AND IS_PRIMARY_KEY 
                            ORDER BY IS_SUBREPORT ASC
                            ",
                            params![object_table_oid],
                            |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)),
                            |(column_oid, ordering)| {
                                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                                let json_safe_column_name: String = json_encode_string(&column.name);
                                
                                let param = self.add_concrete_parameter(trans, recursive_datasource.clone(), column, SelectParameterContext::Scalar)?;
                                key_columns.push((json_safe_column_name, param, ordering));
                                Ok(None::<()>)
                            }
                        )?;
                        key_columns.sort_by_key(|(_, _, ordering)| *ordering);

                        // Construct the Select label
                        return Ok((
                            // Non-recursive plain label
                            String::from("NULL"),

                            // Recursive plain label
                            if key_columns.len() == 1 {
                                key_columns.iter()
                                    .map(|(_, param, _)| param.plain_label_expr_recursion.clone())
                                    .next()
                                    .unwrap()
                            } else {
                                String::from("NULL")
                            },
                            
                            // Non-recursive JSON label
                            if key_columns.len() == 1 {
                                format!(
                                    "IF({value_expr} IS NOT NULL, 'null', NULL)"
                                )
                            } else {
                                format!(
                                    "IF({value_expr} IS NOT NULL, '{{ }}', NULL)"
                                )
                            },

                            // Recursive JSON label
                            if key_columns.len() == 1 {
                                format!(
                                    "
                                    COALESCE({}, 'null') 
                                    ",

                                    // The key columns of the schema
                                    key_columns.iter()
                                        .map(|(_, param, _)| param.json_label_expr_recursion.clone())
                                        .next()
                                        .unwrap()
                                )
                            } else {
                                format!(
                                    "
                                    COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                                    ",

                                    // The key columns of the schema
                                    key_columns.iter()
                                        .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_recursion))
                                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                        .unwrap()
                                )
                            }
                        ));
                    }
                }

                //
                // We have now confirmed that the Object column does not induce recursion.
                // To construct the label for the Object column, we follow a similar procedure to the above
                // 

                let object_datasource = SelectParameterDatasource {
                    datasource: datasource.datasource.append_path(format!("_COLUMN{object_column_oid}"))?,
                    replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, object_table_oid)? {
                        oid
                    } else {
                        return Err(Error::adhoc("No default datasource for table."));
                    },
                    alias: datasource.alias 
                };

                // Add datasource for each inheritor table
                sql_map_then_iter(
                    trans,
                    "
                    SELECT 
                        INHERITOR_DATASOURCE_PATH 
                    FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW 
                    WHERE MASTER_SCHEMA_OID = ?1
                    ",
                    params![object_table_oid],
                    |row| row.get("INHERITOR_DATASOURCE_PATH"),
                    |inheritor_datasource_path| {
                        self.add_datasource(
                            datasource.datasource.append_path(inheritor_datasource_path)?, 
                            is_collection
                        );
                        Ok(None::<()>)
                    }
                )?;

                // Construct labels for each key column on table referenced by Object, including non-required columns
                let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                sql_map_then_iter(
                    trans,
                    "
                    SELECT 
                        COLUMN_OID, 
                        ORDERING 
                    FROM METADATA_SCHEMA_COLUMN_VIEW 
                    WHERE SCHEMA_OID = ?1 
                        AND IS_PRIMARY_KEY 
                    ORDER BY IS_SUBREPORT ASC
                    ",
                    params![object_table_oid],
                    |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)),
                    |(column_oid, ordering)| {
                        let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                        let json_safe_column_name: String = json_encode_string(&column.name);
                                
                        let param = self.add_concrete_parameter(trans, object_datasource.clone(), column, SelectParameterContext::Scalar)?;
                        key_columns.push((json_safe_column_name, param, ordering));
                        Ok(None::<()>)
                    }
                )?;
                key_columns.sort_by_key(|(_, _, ordering)| *ordering);

                // Construct the Object label
                return Ok((
                    // Non-recursive plain label
                    if key_columns.len() == 1 {
                        key_columns.iter()
                            .map(|(_, param, _)| param.plain_label_expr_norecursion.clone())
                            .next()
                            .unwrap()
                    } else {
                        String::from("NULL")
                    },

                    // Recursive plain label
                    if key_columns.len() == 1 {
                        key_columns.iter()
                            .map(|(_, param, _)| param.plain_label_expr_recursion.clone())
                            .next()
                            .unwrap()
                    } else {
                        String::from("NULL")
                    },
                            
                    // Non-recursive JSON label
                    if key_columns.len() == 1 {
                        format!(
                            "
                            COALESCE({}, 'null') 
                            ",

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(_, param, _)| param.json_label_expr_norecursion.clone())
                                .next()
                                .unwrap()
                        )
                    } else {
                        format!(
                            "
                            COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                            ",

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    },

                    // Recursive JSON label
                    if key_columns.len() == 1 {
                        format!(
                            "
                            COALESCE({}, 'null') 
                            ",

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(_, param, _)| param.json_label_expr_recursion.clone())
                                .next()
                                .unwrap()
                        )
                    } else {
                        format!(
                            "
                            COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') 
                            ",

                            // The key columns of the schema
                            key_columns.iter()
                                .map(|(json_safe_column_name, param, _)| format!("SELECT '\"{json_safe_column_name}\": ' || {}", param.json_label_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    }
                ));
            }
        }
    }

    /// Constructs the SQL expression corresponding to a Formula object.
    fn construct_formula(&mut self, trans: &Transaction, datasource: Option<SelectParameterDatasource>, formula: Box<Formula>, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
        Ok(match *formula {
            Formula::Null => {
                SelectParameter { 
                    plain_label_expr_norecursion: String::from("NULL"),
                    plain_label_expr_recursion: String::from("NULL"),
                    json_label_expr_norecursion: String::from("NULL"),
                    json_label_expr_recursion: String::from("NULL"),
                    value_expr_norecursion: String::from("NULL"),
                    value_expr_recursion: String::from("NULL"),
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: HashSet::new(),
                    full_reload_dependency_exprs: HashSet::new(),
                    scalar_type: SelectParameterType::new(),
                    context
                }
            }
            Formula::LiteralBool(value) => {
                if value {
                    let label_expr: String = format!("'true'");
                    let value_expr: String = format!("TRUE");
                    SelectParameter { 
                        plain_label_expr_norecursion: label_expr.clone(),
                        plain_label_expr_recursion: label_expr.clone(), 
                        json_label_expr_norecursion: label_expr.clone(),
                        json_label_expr_recursion: label_expr, 
                        value_expr_norecursion: value_expr.clone(),
                        value_expr_recursion: value_expr,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Boolean),
                        context
                    }
                } else {
                    let label_expr: String = format!("'false'");
                    let value_expr: String = format!("FALSE");
                    SelectParameter { 
                        plain_label_expr_norecursion: label_expr.clone(),
                        plain_label_expr_recursion: label_expr.clone(), 
                        json_label_expr_norecursion: label_expr.clone(),
                        json_label_expr_recursion: label_expr, 
                        value_expr_norecursion: value_expr.clone(),
                        value_expr_recursion: value_expr,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Boolean),
                        context
                    }
                }
            }
            Formula::LiteralFloat(value) => {
                let label_expr: String = format!("'{value}'");
                let value_expr: String = format!("{value}");
                SelectParameter { 
                    plain_label_expr_norecursion: label_expr.clone(),
                    plain_label_expr_recursion: label_expr.clone(), 
                    json_label_expr_norecursion: label_expr.clone(),
                    json_label_expr_recursion: label_expr, 
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: HashSet::new(),
                    full_reload_dependency_exprs: HashSet::new(),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Number),
                    context
                }
            }
            Formula::LiteralInt(value) => {
                let label_expr: String = format!("'{value}'");
                let value_expr: String = format!("{value}");
                SelectParameter { 
                    plain_label_expr_norecursion: label_expr.clone(),
                    plain_label_expr_recursion: label_expr.clone(), 
                    json_label_expr_norecursion: label_expr.clone(),
                    json_label_expr_recursion: label_expr, 
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: HashSet::new(),
                    full_reload_dependency_exprs: HashSet::new(),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Integer),
                    context
                }
            }
            Formula::LiteralString(value) => {
                let value_expr: String = format!("'{}'", sql_encode_string(&value));
                let json_label_expr: String = json_encode_string(&value);
                SelectParameter {
                    plain_label_expr_norecursion: value_expr.clone(),
                    plain_label_expr_recursion: value_expr.clone(), 
                    json_label_expr_norecursion: json_label_expr.clone(),
                    json_label_expr_recursion: json_label_expr, 
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: HashSet::new(),
                    full_reload_dependency_exprs: HashSet::new(),
                    scalar_type: SelectParameterType::from(column_type::Primitive::PlainText),
                    context
                }
            }
            
            Formula::Abs(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("ABS({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("ABS({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of ABS(x: Number)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Ceiling(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("CEILING({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("CEILING({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of CEILING(x: Number)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Floor(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("FLOOR({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("FLOOR({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of FLOOR(x: Number)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Length(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("LENGTH({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("LENGTH({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of LENGTH(x: Text)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Lowercase(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("LOWER({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("LOWER({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of LOWER(x: Text)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Not(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                    let value_expr_norecursion: String = format!("(NOT {})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("(NOT {})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of NOT(x: Boolean)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Round(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("ROUND({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("ROUND({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of ROUND(x: Number)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Sign(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("SIGN({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("SIGN({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of SIGN(x: Number)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Uppercase(inner) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("UPPER({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("UPPER({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: inner_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: inner_param.full_reload_dependency_exprs,
                        scalar_type,
                        context: inner_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of UPPER(x: Text)", 
                        inner_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: inner_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Wrap(inner) => {
                self.construct_formula(trans, datasource, inner, context)?
            }
            
            Formula::Add(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} + {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} + {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of ADD(lhs: Number, rhs: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of ADD(lhs: Number, rhs: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::And(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} AND {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} AND {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of AND(lhs: Boolean, rhs: Boolean)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of AND(lhs: Boolean, rhs: Boolean)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Concat(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                        let value_expr_norecursion: String = format!("({} || {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} || {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of CONCAT(lhs: Text, rhs: Text)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of CONCAT(lhs: Text, rhs: Text)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Divide(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Number);
                        let value_expr_norecursion: String = format!("({} / {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} / {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument denominator of DIVIDE(numerator: Number, denominator: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument numerator of DIVIDE(numerator: Number, denominator: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Eq(lhs, rhs) => {
                context.disable_window_changes();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;

                let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let value_expr_norecursion: String = format!("({} IS {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                let value_expr_recursion: String = format!("({} IS {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                SelectParameter {
                    plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                    json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                    scalar_type,
                    context: rhs_param.context
                }
            }
            Formula::Exponent(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("POW({}, {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("POW({}, {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument exponent of POW(base: Number, exponent: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument base of POW(base: Number, exponent: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::LessThan(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} < {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} < {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of LESSTHAN(lhs: Number, rhs: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of LESSTHAN(lhs: Number, rhs: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::LessThanOrEq(lhs, rhs) => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} <= {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} <= {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of LESSTHANEQUALTO(lhs: Number, rhs: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of LESSTHANEQUALTO(lhs: Number, rhs: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Modulo(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} % {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} % {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument modulus of MODULO(numerator: Number, modulus: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument numerator of MODULO(numerator: Number, modulus: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Multiply(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} * {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} * {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of MULTIPLY(lhs: Number, rhs: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of MULTIPLY(lhs: Number, rhs: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Or(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} OR {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} OR {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of OR(lhs: Boolean, rhs: Boolean)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of OR(lhs: Boolean, rhs: Boolean)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Subtract(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} - {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} - {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                            full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                            scalar_type,
                            context: rhs_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument rhs of SUBTRACT(lhs: Number, rhs: Number)", 
                            inner_name: rhs_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: rhs_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument lhs of SUBTRACT(lhs: Number, rhs: Number)", 
                        inner_name: lhs_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: lhs_param.scalar_type.to_string()
                    });
                }
            }
            
            Formula::Argmax(inners) => {
                context.disable_window_changes();
                let mut params: Vec<SelectParameter> = Vec::new();
                let mut scalar_type: SelectParameterType = SelectParameterType::new();
                for inner in inners {
                    let inner_param = self.construct_formula(trans, datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"),
                        plain_label_expr_recursion: String::from("NULL"),
                        json_label_expr_norecursion: String::from("NULL"),
                        json_label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    let value_expr_norecursion: String = format!(
                        "MAX({})",
                        params.iter().map(|param| param.value_expr_norecursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                    );
                    let value_expr_recursion: String = format!(
                        "MAX({})",
                        params.iter().map(|param| param.value_expr_recursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr != "NULL" {
                                // For each argument that is potentially associated with a cell, build a WHEN clause that checks if the value is the maximum of all parameters
                                match params.iter().enumerate().filter_map(|(param_rhs_idx, param_rhs)| {
                                    if param_lhs.value_expr_norecursion != param_rhs.value_expr_norecursion {
                                        Some(format!(
                                            "({} {} {})", 
                                            param_lhs.value_expr_norecursion, 
                                            if param_lhs_idx < param_rhs_idx { ">=" } else { ">" }, 
                                            param_rhs.value_expr_norecursion
                                        ))
                                    } else {
                                        None
                                    }
                                }).reduce(|acc, e| format!("{acc} AND {e}")) {
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        isolated_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()),
                        full_reload_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()),
                        scalar_type,
                        context
                    }
                }
            }
            Formula::Argmin(inners) => {
                context.disable_window_changes();
                let mut params: Vec<SelectParameter> = Vec::new();
                let mut scalar_type: SelectParameterType = SelectParameterType::new();
                for inner in inners {
                    let inner_param = self.construct_formula(trans, datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"),
                        plain_label_expr_recursion: String::from("NULL"),
                        json_label_expr_norecursion: String::from("NULL"),
                        json_label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    let value_expr_norecursion: String = format!(
                        "MIN({})",
                        params.iter().map(|param| param.value_expr_norecursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                    );
                    let value_expr_recursion: String = format!(
                        "MIN({})",
                        params.iter().map(|param| param.value_expr_recursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr != "NULL" {
                                // For each argument that is potentially associated with a cell, build a WHEN clause that checks if the value is the maximum of all parameters
                                match params.iter().enumerate().filter_map(|(param_rhs_idx, param_rhs)| {
                                    if param_lhs.value_expr_norecursion != param_rhs.value_expr_norecursion {
                                        Some(format!(
                                            "({} {} {})", 
                                            param_lhs.value_expr_norecursion, 
                                            if param_lhs_idx < param_rhs_idx { "<=" } else { "<" }, 
                                            param_rhs.value_expr_norecursion
                                        ))
                                    } else {
                                        None
                                    }
                                }).reduce(|acc, e| format!("{acc} AND {e}")) {
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        isolated_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()),
                        full_reload_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()),
                        scalar_type,
                        context
                    }
                }
            }
            Formula::Coalesce(inners) => {
                context.disable_window_changes();
                let mut params: Vec<SelectParameter> = Vec::new();
                let mut scalar_type: SelectParameterType = SelectParameterType::new();
                for inner in inners {
                    let inner_param = self.construct_formula(trans, datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"),
                        plain_label_expr_recursion: String::from("NULL"),
                        json_label_expr_norecursion: String::from("NULL"),
                        json_label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    SelectParameter {
                        plain_label_expr_norecursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_norecursion,
                                param.plain_label_expr_norecursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        plain_label_expr_recursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_recursion,
                                param.plain_label_expr_recursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        json_label_expr_norecursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_norecursion,
                                param.json_label_expr_norecursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        json_label_expr_recursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_recursion,
                                param.json_label_expr_recursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        value_expr_norecursion: format!(
                            "COALESCE({})",
                            params.iter().map(|param| param.value_expr_norecursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                        ),
                        value_expr_recursion: format!(
                            "COALESCE({})",
                            params.iter().map(|param| param.value_expr_recursion.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                        ),
                        cell_expr: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_norecursion,
                                param.cell_expr
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        isolated_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()),
                        full_reload_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()),
                        scalar_type,
                        context
                    }
                }
            }
            Formula::LiteralArray(inners) => {
                // Make sure the context expects a collection
                if let SelectParameterContext::Scalar = context {
                    return Err(Error::adhoc("A literal List cannot be returned in a scalar context!"));
                }
                
                context.disable_window_changes();
                let mut params: Vec<SelectParameter> = Vec::new();
                let mut scalar_type: SelectParameterType = SelectParameterType::new();
                for inner in inners {
                    let inner_param = self.construct_formula(trans, datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        plain_label_expr_norecursion: String::from("NULL"),
                        plain_label_expr_recursion: String::from("NULL"),
                        json_label_expr_norecursion: String::from("NULL"),
                        json_label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: HashSet::new(),
                        full_reload_dependency_exprs: HashSet::new(),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    SelectParameter {
                        plain_label_expr_norecursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.plain_label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        plain_label_expr_recursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.plain_label_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        json_label_expr_norecursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.json_label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        json_label_expr_recursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.json_label_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        value_expr_norecursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.value_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        value_expr_recursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.value_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        cell_expr: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.cell_expr))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        isolated_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()),
                        full_reload_dependency_exprs: params.iter()
                            .fold(HashSet::new(), |acc, param| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()),
                        scalar_type,
                        context
                    }
                }
            }
            
            Formula::Average(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    let scalar_type = collection_param.scalar_type;
                    let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                        format!("AVG({})", collection_param.value_expr_norecursion),
                        format!("AVG({})", collection_param.value_expr_recursion)
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                        scalar_type,
                        context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of AVERAGE(x: List<Number>)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Count(collection) => {
                let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                    format!("COUNT({})", collection_param.value_expr_norecursion),
                    format!("COUNT({})", collection_param.value_expr_recursion)
                );
                SelectParameter {
                    plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                    json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                    full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                    scalar_type,
                    context
                }
            }
            Formula::Join { collection, delimiter } => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, datasource.clone(), collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    let delimiter_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                    let delimiter_name: String = delimiter.to_string();
                    let delimiter_param = self.construct_formula(trans, datasource, delimiter, context)?;
                    if delimiter_expected_type.encompasses(&delimiter_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                        let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                            format!("GROUP_CONCAT({}, {})", collection_param.value_expr_norecursion, delimiter_param.value_expr_norecursion),
                            format!("GROUP_CONCAT({}, {})", collection_param.value_expr_recursion, delimiter_param.value_expr_recursion)
                        );
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                            full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                            scalar_type,
                            context: delimiter_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument delimiter of JOIN(collection: List<Text>, delimiter: Text)", 
                            inner_name: delimiter_name,
                            expected_type: delimiter_expected_type.to_string(), 
                            received_type: delimiter_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument collection of JOIN(collection: List<Text>, delimiter: Text)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Max(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number).generalize(&SelectParameterType::from(column_type::Primitive::PlainText));
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    let scalar_type = collection_param.scalar_type;
                    let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                        format!("MAX({})", collection_param.value_expr_norecursion),
                        format!("MAX({})", collection_param.value_expr_recursion)
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                        scalar_type,
                        context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of MAX(x: List<Number | Text>)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Min(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number).generalize(&SelectParameterType::from(column_type::Primitive::PlainText));
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    let scalar_type = collection_param.scalar_type;
                    let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                        format!("MIN({})", collection_param.value_expr_norecursion),
                        format!("MIN({})", collection_param.value_expr_recursion)
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                        scalar_type,
                        context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of MIN(x: List<Number | Text>)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Sum(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                    slice_norecursion: SelectParameterSlice::None, 
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None, 
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(), 
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(), 
                    window_changes_disabled: false 
                })?;
                if collection_expected_type.encompasses(&collection_param.scalar_type) {
                    let scalar_type = collection_param.scalar_type;
                    let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                        format!("SUM({})", collection_param.value_expr_norecursion),
                        format!("SUM({})", collection_param.value_expr_recursion)
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: collection_param.isolated_dependency_exprs,
                        full_reload_dependency_exprs: collection_param.full_reload_dependency_exprs,
                        scalar_type,
                        context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument x of SUM(x: List<Number>)", 
                        inner_name: collection_name,
                        expected_type: collection_expected_type.to_string(), 
                        received_type: collection_param.scalar_type.to_string()
                    });
                }
            }
            
            Formula::RandomInt => {
                context.disable_window_changes();
                self.random_values += 1;

                let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                let value_expr: String = format!("w.RANDOM{}", self.random_values);
                let plain_label_expr: String = scalar_type.construct_plain_label_expr(&value_expr);
                let json_label_expr: String = scalar_type.construct_json_label_expr(&value_expr);
                SelectParameter {
                    plain_label_expr_norecursion: plain_label_expr.clone(),
                    plain_label_expr_recursion: plain_label_expr,
                    json_label_expr_norecursion: json_label_expr.clone(),
                    json_label_expr_recursion: json_label_expr,
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: HashSet::new(),
                    full_reload_dependency_exprs: HashSet::new(),
                    scalar_type,
                    context
                }
            }
            Formula::Param { datasource_alias, column_oid } => {
                context.disable_window_changes();
                let column_datasource: SelectParameterDatasource = match datasource {
                    Some(datasource) => { // Formula belongs to a table
                        datasource.branch_norecursion()
                        let column_datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?
                            .substitute_root(datasource.replace_root, datasource.datasource);
                        SelectParameterDatasource {
                            replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, column_datasource.get_table_oid()?)? {
                                oid
                            } else {
                                return Err(Error::adhoc("No default datasource for table."));
                            },
                            datasource: column_datasource,
                            alias: datasource.alias
                        }
                    }

                    None => { // Formula belongs to a report
                        let column_datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?;
                        let column_datasource_schema_oid: i64 = column_datasource.get_table_oid()?;
                        // Since a parameter must belong to a table, we are assured that there is no recursion occurring at this stage
                        SelectParameterDatasource::new_norecursion(
                            column_datasource,
                            if let Some(Datasource::Table { oid, .. }) = Datasource::check_default_datasource_transact(trans, column_datasource_schema_oid)? {
                                oid
                            } else {
                                return Err(Error::adhoc("No default datasource for table."));
                            }
                        )
                    }
                };
                
                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                self.add_concrete_parameter(trans, column_datasource, column, context)?
            }
            
            Formula::Conditional { condition, formula_if_true, formula_if_false } => {
                context.disable_window_changes();
                let condition_expected_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let condition_name: String = condition.to_string();
                let condition_param = self.construct_formula(trans, datasource.clone(), condition, context)?;
                if condition_expected_type.encompasses(&condition_param.scalar_type) {
                    let if_true_param = self.construct_formula(trans, datasource.clone(), formula_if_true, condition_param.context)?;
                    let if_false_param = self.construct_formula(trans, datasource, formula_if_false, if_true_param.context)?;

                    let scalar_type = if_true_param.scalar_type.generalize(&if_false_param.scalar_type);
                    SelectParameter {
                        plain_label_expr_norecursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.plain_label_expr_norecursion, 
                            if_false_param.plain_label_expr_norecursion
                        ),
                        plain_label_expr_recursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_recursion, 
                            if_true_param.plain_label_expr_recursion, 
                            if_false_param.plain_label_expr_recursion
                        ),
                        json_label_expr_norecursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.json_label_expr_norecursion, 
                            if_false_param.json_label_expr_norecursion
                        ),
                        json_label_expr_recursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_recursion, 
                            if_true_param.json_label_expr_recursion, 
                            if_false_param.json_label_expr_recursion
                        ),
                        value_expr_norecursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.value_expr_norecursion, 
                            if_false_param.value_expr_norecursion
                        ),
                        value_expr_recursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_recursion, 
                            if_true_param.value_expr_recursion, 
                            if_false_param.value_expr_recursion
                        ),
                        cell_expr: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.cell_expr, 
                            if_false_param.cell_expr
                        ),
                        isolated_dependency_exprs: condition_param.isolated_dependency_exprs
                            .union(&if_true_param.isolated_dependency_exprs)
                            .map(|e| e.clone())
                            .collect::<HashSet<String>>()
                            .union(&if_false_param.isolated_dependency_exprs)
                            .map(|e| e.clone())
                            .collect(),
                        full_reload_dependency_exprs: condition_param.full_reload_dependency_exprs
                            .union(&if_true_param.full_reload_dependency_exprs)
                            .map(|e| e.clone())
                            .collect::<HashSet<String>>()
                            .union(&if_false_param.full_reload_dependency_exprs)
                            .map(|e| e.clone())
                            .collect(),
                        scalar_type,
                        context: if_false_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument condition of IF(condition: Boolean, ifTrue: Any, ifFalse: Any)", 
                        inner_name: condition_name,
                        expected_type: condition_expected_type.to_string(), 
                        received_type: condition_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Format { format: format_str, format_params } => {
                context.disable_window_changes();
                let format_str_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let format_str_name: String = format_str.to_string();
                let format_str_param = self.construct_formula(trans, datasource.clone(), format_str, context)?;
                if format_str_expected_type.encompasses(&format_str_param.scalar_type) {
                    context = format_str_param.context;

                    let mut params: Vec<SelectParameter> = Vec::new();
                    for inner in format_params {
                        let inner_param = self.construct_formula(trans, datasource.clone(), Box::new(inner), context)?;
                        context = inner_param.context.clone();
                        params.push(inner_param);
                    }

                    let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                    let value_expr_norecursion: String = format!(
                        "FORMAT({})",
                        params.iter().map(|param| param.value_expr_norecursion.clone())
                            .fold(
                                format_str_param.value_expr_norecursion,
                                |acc, e| format!("{acc}, {e}")
                            )
                    );
                    let value_expr_recursion: String = format!(
                        "FORMAT({})",
                        params.iter().map(|param| param.value_expr_recursion.clone())
                            .fold(
                                format_str_param.value_expr_recursion,
                                |acc, e| format!("{acc}, {e}")
                            )
                    );
                    SelectParameter {
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: params.iter()
                            .fold(format_str_param.isolated_dependency_exprs, |acc, param| acc.union(&param.isolated_dependency_exprs).map(|e| e.clone()).collect()),
                        full_reload_dependency_exprs: params.iter()
                            .fold(format_str_param.full_reload_dependency_exprs, |acc, param| acc.union(&param.full_reload_dependency_exprs).map(|e| e.clone()).collect()),
                        scalar_type,
                        context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument format of FORMAT(format: Text, ...args: Any)", 
                        inner_name: format_str_name,
                        expected_type: format_str_expected_type.to_string(), 
                        received_type: format_str_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Glob { str, pattern } => {
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let str_name: String = str.to_string();
                let str_param = self.construct_formula(trans, datasource.clone(), str, context)?;
                if inner_expected_type.encompasses(&str_param.scalar_type) {
                    let pattern_name: String = pattern.to_string();
                    let pattern_param = self.construct_formula(trans, datasource, pattern, str_param.context)?;
                    if inner_expected_type.encompasses(&pattern_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} GLOB {})", str_param.value_expr_norecursion, pattern_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} GLOB {})", str_param.value_expr_recursion, pattern_param.value_expr_recursion);
                        SelectParameter {
                            plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                            json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr: String::from("NULL"),
                            isolated_dependency_exprs: str_param.isolated_dependency_exprs
                                .union(&pattern_param.isolated_dependency_exprs)
                                .map(|e| e.clone())
                                .collect(),
                            full_reload_dependency_exprs: str_param.full_reload_dependency_exprs
                                .union(&pattern_param.full_reload_dependency_exprs)
                                .map(|e| e.clone())
                                .collect(),
                            scalar_type,
                            context: pattern_param.context
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "ISMATCH(str: Text, pattern: Text)", 
                            inner_name: pattern_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: pattern_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "ISMATCH(str: Text, pattern: Text)", 
                        inner_name: str_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: str_param.scalar_type.to_string()
                    });
                }
            }
            Formula::In { value, collection } => {
                let collection_param = self.construct_formula(trans, datasource.clone(), collection, SelectParameterContext::Collection {
                    slice_norecursion: SelectParameterSlice::None,
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None,
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(),
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(),
                    window_changes_disabled: true
                })?;
                let value_param = self.construct_formula(trans, datasource, value, context)?;

                let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let value_expr_norecursion: String = format!("({} IN {})", value_param.value_expr_norecursion, collection_param.value_expr_norecursion);
                let value_expr_recursion: String = format!("({} IN {})", value_param.value_expr_recursion, collection_param.value_expr_recursion);
                SelectParameter {
                    plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                    json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr: String::from("NULL"),
                    isolated_dependency_exprs: value_param.isolated_dependency_exprs.union(&collection_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                    full_reload_dependency_exprs: value_param.full_reload_dependency_exprs.union(&collection_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                    scalar_type,
                    context: value_param.context
                }
            }
            Formula::Index { collection, index } => {
                let index_expected_type = SelectParameterType::from(column_type::Primitive::Integer);
                let index_name: String = index.to_string();
                let index_param = self.construct_formula(trans, datasource.clone(), index, context)?;
                if index_expected_type.encompasses(&index_param.scalar_type) {
                    let collection_param = self.construct_formula(trans, datasource, collection, SelectParameterContext::Collection { 
                        slice_norecursion: SelectParameterSlice::NthValue(index_param.value_expr_norecursion), 
                        slice_recursion: SelectParameterSlice::NthValue(index_param.value_expr_recursion), 
                        filter_expr_norecursion: None, 
                        filter_expr_recursion: None,
                        order_exprs_norecursion: Vec::new(), 
                        order_exprs_recursion: Vec::new(),
                        min_depth: HashMap::new(), 
                        window_changes_disabled: false 
                    })?;

                    let scalar_type = collection_param.scalar_type;
                    let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                        collection_param.value_expr_norecursion,
                        collection_param.value_expr_recursion
                    );
                    SelectParameter { 
                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion), 
                        value_expr_norecursion,
                        value_expr_recursion, 
                        cell_expr: String::from("NULL"),
                        isolated_dependency_exprs: index_param.isolated_dependency_exprs.union(&collection_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                        full_reload_dependency_exprs: index_param.full_reload_dependency_exprs.union(&collection_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                        scalar_type, 
                        context: index_param.context
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument index of INDEX(collection: List<Any>, index: Integer)", 
                        inner_name: index_name,
                        expected_type: index_expected_type.to_string(), 
                        received_type: index_param.scalar_type.to_string()
                    });
                }
            }
            Formula::NullIf { value, null_if_match } => {
                context.disable_window_changes();
                let lhs_param = self.construct_formula(trans, datasource.clone(), value, context)?;
                let rhs_param = self.construct_formula(trans, datasource, null_if_match, lhs_param.context)?;

                let scalar_type = lhs_param.scalar_type;
                SelectParameter {
                    plain_label_expr_norecursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion, lhs_param.plain_label_expr_norecursion),
                    plain_label_expr_recursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion, lhs_param.plain_label_expr_recursion),
                    json_label_expr_norecursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion, lhs_param.json_label_expr_norecursion),
                    json_label_expr_recursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion, lhs_param.json_label_expr_recursion),
                    value_expr_norecursion: format!("NULLIF({}, {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion),
                    value_expr_recursion: format!("NULLIF({}, {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion),
                    cell_expr: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion, lhs_param.cell_expr),
                    isolated_dependency_exprs: lhs_param.isolated_dependency_exprs.union(&rhs_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                    full_reload_dependency_exprs: lhs_param.full_reload_dependency_exprs.union(&rhs_param.full_reload_dependency_exprs).map(|e| e.clone()).collect(),
                    scalar_type,
                    context: rhs_param.context
                }
            }
            Formula::Replace { original, pattern, replacement } => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let original_name: String = original.to_string();
                let original_param = self.construct_formula(trans, datasource.clone(), original, context)?;
                if inner_expected_type.encompasses(&original_param.scalar_type) {
                    let pattern_name: String = pattern.to_string();
                    let pattern_param = self.construct_formula(trans, datasource.clone(), pattern, original_param.context)?;
                    if inner_expected_type.encompasses(&pattern_param.scalar_type) {
                        let replacement_name: String = replacement.to_string();
                        let replacement_param = self.construct_formula(trans, datasource, replacement, pattern_param.context)?;
                        if inner_expected_type.encompasses(&replacement_param.scalar_type) {
                            let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                            let value_expr_norecursion: String = format!("REPLACE({}, {}, {})", original_param.value_expr_norecursion, pattern_param.value_expr_norecursion, replacement_param.value_expr_norecursion);
                            let value_expr_recursion: String = format!("REPLACE({}, {}, {})", original_param.value_expr_recursion, pattern_param.value_expr_recursion, replacement_param.value_expr_recursion);
                            SelectParameter { 
                                plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                                plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                                json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                                value_expr_norecursion,
                                value_expr_recursion, 
                                cell_expr: String::from("NULL"), 
                                isolated_dependency_exprs: original_param.isolated_dependency_exprs
                                    .union(&pattern_param.isolated_dependency_exprs)
                                    .map(|e| e.clone())
                                    .collect::<HashSet<String>>()
                                    .union(&replacement_param.isolated_dependency_exprs)
                                    .map(|e| e.clone())
                                    .collect(),
                                full_reload_dependency_exprs: original_param.full_reload_dependency_exprs
                                    .union(&pattern_param.full_reload_dependency_exprs)
                                    .map(|e| e.clone())
                                    .collect::<HashSet<String>>()
                                    .union(&replacement_param.full_reload_dependency_exprs)
                                    .map(|e| e.clone())
                                    .collect(),
                                scalar_type, 
                                context: replacement_param.context 
                            }
                        } else {
                            return Err(Error::FormulaTypeValidationError { 
                                outer_name: "Argument replacement of REPLACE(str: Text, pattern: Text, replacement: Text)", 
                                inner_name: replacement_name,
                                expected_type: inner_expected_type.to_string(), 
                                received_type: replacement_param.scalar_type.to_string()
                            });
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: "Argument pattern of REPLACE(str: Text, pattern: Text, replacement: Text)", 
                            inner_name: pattern_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: pattern_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "Argument str of REPLACE(str: Text, pattern: Text, replacement: Text)", 
                        inner_name: original_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: original_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Substring { str, start, length } => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let str_name: String = str.to_string();
                let str_param = self.construct_formula(trans, datasource.clone(), str, context)?;
                if inner_expected_type.encompasses(&str_param.scalar_type) {
                    let start_name: String = start.to_string();
                    let start_param = self.construct_formula(trans, datasource.clone(), start, str_param.context)?;
                    if inner_expected_type.encompasses(&start_param.scalar_type) {
                        match length {
                            None => {
                                let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                                let value_expr_norecursion: String = format!("SUBSTR({}, {})", str_param.value_expr_norecursion, start_param.value_expr_norecursion);
                                let value_expr_recursion: String = format!("SUBSTR({}, {})", str_param.value_expr_recursion, start_param.value_expr_recursion);
                                SelectParameter { 
                                    plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion), 
                                    plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                    json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                                    json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                                    value_expr_norecursion,
                                    value_expr_recursion, 
                                    cell_expr: String::from("NULL"), 
                                    isolated_dependency_exprs: str_param.isolated_dependency_exprs
                                        .union(&start_param.isolated_dependency_exprs)
                                        .map(|e| e.clone())
                                        .collect(),
                                    full_reload_dependency_exprs: str_param.full_reload_dependency_exprs
                                        .union(&start_param.full_reload_dependency_exprs)
                                        .map(|e| e.clone())
                                        .collect(),
                                    scalar_type, 
                                    context: start_param.context 
                                }
                            }
                            Some(length) => {
                                let length_name: String = length.to_string();
                                let length_param = self.construct_formula(trans, datasource, length, start_param.context)?;
                                if inner_expected_type.encompasses(&length_param.scalar_type) {
                                    let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                                    let value_expr_norecursion: String = format!("SUBSTR({}, {}, {})", str_param.value_expr_norecursion, start_param.value_expr_norecursion, length_param.value_expr_norecursion);
                                    let value_expr_recursion: String = format!("SUBSTR({}, {}, {})", str_param.value_expr_recursion, start_param.value_expr_recursion, length_param.value_expr_recursion);
                                    SelectParameter { 
                                        plain_label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                                        plain_label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                        json_label_expr_norecursion: scalar_type.construct_json_label_expr(&value_expr_norecursion),
                                        json_label_expr_recursion: scalar_type.construct_json_label_expr(&value_expr_recursion),
                                        value_expr_norecursion,
                                        value_expr_recursion, 
                                        cell_expr: String::from("NULL"), 
                                        isolated_dependency_exprs: str_param.isolated_dependency_exprs
                                            .union(&start_param.isolated_dependency_exprs)
                                            .map(|e| e.clone())
                                            .collect::<HashSet<String>>()
                                            .union(&length_param.isolated_dependency_exprs)
                                            .map(|e| e.clone())
                                            .collect(),
                                        full_reload_dependency_exprs: str_param.full_reload_dependency_exprs
                                            .union(&start_param.full_reload_dependency_exprs)
                                            .map(|e| e.clone())
                                            .collect::<HashSet<String>>()
                                            .union(&length_param.full_reload_dependency_exprs)
                                            .map(|e| e.clone())
                                            .collect(),
                                        scalar_type, 
                                        context: length_param.context 
                                    }
                                } else {
                                    return Err(Error::FormulaTypeValidationError { 
                                        outer_name: "Argument length of SUBSTRING(str: Text, start: Integer, length: Integer)", 
                                        inner_name: length_name,
                                        expected_type: inner_expected_type.to_string(), 
                                        received_type: length_param.scalar_type.to_string()
                                    });
                                }
                            }
                        }
                    } else {
                        return Err(Error::FormulaTypeValidationError { 
                            outer_name: match length {
                                Some(_) => "Argument start of SUBSTRING(str: Text, start: Integer, length: Integer)", 
                                None => "Argument start of SUBSTRING(str: Text, start: Integer)"
                            },
                            inner_name: start_name,
                            expected_type: inner_expected_type.to_string(), 
                            received_type: start_param.scalar_type.to_string()
                        });
                    }
                } else {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: match length {
                            Some(_) => "Argument str of SUBSTRING(str: Text, start: Integer, length: Integer)", 
                            None => "Argument str of SUBSTRING(str: Text, start: Integer)"
                        },
                        inner_name: str_name,
                        expected_type: inner_expected_type.to_string(), 
                        received_type: str_param.scalar_type.to_string()
                    });
                }
            }
            Formula::Switch { value, matches, formula_if_no_match } => {
                let value_param = self.construct_formula(trans, datasource.clone(), value, context)?;
                context = value_param.context;

                let mut return_scalar_type = SelectParameterType::new();
                let (
                    value_norecursion_when_clauses, 
                    value_recursion_when_clauses,
                    plain_label_norecursion_when_clauses,
                    plain_label_recursion_when_clauses, 
                    json_label_norecursion_when_clauses,
                    json_label_recursion_when_clauses,
                    cell_when_clauses,
                    isolated_dependency_exprs_when,
                    full_reload_dependency_exprs_when
                ) = {
                    let mut match_params: Vec<(SelectParameter, SelectParameter)> = Vec::new();
                    for (test_match, formula_if_match) in matches {
                        let test_match_param = self.construct_formula(trans, datasource.clone(), Box::new(test_match), context)?;
                        context = test_match_param.context.clone();

                        let if_match_param = self.construct_formula(trans, datasource.clone(), Box::new(formula_if_match), context)?;
                        context = if_match_param.context.clone();
                        return_scalar_type = return_scalar_type.generalize(&if_match_param.scalar_type);

                        match_params.push((test_match_param, if_match_param));
                    }

                    let when_clauses_norecursion: Vec<_> = match_params.iter()
                        .map(|(test_match_param, if_match_param)| (format!("WHEN {} IS {} THEN ", value_param.value_expr_norecursion, test_match_param.value_expr_norecursion), if_match_param))
                        .collect();
                    let when_clauses_recursion: Vec<_> = match_params.iter()
                        .map(|(test_match_param, if_match_param)| (format!("WHEN {} IS {} THEN ", value_param.value_expr_recursion, test_match_param.value_expr_recursion), if_match_param))
                        .collect();
                    (
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.value_expr_norecursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_recursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.value_expr_recursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.plain_label_expr_norecursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_recursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.plain_label_expr_recursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.json_label_expr_norecursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_recursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.json_label_expr_recursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.cell_expr))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        match_params.iter()
                            .fold(value_param.isolated_dependency_exprs, 
                                |acc, (param1, param2)| 
                                acc.union(&param1.isolated_dependency_exprs).map(|e| e.clone()).collect::<HashSet<String>>()
                                    .union(&param2.isolated_dependency_exprs).map(|e| e.clone()).collect()
                            ),
                        match_params.iter()
                            .fold(value_param.full_reload_dependency_exprs, 
                                |acc, (param1, param2)| 
                                acc.union(&param1.full_reload_dependency_exprs).map(|e| e.clone()).collect::<HashSet<String>>()
                                    .union(&param2.full_reload_dependency_exprs).map(|e| e.clone()).collect()
                            )
                    )
                };
                
                let (
                    value_expr_norecursion, 
                    value_expr_recursion,
                    plain_label_expr_norecursion, 
                    plain_label_expr_recursion, 
                    json_label_expr_norecursion,
                    json_label_expr_recursion,
                    cell_expr,
                    isolated_dependency_exprs,
                    full_reload_dependency_exprs
                ) = {
                    let if_no_match_param = self.construct_formula(trans, datasource, formula_if_no_match, context)?;
                    context = if_no_match_param.context.clone();
                    (
                        format!("CASE {value_norecursion_when_clauses} ELSE {} END", if_no_match_param.value_expr_norecursion),
                        format!("CASE {value_recursion_when_clauses} ELSE {} END", if_no_match_param.value_expr_recursion),
                        format!("CASE {plain_label_norecursion_when_clauses} ELSE {} END", if_no_match_param.plain_label_expr_norecursion),
                        format!("CASE {plain_label_recursion_when_clauses} ELSE {} END", if_no_match_param.plain_label_expr_recursion),
                        format!("CASE {json_label_norecursion_when_clauses} ELSE {} END", if_no_match_param.plain_label_expr_norecursion),
                        format!("CASE {json_label_recursion_when_clauses} ELSE {} END", if_no_match_param.plain_label_expr_recursion),
                        format!("CASE {cell_when_clauses} ELSE {} END", if_no_match_param.cell_expr),
                        isolated_dependency_exprs_when.union(&if_no_match_param.isolated_dependency_exprs).map(|e| e.clone()).collect(),
                        full_reload_dependency_exprs_when.union(&if_no_match_param.full_reload_dependency_exprs).map(|e| e.clone()).collect()
                    )
                };
                SelectParameter {
                    plain_label_expr_norecursion,
                    plain_label_expr_recursion,
                    json_label_expr_norecursion,
                    json_label_expr_recursion,
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr,
                    isolated_dependency_exprs,
                    full_reload_dependency_exprs,
                    scalar_type: return_scalar_type,
                    context
                }
            }
        })
    }
}