use crate::{
    data::{column, column_type, datasource::Datasource, schema, table, view}, util::{error::Error, formula::Formula},
};
use crate::util::db::{sql_iter, sql_map_then_iter};
use bitflags::bitflags;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::{cell, collections::{HashMap, HashSet}, mem::transmute};
use regex::Regex;

mod datasource_cte;
mod wrapper_cte;
mod parameter;




enum SelectMainColumn {
    Cell {
        /// Expression for the cell's value.
        value_expr: String,

        /// The ordinal for the value.
        value_ord: String,

        /// Expression for the cell's label.
        label_expr: String,

        /// The ordinal for the label.
        label_ord: String
    },
    Formula {
        /// Expression for the formula's raw value.
        value_expr: String,

        /// The ordinal for the value.
        value_ord: String,

        /// Expression for the label for the formula's value.
        label_expr: String,

        /// The ordinal for the label.
        label_ord: String,

        /// Expression referencing the cell that the formula's value reflects.
        cell_expr: String,

        /// The ordinal for the referenced cell.
        cell_ord: String,

        /// Expression referencing each table_oid:column_oid:row_oid for which the formula has a dependency that can be resolved with a hot reload.
        isolated_dependencies_expr: String,

        /// The ordinal for the isolated dependencies.
        isolated_dependencies_ord: String,

        /// Expression referencing each table_oid:column_oid:row_oid for which the formula has a dependency that can only be resolved by reloading the entire report.
        full_reload_dependencies_expr: String,

        /// The ordinal for the full-reload dependencies.
        full_reload_dependencies_ord: String 
    }
}

struct SelectLabelColumn {
    /// The expression for the column's label in plaintext, in the base case.
    plain_expr_norecursion: String,

    /// The expression for the column's label in plaintext, in the recursive case.
    plain_expr_recursion: String,

    /// The expression for the column's label as a JSON key-value pair (i.e. "Column Name": "This is the column label."), in the base case.
    json_expr_norecursion: String,

    /// The expression for the column's label as a JSON key-value pair (i.e. "Column Name": "This is the column label."), in the recursive case.
    json_expr_recursion: String,

    /// The ordering of the column.
    ordering: i64,

    /// True if the column is a required key column.
    /// False if the column is a key column of an inheritor schema.
    is_required: bool 
}

impl SelectLabelColumn {
    /// Constructs a new key column for the label that does not involve recursion (i.e. the expression in the base and recursive cases are identical).
    fn new_norecursion(plain_expr: String, json_expr: String, ordering: i64, is_required: bool) -> Self {
        Self {
            plain_expr_norecursion: plain_expr.clone(),
            plain_expr_recursion: plain_expr,
            json_expr_norecursion: json_expr.clone(),
            json_expr_recursion: json_expr,
            ordering,
            is_required
        }
    }
}

enum SelectConstructorType {
    SelectMainConstructor {
        /// The OID of the schema.
        schema_oid: i64,

        /// The columns of the schema.
        columns: Vec<SelectMainColumn>
    },

    SelectLabelConstructor {
        /// The OID of the schema.
        schema_oid: i64,

        /// Locations where a label references itself.
        /// The first item in each tuple is the OID that is a self-reference.
        /// The second item in each tuple is the OID further up in the datasource chain that is already present.
        recursions: Vec<(String, String)>,

        /// The columns referenced by the label.
        columns: Vec<SelectLabelColumn>
    }
}

impl SelectConstructorType {
    fn build(&self, trans: &Transaction, cte_list: Vec<String>, oid_list: Vec<String>) -> Result<String, Error> {
        Ok(match self {
            Self::SelectMainConstructor { schema_oid, columns } => {
                format!(
                    "
                    WITH {} 
                    SELECT 
                        ROW_NUMBER() OVER ({}) AS ROW_INDEX,
                        l.PLAIN_LABEL, 
                        l.JSON_LABEL, 
                        {}
                        {} 
                    FROM WRAPPER w 
                    INNER JOIN SCHEMA{schema_oid}_LABEL_VIEW l {}
                    ",
                    
                    // All of the CTEs, including the wrapper
                    cte_list.join(", "),

                    // ORDER BY expressions
                    // For now, leaving blank
                    String::from(""),

                    // Include OBJECT_LABEL and ROOT{schema_oid}_SCHEMA columns if the schema is of type table
                    if let Some(root_datasource) = Datasource::check_default_datasource_transact(trans, schema_oid.clone())? {
                        format!(
                            "
                            l.OBJECT_LABEL, 
                            l.TABLE_OID, 
                            w.{}_OID AS OID, 
                            ", 
                            root_datasource.get_alias()
                        )
                    } else {
                        format!(
                            "
                            {} AS OBJECT_FILTER,
                            ",
                            oid_list.iter()
                                .map(|oid| format!("'{}=' || CAST(w.{oid} AS TEXT)", sql_encode_string(&oid)))
                                .reduce(|acc, e| format!("{acc} || '&' || {e}"))
                                .unwrap_or(String::from("''"))
                        )
                    },

                    // Select each column from the wrapper
                    oid_list.iter().map(|oid| format!("w.{oid}"))
                        .chain(
                            columns.iter().map(|col| match col {
                                SelectMainColumn::Cell { value_expr, value_ord, label_expr, label_ord } => 
                                    format!("{value_expr} AS {value_ord}, {label_expr} AS {label_ord}"),
                                SelectMainColumn::Formula { value_expr, value_ord, label_expr, label_ord, cell_expr, cell_ord, isolated_dependencies_expr, isolated_dependencies_ord, full_reload_dependencies_expr, full_reload_dependencies_ord } => 
                                    format!("{value_expr} AS {value_ord}, {label_expr} AS {label_ord}, {cell_expr} AS {cell_ord}, {isolated_dependencies_expr} AS {isolated_dependencies_ord}, {full_reload_dependencies_expr} AS {full_reload_dependencies_ord}")
                            })
                        )
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap_or(String::from("NULL AS COLUMN1")),

                    // Filter label view by the same OIDs as the main view
                    match oid_list.iter().map(|oid| format!("w.{oid} = l.{oid}"))
                        .reduce(|acc, e| format!("{acc} AND {e}")) {
                        Some(exprs) => format!("ON {exprs}"),
                        None => String::from("")
                    }
                )
            }
            Self::SelectLabelConstructor { schema_oid, recursions, columns } => {
                // Assume columns are already sorted
                //columns.sort_by_key(|col| col.ordering);

                // Construct expressions for each column
                let (all_columns_norecursion, all_columns_recursion): (String, String) = {
                    let plain_expr_norecursion: String = if columns.len() == 1 {
                        columns[0].plain_expr_norecursion.clone()
                    } else {
                        String::from("NULL")
                    };
                    let json_expr_norecursion: String = {
                        let filtered_columns: Vec<String> = columns.iter()
                            .filter_map(|col| {
                                if col.is_required {
                                    Some(format!("SELECT {}", col.json_expr_norecursion))
                                } else {
                                    None 
                                }
                            })
                            .collect();
                        if filtered_columns.len() > 0 {
                            format!(
                                "'{{ ' || GROUP_CONCAT(({}), ', ') || ' }}'",
                                filtered_columns.into_iter()
                                    .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                    .unwrap()
                            )
                        } else {
                            String::from("NULL")
                        }
                    };

                    let plain_expr_recursion: String = if columns.len() == 1 {
                        columns[0].plain_expr_recursion.clone()
                    } else {
                        String::from("NULL")
                    };
                    let json_expr_recursion: String = {
                        let filtered_columns: Vec<String> = columns.iter()
                            .filter_map(|col| {
                                if col.is_required {
                                    Some(format!("SELECT {}", col.json_expr_recursion))
                                } else {
                                    None 
                                }
                            })
                            .collect();
                        if filtered_columns.len() > 0 {
                            format!(
                                "'{{ ' || GROUP_CONCAT(({}), ', ') || ' }}'",
                                filtered_columns.into_iter()
                                    .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                    .unwrap()
                            )
                        } else {
                            String::from("NULL")
                        }
                    };

                    match Datasource::check_default_datasource_transact(trans, schema_oid.clone())? {
                        Some(root_datasource) => {
                            // Schema is a table, so include OBJECT_LABEL and TABLE_OID
                            let root_datasource_oid: i64 = root_datasource.get_root_datasource_oid();

                            let object_expr_norecursion: String = format!(
                                "'{{ \"' || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = w.ROOT{root_datasource_oid}_TABLE) || '\": {} }}'",

                                // The key columns of the schema
                                if columns.len() == 0 {
                                    String::from("null")
                                } else {
                                    format!(
                                        "' || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', 'null') || '",
                                        columns.iter()
                                            .map(|col| format!("SELECT {}", col.json_expr_norecursion))
                                            .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                            .unwrap()
                                    )
                                }
                            );
                            let object_expr_recursion: String = format!(
                                "'{{ \"' || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = w.ROOT{root_datasource_oid}_TABLE) || '\": {} }}'",

                                // The key columns of the schema
                                if columns.len() == 0 {
                                    String::from("null")
                                } else {
                                    format!(
                                        "' || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', 'null') || '",
                                        columns.iter()
                                            .map(|col| format!("SELECT {}", col.json_expr_norecursion))
                                            .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                            .unwrap()
                                    )
                                }
                            );

                            (
                                oid_list.iter().fold(
                                    format!(
                                        "
                                        {plain_expr_norecursion} AS PLAIN_LABEL, 
                                        {json_expr_norecursion} AS JSON_LABEL,
                                        {object_expr_norecursion} AS OBJECT_LABEL,
                                        w.ROOT{root_datasource_oid}_TABLE AS TABLE_OID,
                                        w.ROOT{root_datasource_oid}_OID AS OID
                                        "
                                    ),
                                    |acc, e| format!("{acc}, w.{e}")
                                ),
                                oid_list.iter().fold(
                                    format!(
                                        "
                                        {plain_expr_recursion} AS PLAIN_LABEL, 
                                        {json_expr_recursion} AS JSON_LABEL,
                                        {object_expr_recursion} AS OBJECT_LABEL,
                                        w.ROOT{root_datasource_oid}_TABLE AS TABLE_OID,
                                        w.ROOT{root_datasource_oid}_OID AS OID
                                        "
                                    ),
                                    |acc, e| format!("{acc}, w.{e}")
                                )
                            )
                        }
                        None => {
                            (
                                oid_list.iter().fold(
                                    format!(
                                        "
                                        {plain_expr_norecursion} AS PLAIN_LABEL, 
                                        {json_expr_norecursion} AS JSON_LABEL
                                        "
                                    ),
                                    |acc, e| format!("{acc}, w.{e}")
                                ),
                                oid_list.iter().fold(
                                    format!(
                                        "
                                        {plain_expr_recursion} AS PLAIN_LABEL, 
                                        {json_expr_recursion} AS JSON_LABEL
                                        "
                                    ),
                                    |acc, e| format!("{acc}, w.{e}")
                                )
                            )
                        }
                    }
                };
                
                if recursions.len() > 0 {
                    // Need to make a recursive CTE
                    let group_by_expr: String = if oid_list.len() > 0 {
                        format!("GROUP BY {}", oid_list.iter().map(|oid| format!("w.{oid}")).reduce(|acc, e| format!("{acc}, {e}")).unwrap())
                    } else {
                        String::from("")
                    };
                    format!(
                        "
                        WITH {}, 
                        LABEL_CTE (PLAIN_LABEL, JSON_LABEL, OBJECT_LABEL {}) AS (
                            SELECT
                                {all_columns_norecursion}
                            FROM WRAPPER w
                            WHERE {}
                            {group_by_expr}

                            UNION

                            SELECT
                                {all_columns_recursion}
                            FROM WRAPPER w
                            {}
                            {group_by_expr}
                        ) 
                        
                        SELECT * FROM LABEL_CTE 
                        UNION ALL 
                        SELECT 
                            {all_columns_norecursion}
                        FROM WRAPPER w
                        WHERE {}
                        {group_by_expr}
                        ",

                        // All of the non-recursive CTEs, including the wrapper
                        cte_list.join(", "),

                        // The OIDs selected by the label CTE
                        oid_list.iter().fold(String::from(""), |acc, e| format!("{acc}, {e}")),

                        // Condition for the base case 
                        recursions.iter().map(|(recursive_datasource, _)| format!("w.{recursive_datasource}_OID IS NULL"))
                            .reduce(|acc, e| format!("{acc} AND {e}"))
                            .unwrap(),

                        // The recursive joins
                        recursions.iter()
                            .map(|(recursive_datasource, recursive_ref_oid)| format!("LEFT JOIN LABEL_CTE AS {recursive_datasource} ON {recursive_datasource}.{recursive_ref_oid} = w.{recursive_datasource}_OID"))
                            .fold(String::from(""), |acc, e| format!("{acc} {e}")),

                        // The condition that causes a label to be understood as being truly self-referential
                        // i.e. there is at least one recursion that cannot be performed
                        recursions.iter()
                            .map(|(recursive_datasource, recursive_ref_oid)| format!("(w.{recursive_datasource}_OID IS NOT NULL AND w.{recursive_datasource}_OID NOT IN (SELECT l.{recursive_ref_oid} FROM LABEL_CTE))"))
                            .reduce(|acc, e| format!("{acc} OR {e}"))
                            .unwrap()
                    )
                } else {
                    // No need for a recursive CTE, can get label straight from wrapper
                    format!(
                        "
                        WITH {} 
                        SELECT 
                            {all_columns_norecursion}
                        FROM WRAPPER w
                        {}
                        ",

                        // All of the CTEs, including the wrapper
                        cte_list.join(", "),

                        if oid_list.len() > 0 {
                            format!("GROUP BY {}", oid_list.iter().map(|oid| format!("w.{oid}")).reduce(|acc, e| format!("{acc}, {e}")).unwrap())
                        } else {
                            String::from("")
                        }
                    )          
                }
            }
        })
    }
}





/// The constructor for a SELECT statement.
struct SelectConstructor {
    /// The number of random values.
    random_values: usize,

    /// The CTEs pulling data from a datasource.
    cte_datasource: HashMap<String, DatasourceCteConstructor>,
    
    /// The type of SELECT statement being constructed.
    constructor_type: SelectConstructorType
}

impl SelectConstructor {
    /// SelectConstructor for the main schema view.
    fn new_main(trans: &Transaction, schema_oid: i64) -> Result<Self, Error> {
        let mut select_constructor: Self = Self {
            random_values: 0,
            cte_datasource: HashMap::new(),
            constructor_type: SelectConstructorType::SelectMainConstructor { 
                schema_oid: schema_oid.clone(),
                columns: Vec::new() 
            }
        };

        let root_datasource: Option<Datasource> = Datasource::check_default_datasource_transact(trans, schema_oid)?;

        // Add all inheritor datasources
        if let Some(root_datasource) = &root_datasource {           
            select_constructor.add_datasource(root_datasource.clone(), false);

            // Add datasource for each inheritor table
            sql_map_then_iter(
                trans,
                "
                SELECT 
                    INHERITOR_DATASOURCE_PATH 
                FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW 
                WHERE MASTER_SCHEMA_OID = ?1
                ",
                params![schema_oid],
                |row| row.get("INHERITOR_DATASOURCE_PATH"),
                |inheritor_datasource_path| {
                    select_constructor.add_datasource(
                        root_datasource.append_path(inheritor_datasource_path)?, 
                        false
                    );
                    Ok(None::<()>)
                }
            )?;
        }

        sql_map_then_iter(
            trans,
            "
            SELECT 
                COLUMN_OID, 
                DATASOURCE_PATH 
            FROM METADATA_SCHEMA_COLUMN_VIEW 
            WHERE SCHEMA_OID = ?1 
            ORDER BY IS_SUBREPORT ASC
            ",
            params![schema_oid],
            |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, String>("DATASOURCE_PATH")?)),
            |(column_oid, datasource_path)| {
                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
                let column_type: column_type::ColumnType = column.column_type.clone();
                let param: SelectParameter = match &root_datasource {
                    Some(root_datasource) => {
                        let column_datasource: SelectDatasource = SelectDatasource::new_norecursion(root_datasource.append_path(datasource_path)?, schema_oid.clone());
                        select_constructor.add_concrete_parameter(trans, column_datasource, column, SelectParameterContext::Scalar)?    
                    }
                    None => {
                        select_constructor.add_virtual_parameter(trans, column, SelectParameterContext::Scalar)?
                    }
                };

                if let SelectConstructorType::SelectMainConstructor { columns, .. } = &mut select_constructor.constructor_type {
                    let value_expr: String = param.value_expr_norecursion;
                    let value_ord: String = format!("COLUMN{column_oid}_VALUE");
                    let label_expr: String = if param.plain_label_expr_norecursion != "NULL" {
                        format!("COALESCE({}, {})", param.plain_label_expr_norecursion, param.json_label_expr_norecursion)
                    } else {
                        param.json_label_expr_norecursion
                    };
                    let label_ord: String = format!("COLUMN{column_oid}_LABEL");
                    match column_type {
                        column_type::ColumnType::Formula { .. } => {
                            columns.push(SelectMainColumn::Formula { 
                                value_expr,
                                value_ord,
                                label_expr,
                                label_ord,
                                cell_expr: param.cell_expr,
                                cell_ord: format!("COLUMN{column_oid}_CELL"),
                                isolated_dependencies_expr: if param.isolated_dependency_exprs.len() > 0 {
                                    param.isolated_dependency_exprs.into_iter()
                                        .reduce(|acc, e| format!("{acc} || ',' || {e}"))
                                        .unwrap()
                                } else {
                                    String::from("NULL")
                                },
                                isolated_dependencies_ord: format!("COLUMN{column_oid}_ISOLATEDRELOAD"),
                                full_reload_dependencies_expr: if param.full_reload_dependency_exprs.len() > 0 {
                                    param.full_reload_dependency_exprs.into_iter()
                                        .reduce(|acc, e| format!("{acc} || ',' || {e}"))
                                        .unwrap()
                                } else {
                                    String::from("NULL")
                                },
                                full_reload_dependencies_ord: format!("COLUMN{column_oid}_FULLRELOAD")
                            });
                        }
                        _ => {
                            columns.push(SelectMainColumn::Cell { 
                                value_expr,
                                value_ord,
                                label_expr,
                                label_ord
                            });
                        }
                    }
                }
                Ok(None::<()>)
            }
        )?;

        Ok(select_constructor)
    }

    /// SelectConstructor for the label schema view.
    fn new_label(trans: &Transaction, schema_oid: i64) -> Result<Self, Error> {
        let mut select_constructor: Self = Self {
            random_values: 0,
            cte_datasource: HashMap::new(),
            constructor_type: SelectConstructorType::SelectLabelConstructor { 
                schema_oid: schema_oid.clone(),
                recursions: Vec::new(),
                columns: Vec::new()
            }
        };

        let root_datasource: Option<Datasource> = Datasource::check_default_datasource_transact(trans, schema_oid)?;

        // Add all inheritor datasources
        if let Some(root_datasource) = &root_datasource {    
            select_constructor.add_datasource(root_datasource.clone(), false);

            // Add datasource for each inheritor table
            sql_map_then_iter(
                trans,
                "
                SELECT 
                    INHERITOR_DATASOURCE_PATH 
                FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW 
                WHERE MASTER_SCHEMA_OID = ?1
                ",
                params![schema_oid],
                |row| row.get::<_, String>("INHERITOR_DATASOURCE_PATH"),
                |inheritor_datasource_path| {
                    select_constructor.add_datasource(
                        root_datasource.append_path(inheritor_datasource_path)?, 
                        false
                    );
                    Ok(None::<()>)
                }
            )?;
        }

        sql_map_then_iter(
            trans, 
            "
            SELECT 
                COLUMN_OID, 
                DATASOURCE_PATH, 
                ORDERING, 
                IS_REQUIRED 
            FROM METADATA_SCHEMA_COLUMN_VIEW 
            WHERE SCHEMA_OID = ?1 
                AND IS_PRIMARY_KEY 
            ORDER BY IS_SUBREPORT ASC
            ", 
            params![schema_oid], 
            |row| Ok((
                row.get::<_, i64>("COLUMN_OID")?, 
                row.get::<_, String>("DATASOURCE_PATH")?, 
                row.get::<_, i64>("ORDERING")?, 
                row.get::<_, bool>("IS_REQUIRED")?
            )), 
            |(column_oid, datasource_path, ordering, is_required)| {
                println!("  Now adding {datasource_path}.COLUMN{column_oid} to the label query...");
                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                println!("    Successfully queried column metadata.");
                let json_safe_column_name: String = json_encode_string(&column.name);
                let param: SelectParameter = match &root_datasource {
                    Some(root_datasource) => {
                        let column_datasource: SelectDatasource = SelectDatasource::new_norecursion(root_datasource.append_path(datasource_path)?, schema_oid.clone());
                        select_constructor.add_concrete_parameter(trans, column_datasource, column, SelectParameterContext::Scalar)?    
                    }
                    None => {
                        select_constructor.add_virtual_parameter(trans, column, SelectParameterContext::Scalar)?
                    }
                };

                if let SelectConstructorType::SelectLabelConstructor { columns, .. } = &mut select_constructor.constructor_type {
                    columns.push(SelectLabelColumn { 
                        plain_expr_norecursion: param.plain_label_expr_norecursion, 
                        plain_expr_recursion: param.plain_label_expr_recursion, 
                        json_expr_norecursion: format!("'\"{json_safe_column_name}\": ' || {}", param.json_label_expr_norecursion), 
                        json_expr_recursion: format!("'\"{json_safe_column_name}\": ' || {}", param.json_label_expr_recursion), 
                        ordering, 
                        is_required
                    });
                }
                Ok(None::<()>)
            }
        )?;

        Ok(select_constructor)
    }

    /// Builds the SQL syntax for this SELECT statement.
    fn build(&self, trans: &Transaction) -> Result<String, Error> {
        let (cte_list, oid_list): (Vec<String>, Vec<String>) = {
            let mut root_datasource_aliases: Vec<String> = Vec::new();
            let mut cte_list: Vec<String> = Vec::new();
            let mut oid_list: Vec<String> = Vec::new();
            
            // Compile each CTE representing a datasource
            for (cte_name, cte) in self.cte_datasource.iter() {
                cte_list.push(format!("{cte_name} AS ({})", cte.build()?));
                if let Datasource::Table { .. } = &cte.datasource {
                    root_datasource_aliases.push(cte.datasource.get_alias());
                }
                if !cte.is_always_collection {
                    oid_list.push(format!("{cte_name}_OID"));
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
                        (1..(self.random_values + 1))
                            .map(|n| format!("RANDOM() AS RANDOM{n}"))
                            .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

                        // FROM/JOIN clauses
                        root_datasource_aliases.into_iter().reduce(|acc, e| format!("{acc} INNER JOIN {e}")).unwrap()
                    )
                } else {
                    String::from("SELECT NULL AS COLUMN1 WHERE FALSE")
                }
            ));

            (cte_list, oid_list)
        };

        self.constructor_type.build(trans, cte_list, oid_list)
    }

    
}




struct ViewsToCreate {
    /// True if the main view needs to be created. False otherwise.
    create_main_view: bool,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool
}

/// Analyze which views are associated with a schema and will need to be recreated.
fn analyze_views(
    trans: &Transaction,
    schema_oid: i64,
    drop_main: bool,
    drop_label: bool,
    views_to_create: &mut HashMap<i64, ViewsToCreate>,
) -> Result<(), Error> {
    if let Some(view_to_create) = views_to_create.get_mut(&schema_oid) {
        println!("  Analyzing views for schema OID: {schema_oid}");
        println!("    Main = {} || {drop_main}", view_to_create.create_main_view);
        println!("    Label = {} || {drop_label}", view_to_create.create_label_view);
        if (!view_to_create.create_main_view && drop_main) || (!view_to_create.create_label_view && drop_label) {
            // Some new information is being added
            view_to_create.create_main_view = drop_main.clone() || view_to_create.create_main_view;
            view_to_create.create_label_view = drop_label.clone() || view_to_create.create_label_view;
        } else {
            // No new information is being added
            println!("    Skipping...");
            return Ok(());
        }
    } else {
        println!("  Analyzing views for schema OID: {schema_oid}");
        println!("    Main = {drop_main}");
        println!("    Label = {drop_label}");
        views_to_create.insert(schema_oid, ViewsToCreate { 
            create_main_view: drop_main.clone(), 
            create_label_view: drop_label.clone() 
        });
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    sql_map_then_iter(
        trans,
        "
        SELECT 
            INHERITOR_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE 
        WHERE MASTER_SCHEMA_OID = ?1
        ",
        params![schema_oid],
        |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"),
        |inheritor_schema_oid| {
            analyze_views(trans, inheritor_schema_oid, true, true, views_to_create)?;
            Ok(None::<()>)
        }
    )?;

    // Drop only the label views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    sql_map_then_iter(
        trans,
        "
        SELECT 
            MASTER_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE 
        WHERE INHERITOR_SCHEMA_OID = ?1
        ",
        params![schema_oid],
        |row| row.get::<_, i64>("MASTER_SCHEMA_OID"),
        |master_schema_oid| {
            analyze_views(trans, master_schema_oid, true, true, views_to_create)?;
            Ok(None::<()>)
        }
    )?;

    if drop_label {
        // Drop the main views that use the label view, or label views that are dependent on the schema where the label view is being dropped
        sql_map_then_iter(
            trans,
            "
            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__OBJECT o ON o.OID = c.TYPE_OID
            WHERE o.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__SELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1
            ",
            params![schema_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("SCHEMA_OID")?,
                    row.get::<_, bool>("IS_PRIMARY_KEY")?,
                ))
            },
            |(referencing_schema_oid, referenced_in_label)| {
                analyze_views(
                    trans,
                    referencing_schema_oid,
                    true,
                    referenced_in_label,
                    views_to_create
                )?;
                Ok(None::<()>)
            }
        )?;
    }
    Ok(())
}

/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    println!("Analyzing which views need to be recreated...");
    let mut views_to_create: HashMap<i64, ViewsToCreate> = HashMap::new();
    analyze_views(trans, schema_oid, true, true, &mut views_to_create)?;

    // Drop all of the main views
    for (view_schema_oid, view_to_drop) in views_to_create.iter() {
        if view_to_drop.create_main_view {
            println!("Now dropping SCHEMA{view_schema_oid}_VIEW...");
            let sql_drop: String = format!("DROP VIEW IF EXISTS SCHEMA{view_schema_oid}_VIEW");
            sql_execute(trans, &sql_drop, [])?;
        }
    }

    // Drop and recreate all of the label views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_label_view {
            println!("Now dropping SCHEMA{view_schema_oid}_LABEL_VIEW...");
            let sql_drop: String = format!("DROP VIEW IF EXISTS SCHEMA{view_schema_oid}_LABEL_VIEW");
            sql_execute(trans, &sql_drop, [])?;

            println!("Now generating SQL for SCHEMA{view_schema_oid}_LABEL_VIEW...");
            let select_constructor: SelectConstructor = SelectConstructor::new_label(trans, view_schema_oid.clone())?;
            let sql_create: String = format!(
                "CREATE VIEW SCHEMA{view_schema_oid}_LABEL_VIEW AS {}",
                select_constructor.build(trans)?
            );
            println!("{sql_create}");
            sql_execute(trans, &sql_create, [])?;
        }
    }

    // Create all of the main views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_main_view {
            println!("Now generating SQL for SCHEMA{view_schema_oid}_VIEW...");
            let select_constructor: SelectConstructor = SelectConstructor::new_main(trans, view_schema_oid.clone())?;
            let sql_create: String = format!(
                "CREATE VIEW SCHEMA{view_schema_oid}_VIEW AS {}",
                select_constructor.build(trans)?
            );
            println!("{sql_create}");
            sql_execute(trans, &sql_create, [])?;
        }
    }
    Ok(())
}
