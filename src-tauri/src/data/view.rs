use crate::{
    data::{column, column_type, datasource::Datasource, schema, table, view}, util::{error::Error, formula::Formula},
};
use bitflags::bitflags;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::{cell, collections::{HashMap, HashSet}, mem::transmute};
use regex::Regex;


/// Encodes a string to make it safe for inserting inside an SQL string.
fn sql_encode_string(str: &String) -> String {
    str.replace("'", "''")
}

/// Encodes a string to make it safe for inserting into a JSON double-quoted string inside an SQL string.
fn json_encode_string(str: &String) -> String {
    str.replace("'", "''")
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
}



#[derive(Clone)]
struct DatasourceCteColumn {
    /// The expression for the column value.
    value_expr: String,

    /// The ordinal for the column value.
    value_ord: String
}

/// A constructor for a CTE that pulls columns from a datasource.
struct DatasourceCteConstructor {
    /// The main datasource.
    datasource: Datasource,

    /// The columns queried in this CTE.
    columns: HashMap<i64, DatasourceCteColumn>,

    /// Datasources that are dependent on this one.
    /// The value for a datasource is true if the child datasource is always grouped, and false if it is ever not grouped.
    child_datasources: HashSet<Datasource>,

    /// True if the values from this CTE are always in a collection.
    /// False if the values from this CTE are ever not in a collection.
    is_always_collection: bool 
}

impl DatasourceCteConstructor {
    /// Builds the SQL statement for this CTE.
    fn build(&self) -> Result<String, Error> {
        Ok(format!(
            "
            SELECT
                -- The row OID of this row in the datasource
                t.OID AS {}_OID
                -- The schema OID of this row in the datasource 
                {}
                -- Columns from this datasource 
                {}
                -- Parent datasource OID, if applicable
                {}
                -- Columns from child datasources
                {}
            FROM TABLE{} t
            -- Join to multiselect table, if applicable
            {}
            -- Joins to child datasources
            {}
            WHERE NOT t.TRASH
            ",
            self.datasource.get_alias(),

            // Table for this datasource
            format!(
                ", {} AS {}_TABLE",
                {
                    let mut child_inheritor_datasources = self.child_datasources.iter()
                        .filter_map(|child_datasource| {
                            if let Datasource::InheritorTable { .. } = child_datasource {
                                Some(format!("{}_TABLE", child_datasource.get_alias()))
                            } else {
                                None
                            }
                        });
                    if child_inheritor_datasources.any(|_| true) {
                        format!(
                            "COALESCE({}, {})",
                            child_inheritor_datasources.reduce(|acc, e| format!("{acc}, {e}")).unwrap(),
                            self.datasource.get_schema_oid()?
                        )
                    } else {
                        format!("{}", self.datasource.get_schema_oid()?)
                    }
                },
                self.datasource.get_alias()
            ),

            // Columns from this datasource
            self.columns.iter()
                .map(|(_, col)| format!("{} AS {}", col.value_expr, col.value_ord))
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),
            
            // Parent datasource OID, if applicable
            match &self.datasource {
                Datasource::Table { .. }
                | Datasource::InheritorTable { .. } => String::from(""),
                Datasource::MasterTable { parent_datasource, table_oid } => 
                    format!(", t.MASTER{table_oid}_OID AS PARENT_{}_OID", parent_datasource.get_alias()),
                Datasource::Column { parent_datasource, column } => {
                    match column.column_type {
                        column_type::ColumnType::Object { table_oid, .. }
                        | column_type::ColumnType::Select { table_oid, .. } => {
                            if self.datasource.get_schema_oid()? == column.schema.oid {
                                // Inverted direction
                                format!(
                                    ", t.COLUMN{} AS PARENT_{}_OID",
                                    column.oid,
                                    parent_datasource.get_alias()
                                )
                            } else {
                                // Normal direction
                                String::from("")
                            }
                        }
                        column_type::ColumnType::Multiselect { table_oid, .. } => {
                            format!(
                                ", m.TABLE{}_OID AS PARENT_{}_OID", 
                                parent_datasource.get_schema_oid()?, 
                                parent_datasource.get_alias()
                            )
                        }
                        _ => {
                            return Err(Error::AdhocError("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                        }
                    }
                }
            },

            // Columns from child datasources
            self.child_datasources.iter()
                .map(|child_datasource| {
                    let child_datasource_alias = child_datasource.get_alias();
                    format!("{child_datasource_alias}.*")
                })
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),

            self.datasource.get_schema_oid()?,

            // Join to multiselect table, if applicable
            match &self.datasource {
                Datasource::Column { column, .. } => {
                    match column.column_type {
                        column_type::ColumnType::Multiselect { .. } => 
                            format!(
                                "INNER JOIN MULTISELECT{} m ON m.TABLE{}_OID = t.OID", 
                                column.oid, 
                                self.datasource.get_schema_oid()?
                            ),
                        _ => String::from("")
                    }
                }
                _ => String::from("")
            },

            // Joins to child datasources
            {
                let mut child_datasource_joins: String = String::from("");
                for child_datasource in self.child_datasources.iter() {
                    let child_datasource_alias: String = child_datasource.get_alias();
                    match child_datasource {
                        Datasource::MasterTable { .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.PARENT_{}_OID = t.OID",
                                self.datasource.get_alias()
                            );
                        }
                        Datasource::InheritorTable { table_oid, .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = t.MASTER{table_oid}_OID"
                            );
                        }
                        Datasource::Column { column, .. } => {
                            child_datasource_joins = format!(
                                "{child_datasource_joins} LEFT JOIN {} ON {}",
                                child_datasource.get_alias(),
                                match column.column_type {
                                    column_type::ColumnType::Multiselect { .. } => format!(
                                        "{child_datasource_alias}.PARENT_{}_OID = t.OID",
                                        self.datasource.get_alias()
                                    ),
                                    column_type::ColumnType::Object { table_oid, .. }
                                    | column_type::ColumnType::Select { table_oid, .. } => {
                                        if column.schema.oid == self.datasource.get_schema_oid()? {
                                            // Normal direction
                                            format!(
                                                "{child_datasource_alias}.{child_datasource_alias}_OID = t.COLUMN{}",
                                                column.oid
                                            )
                                        } else {
                                            // Inverted direction
                                            format!(
                                                "{child_datasource_alias}.PARENT_{}_OID = t.OID",
                                                self.datasource.get_alias()
                                            )
                                        }
                                    }
                                    _ => {
                                        return Err(Error::AdhocError("Datasource cannot be derived from a non-Select, non-Object, non-Multiselect column!"));
                                    }
                                }
                            );
                        }
                        _ => {
                            return Err(Error::AdhocError("Child datasource cannot be a root table!"));
                        }
                    }
                }
                child_datasource_joins
            }
        ))
    }

    /// Gets all columns, from both this CTE and all child datasource CTEs.
    fn get_all_columns(&self, select_constructor: &SelectConstructor) -> Vec<DatasourceCteColumn> {
        let mut columns = Vec::from_iter(self.columns.values().map(|c| c.clone()));
        for child_datasource in self.child_datasources.iter() {
            columns.splice(columns.len()..columns.len(), select_constructor.cte_datasource[&child_datasource.get_alias()].get_all_columns(select_constructor));
        }
        return columns;
    }

    /// Adds a primitive column to the CTE.
    /// Assumes that the column is owned by the schema of this datasource.
    fn add_primitive_column(&mut self, column_oid: i64, prim: column_type::Primitive) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds an object column to the CTE.
    fn add_object_column(&mut self, column_oid: i64, table_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a select column to the CTE.
    fn add_select_column(&mut self, column_oid: i64, table_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("t.COLUMN{column_oid}"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }

    /// Adds a multiselect column to the CTE.
    fn add_multiselect_column(&mut self, column_oid: i64, table_oid: i64) -> DatasourceCteColumn {
        if !self.columns.contains_key(&column_oid) {
            let datasource_alias: String = self.datasource.get_alias();
            self.columns.insert(column_oid, DatasourceCteColumn {
                value_expr: format!("(GROUP_CONCAT(CAST({datasource_alias}_COLUMN{column_oid}_OID AS TEXT), ',') OVER (PARTITION BY t.OID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))"),
                value_ord: format!("{datasource_alias}_COLUMN{column_oid}")
            });
        }
        return self.columns[&column_oid].clone();
    }
}


#[derive(Clone)]
struct SelectParameterType {
    /// The primitive types that the parameter can conform to.
    primitive_types: HashSet<column_type::Primitive>
}

impl SelectParameterType {
    /// Creates a new type representing a null value.
    fn new() -> Self {
        Self {
            primitive_types: HashSet::new()
        }
    }

    /// Creates a new type representing a specific primitive type.
    fn from(prim: column_type::Primitive) -> Self {
        Self {
            primitive_types: HashSet::from_iter(match &prim {
                column_type::Primitive::Datetime => vec![
                    column_type::Primitive::Date, 
                    prim
                ],
                column_type::Primitive::PlainText => vec![
                    column_type::Primitive::JsonText, 
                    prim
                ],
                column_type::Primitive::Number => vec![
                    column_type::Primitive::Integer, 
                    column_type::Primitive::Datetime, 
                    column_type::Primitive::Date, 
                    column_type::Primitive::Boolean,
                    prim
                ],
                column_type::Primitive::Integer => vec![
                    column_type::Primitive::Boolean,
                    prim
                ],
                column_type::Primitive::File => vec![
                    column_type::Primitive::Image, 
                    prim
                ],
                _ => vec![prim]
            })
        }
    }

    /// Constructs a type that represents the most specific type that encompasses both this type and the given type.
    fn generalize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.union(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Constructs a type that represents the most general type that conforms to both this type and the given type.
    fn specialize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.intersection(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Returns true if an instance of the given type can always be passed as a value of this type.
    fn encompasses(&self, other: &Self) -> bool {
        self.primitive_types.is_superset(&(other.primitive_types))
    }


    /// Returns true if a value of this type can be text.
    fn is_text_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::PlainText)
            || self.primitive_types.contains(&column_type::Primitive::JsonText);
    }

    /// Returns true if a value of this type can be numeric.
    fn is_numeric_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::Number)
            || self.primitive_types.contains(&column_type::Primitive::Datetime)
            || self.primitive_types.contains(&column_type::Primitive::Date)
            || self.primitive_types.contains(&column_type::Primitive::Integer)
            || self.primitive_types.contains(&column_type::Primitive::Boolean);
    }

    /// Returns true if a value of this type can be a file.
    fn is_file_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::File)
            || self.primitive_types.contains(&column_type::Primitive::Image);
    }


    /// Constructs an expression for a value's label.
    /// This should be used in cases where an operation combines two or more values, and not in cases where a value is selected from a list.
    fn construct_plain_label_expr(&self, value_expr: &String) -> String {
        // Check if pure file
        if self.is_file_type() && !self.is_text_type() && !self.is_numeric_type() {
            return format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = {value_expr})");
        }

        // Check if pure text
        if self.is_text_type() && !self.is_file_type() && !self.is_numeric_type() {
            return value_expr.clone();
        }
        
        // Check if pure number
        if self.is_numeric_type() && !self.is_file_type() && !self.is_text_type() {
            if self.primitive_types.contains(&column_type::Primitive::Number) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Datetime) {
                return format!("STRFTIME('%FT%TZ', {value_expr}, 'julianday')");
            } else if self.primitive_types.contains(&column_type::Primitive::Date) {
                return format!("DATE({value_expr}, 'julianday')");
            } else if self.primitive_types.contains(&column_type::Primitive::Integer) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Boolean) {
                return format!("IF({value_expr}, 'true', {value_expr} IS NULL, NULL, 'false')")
            }
        }

        // Mixed, unknown type
        return format!("CAST({value_expr} AS TEXT)");
    }

    /// Constructs an expression for a value's label.
    /// This should be used in cases where an operation combines two or more values, and not in cases where a value is selected from a list.
    fn construct_json_label_expr(&self, value_expr: &String) -> String {
        // Check if pure file
        if self.is_file_type() && !self.is_text_type() && !self.is_numeric_type() {
            return format!("'\"' || (SELECT REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_FILE_VIEW f WHERE f.OID = {value_expr}) || '\"'");
        }

        // Check if pure text
        if self.is_text_type() && !self.is_file_type() && !self.is_numeric_type() {
            if self.primitive_types.contains(&column_type::Primitive::JsonText) && !self.primitive_types.contains(&column_type::Primitive::PlainText) {
                return value_expr.clone();
            } else {
                return format!("'\"' || REPLACE(REPLACE({value_expr}, '\\', '\\\\'), '\"', '\\\"') || '\"'");
            }
        }
        
        // Check if pure number
        if self.is_numeric_type() && !self.is_file_type() && !self.is_text_type() {
            if self.primitive_types.contains(&column_type::Primitive::Number) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Datetime) {
                return format!("'\"' || STRFTIME('%FT%TZ', {value_expr}, 'julianday') || '\"'");
            } else if self.primitive_types.contains(&column_type::Primitive::Date) {
                return format!("'\"' || DATE({value_expr}, 'julianday') || '\"'");
            } else if self.primitive_types.contains(&column_type::Primitive::Integer) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Boolean) {
                return format!("IF({value_expr}, 'true', {value_expr} IS NULL, NULL, 'false')")
            }
        }

        // Mixed, unknown type
        return format!("'\"' || REPLACE(REPLACE(CAST({value_expr} AS TEXT), '\\', '\\\\'), '\"', '\\\"') || '\"'");
    }

    /// Describes the type.
    fn to_string(&self) -> String {
        let mut temp = self.primitive_types.clone();
        if temp.contains(&column_type::Primitive::Datetime) {
            temp.remove(&column_type::Primitive::Date);
        }
        if temp.contains(&column_type::Primitive::PlainText) {
            temp.remove(&column_type::Primitive::JsonText);
        }
        if temp.contains(&column_type::Primitive::Number) {
            temp.remove(&column_type::Primitive::Integer);
        }
        if temp.contains(&column_type::Primitive::File) {
            temp.remove(&column_type::Primitive::Image);
        }
        temp.into_iter()
            .map(|prim| String::from(prim.to_str()))
            .reduce(|acc, e| format!("{acc} | {e}"))
            .unwrap_or(String::from("null"))
    }
}

struct SelectParameter {
    plain_label_expr_norecursion: String,
    plain_label_expr_recursion: String,
    json_label_expr_norecursion: String,
    json_label_expr_recursion: String,
    value_expr_norecursion: String,
    value_expr_recursion: String,
    cell_expr: String,
    isolated_dependency_exprs: HashSet<String>,
    full_reload_dependency_exprs: HashSet<String>,
    scalar_type: SelectParameterType,
    context: SelectParameterContext
}

impl SelectParameter {
    /// Constructs a new scalar parameter with no recursion.
    fn new_norecursion(plain_label_expr: String, json_label_expr: String, value_expr: String, cell_expr: String, isolated_dependency_exprs: HashSet<String>, full_reload_dependency_exprs: HashSet<String>, scalar_type: SelectParameterType, context: SelectParameterContext) -> Self {
        Self {
            plain_label_expr_norecursion: plain_label_expr.clone(),
            plain_label_expr_recursion: plain_label_expr,
            json_label_expr_norecursion: json_label_expr.clone(),
            json_label_expr_recursion: json_label_expr,
            value_expr_norecursion: value_expr.clone(),
            value_expr_recursion: value_expr,
            cell_expr,
            isolated_dependency_exprs,
            full_reload_dependency_exprs,
            scalar_type,
            context
        }
    }
}

#[derive(Clone)]
enum SelectParameterSlice {
    None,
    NthValue(String)
}

#[derive(Clone)]
enum SelectParameterContext {
    /// A scalar value.
    Scalar,

    /// A collection.
    /// Induced by aggregate functions, IN operators, and literal Lists.
    Collection {
        /// How the collection is sliced, if at all.
        /// Applies to base case of expressions.
        slice_norecursion: SelectParameterSlice,

        /// How the collection is sliced, if at all.
        /// Applies to recursive case of expressions.
        slice_recursion: SelectParameterSlice,

        /// The expression used to filter the collection.
        /// Applies to base case of expressions.
        filter_expr_norecursion: Option<String>,

        /// The expression used to filter the collection.
        /// Applies to recursive case of expressions.
        filter_expr_recursion: Option<String>,

        /// The first item in the tuple is the expression that is sorted over.
        /// The second item in the tuple is true if the order is ascending, and false if descending.
        /// Applies to base case of expressions.
        order_exprs_norecursion: Vec<(String, bool)>,

        /// The first item in the tuple is the expression that is sorted over.
        /// The second item in the tuple is true if the order is ascending, and false if descending.
        /// Applies to recursive case of expressions.
        order_exprs_recursion: Vec<(String, bool)>,

        /// The datasource representing the minimum depth excluded from the grouping.
        /// The keys are the root datasource OIDs.
        /// Applies to base case of expressions.
        min_depth: HashMap<i64, Option<Datasource>>,

        /// True if changes to the window (e.g. filters, ordering, indexing) are disabled. False if modifications are still permitted.
        window_changes_disabled: bool 
    }
}

impl SelectParameterContext {
    fn wrap_collection(inner_expr: String, slice: &SelectParameterSlice, filter_expr: &Option<String>, order_exprs: &Vec<(String, bool)>, min_depth: &HashMap<i64, Option<Datasource>>) -> String {
        format!(
            "({} {} OVER ({} {}))",

            // Wraps the inner expression in the window function
            match slice {
                SelectParameterSlice::None => inner_expr,
                SelectParameterSlice::NthValue(n_expr) => format!("NTH_VALUE({inner_expr}, {n_expr} + 1)")
            },

            // Filters based on the filter expression
            match filter_expr {
                Some(filter_expr) => format!("FILTER (WHERE {filter_expr})"),
                None => String::from("")
            },

            // Partition based on the minimum datasource depths that are excluded
            if min_depth.len() > 0 {
                format!(
                    "PARTITION BY {}",
                    min_depth.values()
                        .filter_map(|d| if let Some(d) = d { Some(format!("{}_OID", d.get_alias())) } else { None })
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            },

            // Order by the ordering expressions
            if order_exprs.len() > 0 {
                format!(
                    "ORDER BY {}",
                    order_exprs.iter()
                        .map(|(order_expr, order_dir)| format!("{order_expr} {}", if *order_dir { "ASC" } else { "DESC" }))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            }
        )
    }

    /// Wraps an expression in the context.
    fn wrap(&self, inner_expr_norecursion: String, inner_expr_recursion: String) -> (String, String) {
        match self {
            Self::Scalar => (inner_expr_norecursion, inner_expr_recursion),
            Self::Collection { slice_norecursion, slice_recursion, filter_expr_norecursion, filter_expr_recursion, order_exprs_norecursion, order_exprs_recursion, min_depth, .. } => {
                (
                    Self::wrap_collection(inner_expr_norecursion, slice_norecursion, filter_expr_norecursion, order_exprs_norecursion, min_depth),
                    Self::wrap_collection(inner_expr_recursion, slice_recursion, filter_expr_recursion, order_exprs_recursion, min_depth)
                )
            }
        }
    }

    /// Disables changes to the window.
    fn disable_window_changes(&mut self) {
        if let Self::Collection { mut window_changes_disabled, .. } = self {
            window_changes_disabled = true;
        }
    }
}

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
                    if let Some(root_datasource) = Datasource::get_default_datasource_transact(trans, schema_oid.clone())? {
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
                    let json_expr_norecursion: String = if columns.len() > 0 {
                        format!(
                            "'{{ ' || GROUP_CONCAT(({}), ', ') || ' }}'",
                            columns.iter()
                                .filter_map(|col| {
                                    if col.is_required {
                                        Some(format!("SELECT {}", col.json_expr_norecursion))
                                    } else {
                                        None 
                                    }
                                })
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    } else {
                        String::from("NULL")
                    };

                    let plain_expr_recursion: String = if columns.len() == 1 {
                        columns[0].plain_expr_recursion.clone()
                    } else {
                        String::from("NULL")
                    };
                    let json_expr_recursion: String = if columns.len() > 0 {
                        format!(
                            "'{{ ' || GROUP_CONCAT(({}), ', ') || ' }}'",
                            columns.iter()
                                .filter_map(|col| {
                                    if col.is_required {
                                        Some(format!("SELECT {}", col.json_expr_recursion))
                                    } else {
                                        None 
                                    }
                                })
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                .unwrap()
                        )
                    } else {
                        String::from("NULL")
                    };

                    match Datasource::get_default_datasource_transact(trans, schema_oid.clone())? {
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


#[derive(Clone)]
struct SelectDatasource {
    /// The datasource being selected from.
    datasource: Datasource,

    /// Replaces the root datasource with the given OID by the provided datasource. 
    /// Used if a formula uses another formula as a parameter, etc.
    replace_root: i64,

    /// The alias of the CTE being pulled from.
    /// Recursive if not "w".
    alias: String 
}

impl SelectDatasource {
    /// Constructs a new non-recursive datasource.
    fn new_norecursion(datasource: Datasource, replace_root: i64) -> Self {
        Self {
            datasource,
            replace_root,
            alias: String::from("w")
        }
    }

    /// Constructs a new recursive datasource.
    fn new_recursion(datasource: Datasource, replace_root: i64, alias: String) -> Self {
        Self {
            datasource,
            replace_root,
            alias 
        }
    }

    /// Returns true if the datasource is recursive, and false otherwise.
    fn is_recursive(&self) -> bool {
        self.alias != "w"
    }

    /// Constructs an expression to get the OID of the datasource.
    fn get_oid_expr(&self) -> String {
        format!("{}.{}_OID", self.alias, self.datasource.get_alias())
    }

    /// Constructs an expression to get the table OID (accounting for inheritance) of the datasource.
    fn get_schema_expr(&self) -> String {
        format!("{}.{}_SCHEMA", self.alias, self.datasource.get_alias())
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

        let root_datasource: Option<Datasource> = Datasource::get_default_datasource_transact(trans, schema_oid)?;

        // Add all inheritor datasources
        if let Some(root_datasource) = &root_datasource {           
            select_constructor.add_datasource(root_datasource.clone(), false);

            // Add datasource for each inheritor table
            for row_result in trans.prepare("SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get("INHERITOR_DATASOURCE_PATH"))? {
                let inheritor_datasource_path: String = row_result?;
                select_constructor.add_datasource(
                    root_datasource.append_path(inheritor_datasource_path)?, 
                    false
                );
            }
        }

        for row_result in trans.prepare("SELECT COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 ORDER BY IS_SUBREPORT ASC")?.query_map(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
            let column_oid = row_result?;
            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
            let column_type: column_type::ColumnType = column.column_type.clone();
            let param: SelectParameter = match &root_datasource {
                Some(root_datasource) => {
                    let root_datasource: SelectDatasource = SelectDatasource::new_norecursion(root_datasource.clone(), schema_oid.clone());
                    select_constructor.add_concrete_parameter(trans, root_datasource, column, SelectParameterContext::Scalar)?    
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
        }

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

        let root_datasource: Option<Datasource> = Datasource::get_default_datasource_transact(trans, schema_oid)?;

        // Add all inheritor datasources
        if let Some(root_datasource) = &root_datasource {    
            select_constructor.add_datasource(root_datasource.clone(), false);

            // Add datasource for each inheritor table
            for row_result in trans.prepare("SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get("INHERITOR_DATASOURCE_PATH"))? {
                let inheritor_datasource_path: String = row_result?;
                select_constructor.add_datasource(
                    root_datasource.append_path(inheritor_datasource_path)?, 
                    false
                );
            }
        }

        for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING, IS_REQUIRED FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?, row.get::<_, bool>("IS_REQUIRED")?)))? {
            let (column_oid, ordering, is_required) = row_result?;
            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
            let json_safe_column_name: String = json_encode_string(&column.name);
            let param: SelectParameter = match &root_datasource {
                Some(root_datasource) => {
                    let root_datasource: SelectDatasource = SelectDatasource::new_norecursion(root_datasource.clone(), schema_oid.clone());
                    select_constructor.add_concrete_parameter(trans, root_datasource, column, SelectParameterContext::Scalar)?    
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
        }

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

    /// Adds a CTE for a datasource to the SELECT statement.
    fn add_datasource(&mut self, datasource: Datasource, is_collection: bool) {
        if let Some(parent_datasource) = datasource.get_parent() {
            let parent_datasource_alias: String = parent_datasource.get_alias();
            self.add_datasource(parent_datasource, is_collection);
            if let Some(parent_datasource_cte) = self.cte_datasource.get_mut(&parent_datasource_alias) {
                parent_datasource_cte.child_datasources.insert(datasource.clone());
            }
        }

        let datasource_alias: String = datasource.get_alias();
        if !self.cte_datasource.contains_key(&datasource_alias) {
            self.cte_datasource.insert(datasource_alias, DatasourceCteConstructor { 
                datasource, 
                columns: HashMap::new(), 
                child_datasources: HashSet::new(),
                is_always_collection: is_collection
            });
        } else {
            if let Some(datasource_cte) = self.cte_datasource.get_mut(&datasource_alias) {
                datasource_cte.is_always_collection = datasource_cte.is_always_collection && is_collection;
            }
        }
    }

    /// Adds a column on a datasource as a parameter to this SELECT statement.
    /// Make sure to add Subreport columns after all other columns.
    fn add_concrete_parameter(&mut self, trans: &Transaction, datasource: SelectDatasource, column: column::FullMetadata, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
        match &mut context {
            SelectParameterContext::Scalar => {
                self.add_datasource(datasource.datasource.clone(), false);
            }
            SelectParameterContext::Collection { min_depth, .. } => {
                let datasource: Datasource = datasource.datasource.clone();
                self.add_datasource(datasource.clone(), true);
                
                // Check if the minimum depth has changed
                let root_oid: i64 = if let Datasource::Table { oid, .. } = datasource.seek_root() { oid } else { return Err(Error::AdhocError("Root datasource was not a table.")); };
                if let Some(former_min_depth) = min_depth.get_mut(&root_oid) {
                    if let Some(former_min_depth_inner) = former_min_depth {
                        *former_min_depth = datasource.find_commonality(former_min_depth_inner);
                    }
                } else {
                    min_depth.insert(root_oid, datasource.get_parent());
                }
            }
        }
        
        let cell_expr: String = format!(
            "('{}:{}:{}:' || CAST(w.{}_OID AS TEXT))",
            column.column_type.to_str(),
            column.schema.oid,
            column.oid,
            datasource.datasource.get_alias()
        );
                    
        let (isolated_dependency_exprs, full_reload_dependency_exprs): (HashSet<String>, HashSet<String>) = {
            match context {
                SelectParameterContext::Scalar => {
                    let mut isolated_dependency_exprs: HashSet<String> = HashSet::new();
                    let mut full_reload_dependency_exprs: HashSet<String> = HashSet::new();
                    let dependent_basis_datasource_alias = datasource.datasource.seek_basis()?.get_alias();
                    for dependent_datasource in datasource.datasource.linearize() {
                        let dependent_datasource_alias: String = dependent_datasource.get_alias();
                        let dependent_datasource_table_oid: i64 = dependent_datasource.get_schema_oid()?;
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
                    for dependent_datasource in datasource.datasource.linearize() {
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
        
        match column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.datasource.get_alias()) {
                    let cte_column = cte.add_primitive_column(column.oid, prim.clone());
                    let scalar_type = SelectParameterType::from(prim);
                    
                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let plain_label_expr: String = scalar_type.construct_plain_label_expr(&value_expr);
                    let json_label_expr: String = scalar_type.construct_json_label_expr(&value_expr);

                    return Ok(SelectParameter::new_norecursion(
                        plain_label_expr, 
                        json_label_expr, 
                        value_expr, 
                        cell_expr,
                        isolated_dependency_exprs,
                        full_reload_dependency_exprs,
                        scalar_type, 
                        context
                    ));
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.datasource.get_alias()) {
                    let cte_column = cte.add_object_column(column.oid, table_oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
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
                    let cte_column = cte.add_select_column(column.oid, table_oid);

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
                    let cte_column = cte.add_multiselect_column(column.oid, table_oid);

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
                        for row_result in trans.prepare(&pragma_sql)?.query_map([], |row| row.get("NAME"))? {
                            let oid_column_name: String = row_result?;
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
                        }

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
                        let subreport_datasource: SelectDatasource = datasource.clone();

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
                        let mut params: HashMap<column::FullMetadata, SelectParameter> = HashMap::new();
                        for row_result in trans.prepare("SELECT COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED ORDER BY IS_SUBREPORT ASC")?.query_map(params![report_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
                            let column_oid = row_result?;
                            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                            // Insert the parameter with no datasource
                            let param: SelectParameter = self.add_concrete_parameter(trans, subreport_datasource.clone(), column.clone(), param_context)?;
                            param_context = param.context.clone();
                            params.insert(column, param);
                        }

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
        return Err(Error::AdhocError("Unable to add parameter."));
    }

    /// Adds a column on a report as a parameter to this SELECT statement.
    fn add_virtual_parameter(&mut self, trans: &Transaction, column: column::FullMetadata, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
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
                let mut params: HashMap<column::FullMetadata, SelectParameter> = HashMap::new();
                for row_result in trans.prepare("SELECT COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED ORDER BY IS_SUBREPORT ASC")?.query_map(params![report_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
                    let column_oid = row_result?;
                    let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

                    // Insert the parameter with no datasource
                    let param: SelectParameter = self.add_virtual_parameter(trans, column.clone(), param_context)?;
                    param_context = param.context.clone();
                    params.insert(column, param);
                }

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
                return Err(Error::AdhocError("Unable to add virtual parameter - column type belongs to a table."));
            }
        }
    }


    /// Constructs a label for an Object column.
    /// The first item of the returned tuple is the non-recursive plain label. (Always NULL, since the label for an Object is always JSON.)
    /// The second item of the returned tuple is the recursive plain label. (Always NULL, since the label for an Object is always JSON.)
    /// The third item of the returned tuple is the non-recursive JSON label.
    /// The fourth item of the returned tuple is the recursive JSON label.
    fn construct_object_label(&mut self, trans: &Transaction, datasource: SelectDatasource, object_column_oid: i64, object_table_oid: i64, value_expr: &String, is_collection: bool) -> Result<(String, String, String, String), Error> {
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
                    let looped_datasource_schema_oid: i64 = looped_datasource.get_schema_oid()?;
                    if looped_datasource_schema_oid == object_table_oid {
                        //
                        // This meets the condition set above, so we have confirmed the Object column induces recursion in the label
                        // 

                        // First, we note where the recursion occurred, and where it should loop backwards to
                        let recursive_datasource: SelectDatasource = SelectDatasource::new_recursion(
                            looped_datasource, 
                            if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, object_table_oid)? {
                                oid
                            } else {
                                return Err(Error::AdhocError("No default datasource for table."));
                            }, 
                            datasource.datasource.get_alias()
                        );
                        recursions.push((value_expr.clone(), recursive_datasource.get_oid_expr()));
                        
                        // Add datasource for each inheritor table
                        for row_result in trans.prepare("SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![object_table_oid], |row| row.get("INHERITOR_DATASOURCE_PATH"))? {
                            let inheritor_datasource_path: String = row_result?;
                            self.add_datasource(
                                recursive_datasource.datasource.append_path(inheritor_datasource_path)?, 
                                is_collection
                            );
                        }

                        // Construct labels for each key column on table referenced by Object, including non-required columns
                        let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                        for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![object_table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)))? {
                            let (column_oid, ordering) = row_result?;
                            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                            let json_safe_column_name: String = json_encode_string(&column.name);
                            
                            let param = self.add_concrete_parameter(trans, recursive_datasource.clone(), column, SelectParameterContext::Scalar)?;
                            key_columns.push((json_safe_column_name, param, ordering));
                        }
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

                let object_datasource = SelectDatasource {
                    datasource: datasource.datasource.append_path(format!("_COLUMN{object_column_oid}"))?,
                    replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, object_table_oid)? {
                        oid
                    } else {
                        return Err(Error::AdhocError("No default datasource for table."));
                    },
                    alias: datasource.alias 
                };

                // Add datasource for each inheritor table
                for row_result in trans.prepare("SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![object_table_oid], |row| row.get("INHERITOR_DATASOURCE_PATH"))? {
                    let inheritor_datasource_path: String = row_result?;
                    self.add_datasource(
                        datasource.datasource.append_path(inheritor_datasource_path)?, 
                        is_collection
                    );
                }

                // Construct labels for each key column on table referenced by Object, including non-required columns
                let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![object_table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)))? {
                    let (column_oid, ordering) = row_result?;
                    let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                    let json_safe_column_name: String = json_encode_string(&column.name);
                            
                    let param = self.add_concrete_parameter(trans, object_datasource.clone(), column, SelectParameterContext::Scalar)?;
                    key_columns.push((json_safe_column_name, param, ordering));
                }
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
    fn construct_select_label(&mut self, trans: &Transaction, datasource: SelectDatasource, object_column_oid: i64, object_table_oid: i64, value_expr: &String, is_collection: bool) -> Result<(String, String, String, String), Error> {
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
                    let looped_datasource_schema_oid: i64 = looped_datasource.get_schema_oid()?;
                    if looped_datasource_schema_oid == object_table_oid {
                        //
                        // This meets the condition set above, so we have confirmed the Select/Multiselect column induces recursion in the label
                        // 

                        // First, we note where the recursion occurred, and where it should loop backwards to
                        let recursive_datasource: SelectDatasource = SelectDatasource::new_recursion(
                            looped_datasource, 
                            if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, object_table_oid)? {
                                oid
                            } else {
                                return Err(Error::AdhocError("No default datasource for table."));
                            }, 
                            datasource.datasource.get_alias()
                        );
                        recursions.push((value_expr.clone(), recursive_datasource.get_oid_expr()));

                        // Construct labels for each key column on table referenced by Object, including non-required columns
                        let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                        for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![object_table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)))? {
                            let (column_oid, ordering) = row_result?;
                            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                            let json_safe_column_name: String = json_encode_string(&column.name);
                            
                            let param = self.add_concrete_parameter(trans, recursive_datasource.clone(), column, SelectParameterContext::Scalar)?;
                            key_columns.push((json_safe_column_name, param, ordering));
                        }
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

                let object_datasource = SelectDatasource {
                    datasource: datasource.datasource.append_path(format!("_COLUMN{object_column_oid}"))?,
                    replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, object_table_oid)? {
                        oid
                    } else {
                        return Err(Error::AdhocError("No default datasource for table."));
                    },
                    alias: datasource.alias 
                };

                // Add datasource for each inheritor table
                for row_result in trans.prepare("SELECT INHERITOR_DATASOURCE_PATH FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![object_table_oid], |row| row.get("INHERITOR_DATASOURCE_PATH"))? {
                    let inheritor_datasource_path: String = row_result?;
                    self.add_datasource(
                        datasource.datasource.append_path(inheritor_datasource_path)?, 
                        is_collection
                    );
                }

                // Construct labels for each key column on table referenced by Object, including non-required columns
                let mut key_columns: Vec<(String, SelectParameter, i64)> = Vec::new();
                for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![object_table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?)))? {
                    let (column_oid, ordering) = row_result?;
                    let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                    let json_safe_column_name: String = json_encode_string(&column.name);
                            
                    let param = self.add_concrete_parameter(trans, object_datasource.clone(), column, SelectParameterContext::Scalar)?;
                    key_columns.push((json_safe_column_name, param, ordering));
                }
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
    fn construct_formula(&mut self, trans: &Transaction, datasource: Option<SelectDatasource>, formula: Box<Formula>, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
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
                    return Err(Error::AdhocError("A literal List cannot be returned in a scalar context!"));
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
                let column_datasource: SelectDatasource = match datasource {
                    Some(datasource) => { // Formula belongs to a table
                        let column_datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?
                            .substitute_root(datasource.replace_root, datasource.datasource);
                        SelectDatasource {
                            replace_root: if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, column_datasource.get_schema_oid()?)? {
                                oid
                            } else {
                                return Err(Error::AdhocError("No default datasource for table."));
                            },
                            datasource: column_datasource,
                            alias: datasource.alias
                        }
                    }

                    None => { // Formula belongs to a report
                        let column_datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?;
                        let column_datasource_schema_oid: i64 = column_datasource.get_schema_oid()?;
                        // Since a parameter must belong to a table, we are assured that there is no recursion occurring at this stage
                        SelectDatasource::new_norecursion(
                            column_datasource,
                            if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, column_datasource_schema_oid)? {
                                oid
                            } else {
                                return Err(Error::AdhocError("No default datasource for table."));
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




struct ViewsToCreate {
    /// True if the main view needs to be created. False otherwise.
    create_main_view: bool,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool
}

/// Drop the views associated with a schema.
fn drop_views(
    trans: &Transaction,
    schema_oid: i64,
    drop_main: bool,
    drop_label: bool,
    views_to_create: &mut HashMap<i64, ViewsToCreate>,
) -> Result<(), Error> {
    if views_to_create.contains_key(&schema_oid) {
        if (!views_to_create[&schema_oid].create_main_view && drop_main) || (!views_to_create[&schema_oid].create_label_view && drop_label) {
            // Some new information is being added
            if let Some(view_to_create) = views_to_create.get_mut(&schema_oid) {
                view_to_create.create_main_view = drop_main.clone();
                view_to_create.create_label_view = drop_label.clone();
            }
        } else {
            // No new information is being added
            return Ok(());
        }
    } else {
        views_to_create.insert(schema_oid, ViewsToCreate { 
            create_main_view: drop_main.clone(), 
            create_label_view: drop_label.clone() 
        });
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        let inheritor_schema_oid: i64 = row_result?;
        drop_views(trans, inheritor_schema_oid, true, true, views_to_create)?;
    }

    // Drop only the label views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let master_schema_oid: i64 = row_result?;
        drop_views(trans, master_schema_oid, false, true, views_to_create)?;
    }

    if drop_label {
        // Drop the main views that use the label view, or label views that are dependent on the schema where the label view is being dropped
        for row_result in trans
            .prepare(
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
            )?
            .query_map(params![schema_oid], |row| {
                Ok((
                    row.get::<_, i64>("SCHEMA_OID")?,
                    row.get::<_, bool>("IS_PRIMARY_KEY")?,
                ))
            })?
        {
            let (referencing_schema_oid, referenced_in_label) = row_result?;
            drop_views(
                trans,
                referencing_schema_oid,
                true,
                referenced_in_label,
                views_to_create
            )?;
        }
    }

    // Drop the associated views
    let drop_sql: String = format!(
        "{}{}",
        if drop_main {
            format!("DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW;")
        } else {
            String::from("")
        },
        if drop_label {
            format!("DROP VIEW IF EXISTS SCHEMA{schema_oid}_LABEL_VIEW;")
        } else {
            String::from("")
        }
    );
    trans.execute_batch(&drop_sql)?;
    Ok(())
}

/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    let mut views_to_create: HashMap<i64, ViewsToCreate> = HashMap::new();
    drop_views(trans, schema_oid, true, true, &mut views_to_create)?;

    // Create all of the label views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_label_view {
            let select_constructor: SelectConstructor = SelectConstructor::new_label(trans, view_schema_oid.clone())?;
            let sql_create: String = format!(
                "CREATE VIEW SCHEMA{view_schema_oid}_LABEL_VIEW AS {}",
                select_constructor.build(trans)?
            );
            println!("{sql_create}");
            trans.execute(&sql_create, [])?;
        }
    }

    // Create all of the main views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_main_view {
            let select_constructor: SelectConstructor = SelectConstructor::new_main(trans, view_schema_oid.clone())?;
            let sql_create: String = format!(
                "CREATE VIEW SCHEMA{view_schema_oid}_VIEW AS {}",
                select_constructor.build(trans)?
            );
            println!("{sql_create}");
            trans.execute(&sql_create, [])?;
        }
    }
    Ok(())
}
