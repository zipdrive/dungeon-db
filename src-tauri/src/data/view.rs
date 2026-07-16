use crate::{
    data::{column, column_type, datasource::Datasource, schema, table}, util::{error::Error, formula::Formula},
};
use bitflags::bitflags;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::collections::{HashMap, HashSet};
use regex::Regex;



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


    /// Constructs an expression for a label.
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

    fn construct_json_label_expr(&self, value_expr: &String) -> String {
        // Check if pure file
        if self.is_file_type() && !self.is_text_type() && !self.is_numeric_type() {
            return format!("'\"' || (SELECT REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_FILE_VIEW f WHERE f.OID = {value_expr}) || '\"'");
        }

        // Check if pure text
        if self.is_text_type() && !self.is_file_type() && !self.is_numeric_type() {
            return format!("'\"' || REPLACE(REPLACE({value_expr}, '\\', '\\\\'), '\"', '\\\"') || '\"'");
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
    label_expr_norecursion: String,
    label_expr_recursion: String,
    value_expr_norecursion: String,
    value_expr_recursion: String,
    cell_expr_norecursion: String,
    cell_expr_recursion: String,
    scalar_type: SelectParameterType,
    context: SelectParameterContext
}

impl SelectParameter {
    /// Constructs a new scalar parameter with no recursion.
    fn new_norecursion(label_expr: String, value_expr: String, cell_expr: String, scalar_type: SelectParameterType, context: SelectParameterContext) -> Self {
        Self {
            label_expr_norecursion: label_expr.clone(),
            label_expr_recursion: label_expr,
            value_expr_norecursion: value_expr.clone(),
            value_expr_recursion: value_expr,
            cell_expr_norecursion: cell_expr.clone(),
            cell_expr_recursion: cell_expr,
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
        cell_ord: String 
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
    fn build(&self, cte_list: Vec<String>, oid_list: Vec<String>) -> Result<String, Error> {
        Ok(match self {
            Self::SelectMainConstructor { columns } => {
                format!(
                    "WITH {} SELECT {} FROM WRAPPER w", 
                    
                    // All of the CTEs, including the wrapper
                    cte_list.join(", "),

                    // Select each column from the wrapper
                    oid_list.iter().map(|oid| format!("w.{oid}"))
                        .chain(
                            columns.iter().map(|col| match col {
                                SelectMainColumn::Cell { value_expr, value_ord, label_expr, label_ord } => 
                                    format!("{value_expr} AS {value_ord}, {label_expr} AS {label_ord}"),
                                SelectMainColumn::Formula { value_expr, value_ord, label_expr, label_ord, cell_expr, cell_ord } => 
                                    format!("{value_expr} AS {value_ord}, {label_expr} AS {label_ord}, {cell_expr} AS {cell_ord}")
                            })
                        )
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap_or(String::from("NULL AS COLUMN1"))
                )
            }
            Self::SelectLabelConstructor { schema_oid, recursions, columns } => {
                // Assume columns are already sorted
                //columns.sort_by_key(|col| col.ordering);

                let plain_expr_norecursion: String = if columns.len() == 1 {
                    columns[0].plain_expr_norecursion.clone()
                } else {
                    String::from("NULL")
                };
                let json_expr_norecursion: String = if columns.len() > 1 {
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
                let object_expr_norecursion: String = format!(
                    "'{{ \"' || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) || '\": ' || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', 'null') || ' }}'",

                    // The OID of the schema
                    // If the schema is a table, this is the inheritor schema OID
                    // If the schema is a report, this is the report's schema OID
                    schema_oid,

                    // The key columns of the schema
                    columns.iter()
                        .map(|col| format!("SELECT {}", col.json_expr_norecursion))
                        .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                        .unwrap()
                );
                let oid_columns: String = oid_list.iter().fold(String::from(""), |acc, e| format!("{acc}, w.{e}"));
                
                if recursions.len() > 0 {
                    // Need to make a recursive CTE
                    let plain_expr_recursion: String = if columns.len() == 1 {
                        columns[0].plain_expr_recursion.clone()
                    } else {
                        String::from("NULL")
                    };
                    let json_expr_recursion: String = if columns.len() > 1 {
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
                    let object_expr_recursion: String = format!(
                        "'{{ \"' || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) || '\": ' || COALESCE('{{ ' || GROUP_CONCAT(({}), ', ') || ' }}', '{{ }}') || ' }}'",

                        // The OID of the schema
                        // If the schema is a table, this is the inheritor schema OID
                        // If the schema is a report, this is the report's schema OID
                        schema_oid,

                        // The key columns of the schema
                        columns.iter()
                            .map(|col| format!("SELECT {}", col.json_expr_recursion))
                            .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                            .unwrap()
                    );

                    format!(
                        "
                        WITH {}, 
                        LABEL_CTE (PLAIN_LABEL, JSON_LABEL, OBJECT_LABEL {}) AS (
                            SELECT
                                {plain_expr_norecursion} AS PLAIN_LABEL,
                                {json_expr_norecursion} AS JSON_LABEL,
                                {object_expr_norecursion} AS OBJECT_LABEL
                                {oid_columns}
                            FROM WRAPPER w
                            WHERE {}

                            UNION

                            SELECT
                                {plain_expr_recursion} AS PLAIN_LABEL,
                                {json_expr_recursion} AS JSON_LABEL,
                                {object_expr_recursion} AS OBJECT_LABEL
                                {oid_columns}
                            FROM WRAPPER w
                            {}
                        ) 
                        
                        SELECT * FROM LABEL_CTE 
                        UNION ALL 
                        SELECT 
                            {plain_expr_norecursion} AS PLAIN_LABEL,
                            {json_expr_norecursion} AS JSON_LABEL,
                            {object_expr_norecursion} AS OBJECT_LABEL,
                            {oid_columns}
                        FROM WRAPPER w
                        WHERE {}
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
                            {plain_expr_norecursion} AS PLAIN_LABEL, 
                            {json_expr_norecursion} AS JSON_LABEL, 
                            {object_expr_norecursion} AS OBJECT_LABEL 
                            {oid_columns} 
                        FROM WRAPPER w
                        ",

                        // All of the CTEs, including the wrapper
                        cte_list.join(", ")
                    )          
                }
            }
        })
    }
}

struct SelectDatasource {
    /// The datasource being selected from.
    datasource: Datasource,

    /// The alias of the CTE being pulled from.
    /// Recursive if not "w".
    alias: String 
}

impl SelectDatasource {
    /// Constructs a new non-recursive datasource.
    fn new_norecursion(datasource: Datasource) -> Self {
        Self {
            datasource,
            alias: String::from("w")
        }
    }

    /// Constructs a new recursive datasource.
    fn new_recursion(datasource: Datasource, alias: String) -> Self {
        Self {
            datasource,
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
                columns: Vec::new() 
            }
        };

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

        for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING, IS_REQUIRED FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?, row.get::<_, bool>("IS_REQUIRED")?)))? {
            let (column_oid, ordering, is_required) = row_result?;
            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
            match &root_datasource {
                Some(root_datasource) => {
                    let param = select_constructor.add_parameter(trans, root_datasource.clone(), column, SelectParameterContext::Scalar)?;
                    if let SelectConstructorType::SelectLabelConstructor { columns, .. } = &mut select_constructor.constructor_type {
                        columns.push(SelectLabelColumn { 
                            plain_expr_norecursion: param.label_expr_norecursion, 
                            plain_expr_recursion: param.label_expr_recursion, 
                            json_expr_norecursion: param.scalar_type.construct_json_label_expr(&param.value_expr_norecursion), 
                            json_expr_recursion: param.scalar_type.construct_json_label_expr(&param.value_expr_recursion), 
                            ordering, 
                            is_required
                        });
                    }
                }
                None => {
                    todo!("idk what to do when it's on a report")
                }
            }
        }

        Ok(select_constructor)
    }

    /// Builds the SQL syntax for this SELECT statement.
    fn build(&self) -> Result<String, Error> {
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
                    String::from("NULL AS COLUMN1 WHERE FALSE")
                }
            ));

            (cte_list, oid_list)
        };

        self.constructor_type.build(cte_list, oid_list)
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
    fn add_parameter(&mut self, trans: &Transaction, datasource: SelectDatasource, column: column::FullMetadata, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
        match &mut context {
            SelectParameterContext::Scalar => {
                self.add_datasource(datasource.datasource.clone(), false);
            }
            SelectParameterContext::Collection { min_depth, .. } => {
                let datasource: Datasource = datasource.datasource;
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
        
        match column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_primitive_column(column.oid, prim.clone());
                    let scalar_type = SelectParameterType::from(prim);
                    
                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let label_expr: String = scalar_type.construct_plain_label_expr(&cte_column.value_ord);
                    let cell_expr: String = format!(
                        "('{}:{}:' || CAST({} AS TEXT))",
                        column.schema.oid,
                        column.oid,
                        datasource.get_oid_expr()
                    );
                    
                    return Ok(SelectParameter::new_norecursion(label_expr, value_expr, cell_expr, scalar_type, context));
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_object_column(column.oid, table_oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let cell_expr: String = format!(
                        "('{}:{}:' || CAST(w.{}_OID AS TEXT))",
                        column.schema.oid,
                        column.oid,
                        datasource.get_alias()
                    );
                    match &mut self.constructor_type {
                        SelectConstructorType::SelectMainConstructor { .. } => {
                            let label_expr: String = format!("(SELECT l.OBJECT_LABEL FROM SCHEMA{table_oid}_LABEL l WHERE l.OID = {value_expr})");
                            return Ok(SelectParameter::new_norecursion(label_expr, value_expr, cell_expr, SelectParameterType::new(), context));
                        }
                        SelectConstructorType::SelectLabelConstructor { recursions, .. } => {
                            // First, ensure that the Object label does not cause recursion
                            for looped_datasource in datasource.linearize() {
                                let looped_datasource_schema_oid: i64 = looped_datasource.get_schema_oid()?;
                                if looped_datasource_schema_oid == table_oid {
                                    recursions.push((value_expr.clone(), format!("{}_COLUMN{}.{}_OID", datasource.get_alias(), column.oid, looped_datasource.get_alias())));

                                    // Construct recursive Object label for table with OID {table_oid}
                                    let mut object_columns: Vec<SelectLabelColumn> = Vec::new();
                                    for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING, IS_REQUIRED FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?, row.get::<_, bool>("IS_REQUIRED")?)))? {
                                        let (column_oid, ordering, is_required) = row_result?;
                                        let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                                        
                                        let param = self.add_parameter(trans, datasource.clone(), column, SelectParameterContext::Scalar)?;
                                        object_columns.push(SelectLabelColumn { 
                                            plain_expr_norecursion: param.label_expr_norecursion, 
                                            plain_expr_recursion: param.label_expr_recursion, 
                                            json_expr_norecursion: param.scalar_type.construct_json_label_expr(&param.value_expr_norecursion), 
                                            json_expr_recursion: param.scalar_type.construct_json_label_expr(&param.value_expr_recursion), 
                                            ordering, 
                                            is_required
                                        });
                                    }
                                    return SelectParameter {
                                        label_expr_norecursion: format!(
                                            "IF({value_expr} IS NOT NULL, '{{}}', NULL)"
                                        ),
                                        label_expr_recursion: format!(
                                            "'{{ \"' || (SELECT REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA s WHERE s.OID = {}) || '\": ' || COALESCE(GROUP_CONCAT(({}), ', '), '{{ }}') || ' }}'",

                                            // The OID of the schema
                                            // If the schema is a table, this is the inheritor schema OID
                                            // If the schema is a report, this is the report's schema OID
                                            schema_oid,

                                            // The key columns of the schema
                                            object_columns.iter()
                                                .map(|col| format!("SELECT {}", col.json_expr_recursion))
                                                .reduce(|acc, e| format!("{acc} UNION ALL {e}"))
                                                .unwrap()
                                        )
                                        value_expr_norecursion: value_expr.clone(),
                                        value_expr_recursion: value_expr,

                                    };
                                }
                            }
                            // Construct non-recursive Object label for table with OID {table_oid}
                            let mut object_columns: Vec<SelectLabelColumn> = Vec::new();
                            for row_result in trans.prepare("SELECT COLUMN_OID, ORDERING, IS_REQUIRED FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY ORDER BY IS_SUBREPORT ASC")?.query_map(params![table_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, i64>("ORDERING")?, row.get::<_, bool>("IS_REQUIRED")?)))? {
                                let (column_oid, ordering, is_required) = row_result?;
                                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                                
                                let param = self.add_parameter(trans, datasource.clone(), column, SelectParameterContext::Scalar)?;
                                object_columns.push(SelectLabelColumn { 
                                    plain_expr_norecursion: param.label_expr_norecursion, 
                                    plain_expr_recursion: param.label_expr_recursion, 
                                    json_expr_norecursion: param.scalar_type.construct_json_label_expr(&param.value_expr_norecursion), 
                                    json_expr_recursion: param.scalar_type.construct_json_label_expr(&param.value_expr_recursion), 
                                    ordering, 
                                    is_required
                                });
                            }                            
                        }
                    }
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_select_column(column.oid, table_oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let cell_expr: String = format!(
                        "('{}:{}:' || CAST(w.{}_OID AS TEXT))",
                        column.schema.oid,
                        column.oid,
                        datasource.get_alias()
                    );
                    match self.constructor_type {
                        SelectConstructorType::SelectMainConstructor { .. } => {
                            let label_expr: String = format!("(SELECT COALESCE(l.PLAIN_LABEL, l.JSON_LABEL) FROM SCHEMA{table_oid}_LABEL l WHERE l.OID = {value_expr})");
                            return Ok(SelectParameter::new_norecursion(label_expr, value_expr, cell_expr, SelectParameterType::new(), context));
                        }
                        SelectConstructorType::SelectLabelConstructor { .. } => {
                            todo!("Construct Plain/JSON label for the table with OID {table_oid}");
                        }
                    }
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                // Add the datasource for the OIDs of the Multiselect column
                let multiselect_datasource = datasource.append_path(format!("_COLUMN{}", column.oid))?;
                let multiselect_datasource_oid: String = format!("{}_OID", multiselect_datasource.get_alias());
                self.add_datasource(multiselect_datasource, true);

                if let Some(cte) = self.cte_datasource.get_mut(&datasource.get_alias()) {
                    let cte_column = cte.add_multiselect_column(column.oid, table_oid);

                    let value_expr: String = format!("w.{}", cte_column.value_ord);
                    let cell_expr: String = format!(
                        "('{}:{}:' || CAST(w.{}_OID AS TEXT))",
                        column.schema.oid,
                        column.oid,
                        datasource.get_alias()
                    );
                    match self.constructor_type {
                        SelectConstructorType::SelectMainConstructor { .. } => {
                            let label_expr: String = format!("NULLIF('[ ' || GROUP_CONCAT(COALESCE((SELECT l.JSON_LABEL FROM SCHEMA{table_oid}_LABEL l WHERE l.OID = {multiselect_datasource_oid}), '{{}}'), ', ') || ' ]', '[  ]')");
                            return Ok(SelectParameter::new_norecursion(label_expr, value_expr, cell_expr, SelectParameterType::new(), context));
                        }
                        SelectConstructorType::SelectLabelConstructor { .. } => {
                            todo!("Construct JSON label for the table with OID {table_oid}");
                        }
                    }
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                return self.construct_formula(
                    trans,
                    {
                        if let Some(Datasource::Table { oid, .. }) = Datasource::get_default_datasource_transact(trans, datasource.get_schema_oid()?)? {
                            (oid, datasource)
                        } else {
                            return Err(Error::AdhocError("No default datasource for table."));
                        }
                    },
                    parsed_formula,
                    context
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                match self.constructor_type {
                    SelectConstructorType::SelectMainConstructor { .. } => {
                        // Examine the schema of SCHEMA{report_oid}_LABEL_VIEW to see what filters are applicable to the report
                        let mut filtered_columns: Vec<(String, String)> = Vec::new();
                        let oid_regex = Regex::new(r"ROOT\d+(?:_MASTER\d+|_INHERITOR\d+|_COLUMN\d+)*_OID").unwrap();
                        let pragma_sql: String = format!("PRAGMA table_info(SCHEMA{report_oid}_LABEL_VIEW)");
                        for row_result in trans.prepare(&pragma_sql)?.query_map([], |row| row.get("NAME"))? {
                            let oid_column_name: String = row_result?;
                            if oid_regex.is_match(&oid_column_name) {
                                // Test if the OID is being selected in this view
                                // TODO
                            }
                        }

                        // Construct the column
                        return Ok(SelectParameter::new_norecursion(
                            format!(
                                "NULLIF('[ ' || GROUP_CONCAT((SELECT l.JSON_LABEL FROM SCHEMA{report_oid}_LABEL_VIEW l {}), ', ') || ' ]', '[  ]')",
                                if filtered_columns.len() > 0 {
                                    format!(
                                        "WHERE {}",
                                        filtered_columns.iter().map(|(filtered_oid_ord, filtered_oid_value)| format!("l.{filtered_oid_ord} IS {filtered_oid_value}"))
                                            .reduce(|acc, e| format!("{acc} AND {e}"))
                                            .unwrap()
                                    )
                                } else {
                                    String::from("")
                                }
                            ),
                            filtered_columns.iter()
                                .map(|(filtered_oid_ord, filtered_oid_value)| format!("'{filtered_oid_ord}=' || CAST({filtered_oid_value} AS TEXT)"))
                                .reduce(|acc, e| format!("{acc} || '&' || {e}"))
                                .unwrap_or(String::from("''")),
                            format!(
                                "('{}:{}:' || CAST(w.{}_OID AS TEXT))",
                                column.schema.oid,
                                column.oid,
                                datasource.get_alias()
                            ),
                            SelectParameterType::new(),
                            context
                        ));
                    }
                    SelectConstructorType::SelectLabelConstructor { .. } => {
                        todo!("Construct labels for the report with OID {report_oid}");
                    }
                }
            }
        }
        return Err(Error::AdhocError("Unable to add parameter."));
    }

    /// Constructs the SQL expression corresponding to a Formula object.
    fn construct_formula(&mut self, trans: &Transaction, root_datasource: (i64, Datasource), formula: Box<Formula>, mut context: SelectParameterContext) -> Result<SelectParameter, Error> {
        Ok(match *formula {
            Formula::Null => {
                SelectParameter { 
                    label_expr_norecursion: String::from("NULL"),
                    label_expr_recursion: String::from("NULL"),
                    value_expr_norecursion: String::from("NULL"),
                    value_expr_recursion: String::from("NULL"),
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type: SelectParameterType::new(),
                    context
                }
            }
            Formula::LiteralBool(value) => {
                if value {
                    let label_expr: String = format!("'true'");
                    let value_expr: String = format!("TRUE");
                    SelectParameter { 
                        label_expr_norecursion: label_expr.clone(),
                        label_expr_recursion: label_expr, 
                        value_expr_norecursion: value_expr.clone(),
                        value_expr_recursion: value_expr,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Boolean),
                        context
                    }
                } else {
                    let label_expr: String = format!("'false'");
                    let value_expr: String = format!("FALSE");
                    SelectParameter { 
                        label_expr_norecursion: label_expr.clone(),
                        label_expr_recursion: label_expr, 
                        value_expr_norecursion: value_expr.clone(),
                        value_expr_recursion: value_expr,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
                        scalar_type: SelectParameterType::from(column_type::Primitive::Boolean),
                        context
                    }
                }
            }
            Formula::LiteralFloat(value) => {
                let label_expr: String = format!("'{value}'");
                let value_expr: String = format!("{value}");
                SelectParameter { 
                    label_expr_norecursion: label_expr.clone(),
                    label_expr_recursion: label_expr, 
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Number),
                    context
                }
            }
            Formula::LiteralInt(value) => {
                let label_expr: String = format!("'{value}'");
                let value_expr: String = format!("{value}");
                SelectParameter { 
                    label_expr_norecursion: label_expr.clone(),
                    label_expr_recursion: label_expr, 
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type: SelectParameterType::from(column_type::Primitive::Integer),
                    context
                }
            }
            Formula::LiteralString(value) => {
                let sql_value: String = format!("'{}'", value.replace("'", "''"));
                SelectParameter {
                    label_expr_norecursion: sql_value.clone(),
                    label_expr_recursion: sql_value.clone(),
                    value_expr_norecursion: sql_value.clone(),
                    value_expr_recursion: sql_value,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type: SelectParameterType::from(column_type::Primitive::PlainText),
                    context
                }
            }
            
            Formula::Abs(inner) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let inner_name: String = inner.to_string();
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("ABS({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("ABS({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("CEILING({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("CEILING({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("FLOOR({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("FLOOR({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("LENGTH({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("LENGTH({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("LOWER({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("LOWER({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                    let value_expr_norecursion: String = format!("(NOT {})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("(NOT {})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("ROUND({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("ROUND({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = SelectParameterType::from(column_type::Primitive::Integer);
                    let value_expr_norecursion: String = format!("SIGN({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("SIGN({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let inner_param = self.construct_formula(trans, root_datasource, inner, context)?;
                if inner_expected_type.encompasses(&inner_param.scalar_type) {
                    let scalar_type = inner_param.scalar_type;
                    let value_expr_norecursion: String = format!("UPPER({})", inner_param.value_expr_norecursion);
                    let value_expr_recursion: String = format!("UPPER({})", inner_param.value_expr_recursion);
                    SelectParameter {
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                self.construct_formula(trans, root_datasource, inner, context)?
            }
            
            Formula::Add(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} + {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} + {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} AND {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} AND {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                        let value_expr_norecursion: String = format!("({} || {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} || {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Number);
                        let value_expr_norecursion: String = format!("({} / {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} / {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;

                let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let value_expr_norecursion: String = format!("({} IS {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                let value_expr_recursion: String = format!("({} IS {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                SelectParameter {
                    label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type,
                    context: rhs_param.context
                }
            }
            Formula::Exponent(lhs, rhs) => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let lhs_name: String = lhs.to_string();
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("POW({}, {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("POW({}, {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} < {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} < {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} <= {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} <= {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} % {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} % {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} * {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} * {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} OR {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} OR {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), lhs, context)?;
                if inner_expected_type.encompasses(&lhs_param.scalar_type) {
                    let rhs_name: String = rhs.to_string();
                    let rhs_param = self.construct_formula(trans, root_datasource, rhs, lhs_param.context)?;
                    if inner_expected_type.encompasses(&rhs_param.scalar_type) {
                        let scalar_type = lhs_param.scalar_type.generalize(&rhs_param.scalar_type);
                        let value_expr_norecursion: String = format!("({} - {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} - {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                    let inner_param = self.construct_formula(trans, root_datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        label_expr_norecursion: String::from("NULL"),
                        label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr_norecursion != "NULL" {
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
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr_norecursion)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        cell_expr_recursion: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr_recursion != "NULL" {
                                // For each argument that is potentially associated with a cell, build a WHEN clause that checks if the value is the maximum of all parameters
                                match params.iter().enumerate().filter_map(|(param_rhs_idx, param_rhs)| {
                                    if param_lhs.value_expr_recursion != param_rhs.value_expr_recursion {
                                        Some(format!(
                                            "({} {} {})", 
                                            param_lhs.value_expr_recursion, 
                                            if param_lhs_idx < param_rhs_idx { ">=" } else { ">" }, 
                                            param_rhs.value_expr_recursion
                                        ))
                                    } else {
                                        None
                                    }
                                }).reduce(|acc, e| format!("{acc} AND {e}")) {
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr_recursion)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
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
                    let inner_param = self.construct_formula(trans, root_datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        label_expr_norecursion: String::from("NULL"),
                        label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"), 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr_norecursion != "NULL" {
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
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr_norecursion)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        cell_expr_recursion: match params.iter().enumerate().filter_map(|(param_lhs_idx, param_lhs)| {
                            // Iterate over each argument, checking if the cell_expr is not trivial
                            if param_lhs.cell_expr_recursion != "NULL" {
                                // For each argument that is potentially associated with a cell, build a WHEN clause that checks if the value is the maximum of all parameters
                                match params.iter().enumerate().filter_map(|(param_rhs_idx, param_rhs)| {
                                    if param_lhs.value_expr_recursion != param_rhs.value_expr_recursion {
                                        Some(format!(
                                            "({} {} {})", 
                                            param_lhs.value_expr_recursion, 
                                            if param_lhs_idx < param_rhs_idx { "<=" } else { "<" }, 
                                            param_rhs.value_expr_recursion
                                        ))
                                    } else {
                                        None
                                    }
                                }).reduce(|acc, e| format!("{acc} AND {e}")) {
                                    Some(conditions) => Some(format!("WHEN {conditions} THEN {}", param_lhs.cell_expr_recursion)),
                                    None => None
                                }
                            } else {
                                None
                            }
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
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
                    let inner_param = self.construct_formula(trans, root_datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        label_expr_norecursion: String::from("NULL"),
                        label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    SelectParameter {
                        label_expr_norecursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_norecursion,
                                param.label_expr_norecursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        label_expr_recursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_recursion,
                                param.label_expr_recursion
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
                        cell_expr_norecursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_norecursion,
                                param.cell_expr_norecursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
                        cell_expr_recursion: match params.iter().map(|param| {
                            format!(
                                "WHEN {} IS NOT NULL THEN {}",
                                param.value_expr_recursion,
                                param.cell_expr_recursion
                            )
                        }).reduce(|acc, e| format!("{acc} {e}")) {
                            Some(when_conditions) => format!("CASE {when_conditions} ELSE NULL END"),
                            None => String::from("NULL")
                        },
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
                    let inner_param = self.construct_formula(trans, root_datasource.clone(), Box::new(inner), context)?;
                    context = inner_param.context.clone();
                    scalar_type = scalar_type.generalize(&inner_param.scalar_type);
                    params.push(inner_param);
                }

                if params.len() == 0 {
                    SelectParameter { 
                        label_expr_norecursion: String::from("NULL"),
                        label_expr_recursion: String::from("NULL"),
                        value_expr_norecursion: String::from("NULL"),
                        value_expr_recursion: String::from("NULL"),
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
                        scalar_type, 
                        context 
                    }
                } else if params.len() == 1 {
                    params.pop().unwrap()
                } else {
                    SelectParameter {
                        label_expr_norecursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.label_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        label_expr_recursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.label_expr_recursion))
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
                        cell_expr_norecursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.cell_expr_norecursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        cell_expr_recursion: format!(
                            "({})",
                            params.iter()
                                .map(|param| format!("SELECT {}", param.cell_expr_recursion))
                                .reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                        ),
                        scalar_type,
                        context
                    }
                }
            }
            
            Formula::Average(collection) => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::Number);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                    label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type,
                    context
                }
            }
            Formula::Join { collection, delimiter } => {
                let collection_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let collection_name: String = collection.to_string();
                let collection_param = self.construct_formula(trans, root_datasource.clone(), collection, SelectParameterContext::Collection { 
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
                    let delimiter_param = self.construct_formula(trans, root_datasource, delimiter, context)?;
                    if delimiter_expected_type.encompasses(&delimiter_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                        let (value_expr_norecursion, value_expr_recursion) = collection_param.context.wrap(
                            format!("GROUP_CONCAT({}, {})", collection_param.value_expr_norecursion, delimiter_param.value_expr_norecursion),
                            format!("GROUP_CONCAT({}, {})", collection_param.value_expr_recursion, delimiter_param.value_expr_recursion)
                        );
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let value_expr: String = format!("RANDOM{}", self.random_values);
                let label_expr: String = scalar_type.construct_plain_label_expr(&value_expr);
                SelectParameter {
                    label_expr_norecursion: label_expr.clone(),
                    label_expr_recursion: label_expr,
                    value_expr_norecursion: value_expr.clone(),
                    value_expr_recursion: value_expr,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type,
                    context
                }
            }
            Formula::Param { datasource_alias, column_oid } => {
                context.disable_window_changes();
                let datasource: Datasource = Datasource::from_alias_transact(trans, datasource_alias)?.substitute_root(root_datasource.0, root_datasource.1);
                let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
                self.add_parameter(trans, datasource, column, context)?
            }
            
            Formula::Conditional { condition, formula_if_true, formula_if_false } => {
                context.disable_window_changes();
                let condition_expected_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let condition_name: String = condition.to_string();
                let condition_param = self.construct_formula(trans, root_datasource.clone(), condition, context)?;
                if condition_expected_type.encompasses(&condition_param.scalar_type) {
                    let if_true_param = self.construct_formula(trans, root_datasource.clone(), formula_if_true, condition_param.context)?;
                    let if_false_param = self.construct_formula(trans, root_datasource, formula_if_false, if_true_param.context)?;

                    let scalar_type = if_true_param.scalar_type.generalize(&if_false_param.scalar_type);
                    SelectParameter {
                        label_expr_norecursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.label_expr_norecursion, 
                            if_false_param.label_expr_norecursion
                        ),
                        label_expr_recursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_recursion, 
                            if_true_param.label_expr_recursion, 
                            if_false_param.label_expr_recursion
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
                        cell_expr_norecursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_norecursion, 
                            if_true_param.cell_expr_norecursion, 
                            if_false_param.cell_expr_norecursion
                        ),
                        cell_expr_recursion: format!(
                            "IF({}, {}, {})", 
                            condition_param.value_expr_recursion, 
                            if_true_param.cell_expr_recursion, 
                            if_false_param.cell_expr_recursion
                        ),
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
                let format_str_param = self.construct_formula(trans, root_datasource.clone(), format_str, context)?;
                if format_str_expected_type.encompasses(&format_str_param.scalar_type) {
                    context = format_str_param.context;

                    let mut params: Vec<SelectParameter> = Vec::new();
                    for inner in format_params {
                        let inner_param = self.construct_formula(trans, root_datasource.clone(), Box::new(inner), context)?;
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                        value_expr_norecursion,
                        value_expr_recursion,
                        cell_expr_norecursion: String::from("NULL"),
                        cell_expr_recursion: String::from("NULL"),
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
                let str_param = self.construct_formula(trans, root_datasource.clone(), str, context)?;
                if inner_expected_type.encompasses(&str_param.scalar_type) {
                    let pattern_name: String = pattern.to_string();
                    let pattern_param = self.construct_formula(trans, root_datasource, pattern, str_param.context)?;
                    if inner_expected_type.encompasses(&pattern_param.scalar_type) {
                        let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                        let value_expr_norecursion: String = format!("({} GLOB {})", str_param.value_expr_norecursion, pattern_param.value_expr_norecursion);
                        let value_expr_recursion: String = format!("({} GLOB {})", str_param.value_expr_recursion, pattern_param.value_expr_recursion);
                        SelectParameter {
                            label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                            label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                            value_expr_norecursion,
                            value_expr_recursion,
                            cell_expr_norecursion: String::from("NULL"),
                            cell_expr_recursion: String::from("NULL"),
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
                let collection_param = self.construct_formula(trans, root_datasource.clone(), collection, SelectParameterContext::Collection {
                    slice_norecursion: SelectParameterSlice::None,
                    slice_recursion: SelectParameterSlice::None,
                    filter_expr_norecursion: None,
                    filter_expr_recursion: None,
                    order_exprs_norecursion: Vec::new(),
                    order_exprs_recursion: Vec::new(),
                    min_depth: HashMap::new(),
                    window_changes_disabled: true
                })?;
                let value_param = self.construct_formula(trans, root_datasource, value, context)?;

                let scalar_type = SelectParameterType::from(column_type::Primitive::Boolean);
                let value_expr_norecursion: String = format!("({} IN {})", value_param.value_expr_norecursion, collection_param.value_expr_norecursion);
                let value_expr_recursion: String = format!("({} IN {})", value_param.value_expr_recursion, collection_param.value_expr_recursion);
                SelectParameter {
                    label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                    label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion),
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr_norecursion: String::from("NULL"),
                    cell_expr_recursion: String::from("NULL"),
                    scalar_type,
                    context: value_param.context
                }
            }
            Formula::Index { collection, index } => {
                let index_expected_type = SelectParameterType::from(column_type::Primitive::Integer);
                let index_name: String = index.to_string();
                let index_param = self.construct_formula(trans, root_datasource.clone(), index, context)?;
                if index_expected_type.encompasses(&index_param.scalar_type) {
                    let collection_param = self.construct_formula(trans, root_datasource, collection, SelectParameterContext::Collection { 
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
                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                        value_expr_norecursion,
                        value_expr_recursion, 
                        cell_expr_norecursion: String::from("NULL"), 
                        cell_expr_recursion: String::from("NULL"),
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
                let lhs_param = self.construct_formula(trans, root_datasource.clone(), value, context)?;
                let rhs_param = self.construct_formula(trans, root_datasource, null_if_match, lhs_param.context)?;

                let scalar_type = lhs_param.scalar_type;
                SelectParameter {
                    label_expr_norecursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion, lhs_param.label_expr_norecursion),
                    label_expr_recursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion, lhs_param.label_expr_recursion),
                    value_expr_norecursion: format!("NULLIF({}, {})", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion),
                    value_expr_recursion: format!("NULLIF({}, {})", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion),
                    cell_expr_norecursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_norecursion, rhs_param.value_expr_norecursion, lhs_param.cell_expr_norecursion),
                    cell_expr_recursion: format!("CASE WHEN ({} IS {}) THEN NULL ELSE {} END", lhs_param.value_expr_recursion, rhs_param.value_expr_recursion, lhs_param.cell_expr_recursion),
                    scalar_type,
                    context: rhs_param.context
                }
            }
            Formula::Replace { original, pattern, replacement } => {
                context.disable_window_changes();
                let inner_expected_type = SelectParameterType::from(column_type::Primitive::PlainText);
                let original_name: String = original.to_string();
                let original_param = self.construct_formula(trans, root_datasource.clone(), original, context)?;
                if inner_expected_type.encompasses(&original_param.scalar_type) {
                    let pattern_name: String = pattern.to_string();
                    let pattern_param = self.construct_formula(trans, root_datasource.clone(), pattern, original_param.context)?;
                    if inner_expected_type.encompasses(&pattern_param.scalar_type) {
                        let replacement_name: String = replacement.to_string();
                        let replacement_param = self.construct_formula(trans, root_datasource, replacement, pattern_param.context)?;
                        if inner_expected_type.encompasses(&replacement_param.scalar_type) {
                            let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                            let value_expr_norecursion: String = format!("REPLACE({}, {}, {})", original_param.value_expr_norecursion, pattern_param.value_expr_norecursion, replacement_param.value_expr_norecursion);
                            let value_expr_recursion: String = format!("REPLACE({}, {}, {})", original_param.value_expr_recursion, pattern_param.value_expr_recursion, replacement_param.value_expr_recursion);
                            SelectParameter { 
                                label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                                label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                value_expr_norecursion,
                                value_expr_recursion, 
                                cell_expr_norecursion: String::from("NULL"), 
                                cell_expr_recursion: String::from("NULL"),
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
                let str_param = self.construct_formula(trans, root_datasource.clone(), str, context)?;
                if inner_expected_type.encompasses(&str_param.scalar_type) {
                    let start_name: String = start.to_string();
                    let start_param = self.construct_formula(trans, root_datasource.clone(), start, str_param.context)?;
                    if inner_expected_type.encompasses(&start_param.scalar_type) {
                        match length {
                            None => {
                                let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                                let value_expr_norecursion: String = format!("SUBSTR({}, {})", str_param.value_expr_norecursion, start_param.value_expr_norecursion);
                                let value_expr_recursion: String = format!("SUBSTR({}, {})", str_param.value_expr_recursion, start_param.value_expr_recursion);
                                SelectParameter { 
                                    label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion), 
                                    label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                    value_expr_norecursion,
                                    value_expr_recursion, 
                                    cell_expr_norecursion: String::from("NULL"), 
                                    cell_expr_recursion: String::from("NULL"),
                                    scalar_type, 
                                    context: start_param.context 
                                }
                            }
                            Some(length) => {
                                let length_name: String = length.to_string();
                                let length_param = self.construct_formula(trans, root_datasource, length, start_param.context)?;
                                if inner_expected_type.encompasses(&length_param.scalar_type) {
                                    let scalar_type = SelectParameterType::from(column_type::Primitive::PlainText);
                                    let value_expr_norecursion: String = format!("SUBSTR({}, {}, {})", str_param.value_expr_norecursion, start_param.value_expr_norecursion, length_param.value_expr_norecursion);
                                    let value_expr_recursion: String = format!("SUBSTR({}, {}, {})", str_param.value_expr_recursion, start_param.value_expr_recursion, length_param.value_expr_recursion);
                                    SelectParameter { 
                                        label_expr_norecursion: scalar_type.construct_plain_label_expr(&value_expr_norecursion),
                                        label_expr_recursion: scalar_type.construct_plain_label_expr(&value_expr_recursion), 
                                        value_expr_norecursion,
                                        value_expr_recursion, 
                                        cell_expr_norecursion: String::from("NULL"), 
                                        cell_expr_recursion: String::from("NULL"), 
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
                let value_param = self.construct_formula(trans, root_datasource.clone(), value, context)?;
                context = value_param.context;

                let mut return_scalar_type = SelectParameterType::new();
                let (
                    value_norecursion_when_clauses, 
                    value_recursion_when_clauses,
                    label_norecursion_when_clauses,
                    label_recursion_when_clauses, 
                    cell_norecursion_when_clauses,
                    cell_recursion_when_clauses
                ) = {
                    let mut match_params: Vec<(SelectParameter, SelectParameter)> = Vec::new();
                    for (test_match, formula_if_match) in matches {
                        let test_match_param = self.construct_formula(trans, root_datasource.clone(), Box::new(test_match), context)?;
                        context = test_match_param.context.clone();

                        let if_match_param = self.construct_formula(trans, root_datasource.clone(), Box::new(formula_if_match), context)?;
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
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.label_expr_norecursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_recursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.label_expr_recursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_norecursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.cell_expr_norecursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from("")),
                        when_clauses_recursion.iter().map(|(when_clause, if_match_param)| format!("{when_clause} {}", if_match_param.cell_expr_recursion))
                            .reduce(|acc, e| format!("{acc} {e}"))
                            .unwrap_or(String::from(""))
                    )
                };
                
                let (
                    value_expr_norecursion, 
                    value_expr_recursion,
                    label_expr_norecursion, 
                    label_expr_recursion, 
                    cell_expr_norecursion,
                    cell_expr_recursion
                ) = match formula_if_no_match {
                    None => (
                        format!("CASE {value_norecursion_when_clauses} ELSE NULL END"),
                        format!("CASE {value_recursion_when_clauses} ELSE NULL END"),
                        format!("CASE {label_norecursion_when_clauses} ELSE NULL END"),
                        format!("CASE {label_recursion_when_clauses} ELSE NULL END"),
                        format!("CASE {cell_norecursion_when_clauses} ELSE NULL END"),
                        format!("CASE {cell_recursion_when_clauses} ELSE NULL END")
                    ),
                    Some(formula_if_false) => {
                        let if_no_match_param = self.construct_formula(trans, root_datasource, formula_if_false, context)?;
                        context = if_no_match_param.context.clone();
                        (
                            format!("CASE {value_norecursion_when_clauses} ELSE {} END", if_no_match_param.value_expr_norecursion),
                            format!("CASE {value_recursion_when_clauses} ELSE {} END", if_no_match_param.value_expr_recursion),
                            format!("CASE {label_norecursion_when_clauses} ELSE {} END", if_no_match_param.label_expr_norecursion),
                            format!("CASE {label_recursion_when_clauses} ELSE {} END", if_no_match_param.label_expr_recursion),
                            format!("CASE {cell_norecursion_when_clauses} ELSE {} END", if_no_match_param.cell_expr_norecursion),
                            format!("CASE {cell_recursion_when_clauses} ELSE {} END", if_no_match_param.cell_expr_recursion)
                        )
                    }
                };
                SelectParameter {
                    label_expr_norecursion,
                    label_expr_recursion,
                    value_expr_norecursion,
                    value_expr_recursion,
                    cell_expr_norecursion,
                    cell_expr_recursion,
                    scalar_type: return_scalar_type,
                    context
                }
            }
        })
    }
}




struct ViewsToCreate {
    /// The OID of the schema to create views for.
    schema_oid: i64,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool,

    /// True if the polymorphism view needs to be created. False otherwise.
    create_polymorphism_view: bool,
}

/// Drop the views associated with a schema.
fn drop_all_views(
    trans: &Transaction,
    schema_oid: i64,
    create_schema_oid_seq: &mut Vec<ViewsToCreate>,
) -> Result<(), Error> {
    if create_schema_oid_seq
        .iter()
        .any(|view_to_create| view_to_create.schema_oid == schema_oid)
    {
        // Prevent possible infinite recursions
        return Ok(());
    }
    create_schema_oid_seq.push(ViewsToCreate {
        schema_oid,
        create_label_view: true,
        create_polymorphism_view: true,
    });

    // Drop the views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let master_schema_oid: i64 = row_result?;
        drop_all_views(trans, master_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        let inheritor_schema_oid: i64 = row_result?;
        drop_all_views(trans, inheritor_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views that use the label
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
        drop_views_associated_with_label(
            trans,
            referencing_schema_oid,
            referenced_in_label,
            create_schema_oid_seq,
        )?;
    }

    // Drop the associated views
    let drop_sql: String = format!(
        "
        DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_POLYMORPHISM_VIEW;
    "
    );
    trans.execute_batch(&drop_sql)?;
    Ok(())
}

/// Drop the views that reference the label of another schema.
fn drop_views_associated_with_label(
    trans: &Transaction,
    schema_oid: i64,
    drop_label_view: bool,
    create_schema_oid_seq: &mut Vec<ViewsToCreate>,
) -> Result<(), Error> {
    if create_schema_oid_seq
        .iter()
        .any(|view_to_create| view_to_create.schema_oid == schema_oid)
    {
        // Prevent possible infinite recursions
        return Ok(());
    }
    create_schema_oid_seq.push(ViewsToCreate {
        schema_oid,
        create_label_view: drop_label_view.clone(),
        create_polymorphism_view: false,
    });

    // Drop the primary schema view
    let drop_main_view_sql: String = format!("DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW");
    trans.execute(&drop_main_view_sql, [])?;

    if drop_label_view {
        // Drop the views associated with any inheritor schema
        // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
        for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
            let inheritor_schema_oid: i64 = row_result?;
            drop_views_associated_with_label(trans, inheritor_schema_oid, true, create_schema_oid_seq)?;
        }

        // Drop all views that require that label view
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
            drop_views_associated_with_label(
                trans,
                referencing_schema_oid,
                referenced_in_label,
                create_schema_oid_seq,
            )?;
        }

        // Drop the label view
        let drop_label_view_sql: String =
            format!("DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW");
        trans.execute(&drop_label_view_sql, [])?;
    }
    Ok(())
}

/// Compiles a CTE to determine the lowest-level inheritor table that is associated with a particular row in the master table.
fn compile_polymorphism_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<(), Error> {
    let cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(()); // Prevent infinite recursion, just in case
    }

    // The components of the query will be combined with UNION
    let mut polymorphism_cte_components: Vec<String> = Vec::new();

    // Get polymorphism of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        compile_polymorphism_cte(trans, inheritor_table_oid, compiled_cte)?;

        // Get the polymorphism from the inheritor CTE
        polymorphism_cte_components.push(format!(
            "
            SELECT
                i.MASTER{table_oid}_OID AS OID,
                p.TABLE_OID,
                p.ROW_OID
            FROM TABLE{inheritor_table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = p.OID
            "
        ));
    }

    // Compile the final CTE
    compiled_cte.insert(
        cte_name,
        if polymorphism_cte_components.len() > 0 {
            format!(
                "
                SELECT
                    t.OID,
                    COALESCE(u.TABLE_OID, {table_oid}) AS TABLE_OID,
                    COALESCE(u.ROW_OID, t.OID) AS ROW_OID
                FROM TABLE{table_oid} t
                LEFT JOIN ({}) u ON u.OID = t.OID
                WHERE NOT t.TRASH
                ",
                polymorphism_cte_components
                    .into_iter()
                    .reduce(|acc, e| format!("{acc} UNION {e}"))
                    .unwrap()
            )
        } else {
            format!(
                "
                SELECT
                    t.OID,
                    {table_oid} AS TABLE_OID,
                    t.OID AS ROW_OID
                FROM TABLE{table_oid} t
                WHERE NOT t.TRASH
                "
            )
        },
    );
    Ok(())
}

/// Create a view describing the lowest-level table that has a row inheriting from a particular row in the table.
fn create_table_polymorphism_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    println!("Creating TABLE{table_oid}_POLYMORPHISM_VIEW...");

    let final_cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    let view_name: String = format!("TABLE{table_oid}_POLYMORPHISM_VIEW");

    // Compile all necessary CTEs
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    compile_polymorphism_cte(trans, table_oid.clone(), &mut compiled_cte)?;

    // Compile and create the final view
    if let Some(final_cte) = compiled_cte.remove(&final_cte_name) {
        let create_sql: String = format!(
            "
            CREATE VIEW IF NOT EXISTS {view_name} AS 
            {}
            {final_cte}
            ",
            if compiled_cte.len() > 0 {
                format!(
                    "WITH {}",
                    compiled_cte
                        .into_iter()
                        .map(|(cte_name, cte_sql)| format!("{cte_name} AS ({cte_sql})"))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            }
        );
        println!("{create_sql}");
        trans.execute(&create_sql, [])?;
    }
    Ok(())
}

/// Compiles a CTE to get the primary key columns for a particular row in a table.
fn compile_keycolumn_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<bool, Error> {
    // Prevent duplication
    let cte_name: String = format!("TABLE{table_oid}_KEYCOLUMNS_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] != "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // The components of the query will be combined with UNION
    let mut column_cte_components: Vec<String> = Vec::new();

    // Get primary keys of master tables
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        // Ensure that master CTE is compiled
        let master_table_oid: i64 = row_result?;
        if compile_keycolumn_cte(trans, master_table_oid, compiled_cte)? {
            // Get the columns from the master CTE
            column_cte_components.push(format!(
                "
                SELECT
                    t.OID,
                    m.PLAIN_LABEL,
                    m.JSON_LABEL
                FROM TABLE{master_table_oid}_KEYCOLUMNS_CTE m
                INNER JOIN TABLE{table_oid} t ON t.MASTER{master_table_oid}_OID = m.OID
                "
            ));
        } // Otherwise, ignore the primary keys of the master table
    }

    // Get each primary key column from this table
    for row_result in trans.prepare("SELECT OID FROM METADATA_COLUMN WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY AND NOT TRASH")?.query_map(params![table_oid], |row| row.get::<_, i64>("OID"))? {
        let column_oid: i64 = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
        let sanitized_column_name: String = column_metadata.name.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "''");

        // Determine expression for this specific column
        match column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                match prim {
                    column_type::Primitive::Integer
                    | column_type::Primitive::Number 
                    | column_type::Primitive::JsonText => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE(CAST(t.COLUMN{column_oid} AS TEXT), 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::PlainText => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(t.COLUMN{column_oid}, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Boolean => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                IF(t.COLUMN{column_oid}, 'True', 'False') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || CASE WHEN t.COLUMN{column_oid} IS NULL THEN 'null' WHEN t.COLUMN{column_oid} THEN 'true' ELSE 'false' END AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Date => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                DATE(t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || DATE(t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Datetime => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::File 
                    | column_type::Primitive::Image => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                f.LABEL AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            LEFT JOIN METADATA_FILE_VIEW f ON f.OID = t.COLUMN{column_oid}
                            "
                        ));
                    }
                }
            }
            column_type::ColumnType::Object { table_oid: object_table_oid, .. } => {
                if compile_label_cte(trans, object_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            o.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{' || s.JSON_LABEL || '}}', 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{object_table_oid}_LABEL_CTE o ON o.OID = t.COLUMN{column_oid}
                        LEFT JOIN METADATA_SCHEMA s ON s.OID = o.SCHEMA_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Select { table_oid: select_table_oid, .. } => {
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            s.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE(s.JSON_LABEL, 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = t.COLUMN{column_oid}
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Multiselect { table_oid: select_table_oid, .. } => {
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            m.TABLE{table_oid}_OID AS OID,
                            '[' || GROUP_CONCAT('\"' || REPLACE(REPLACE(s.PLAIN_LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', ', ') || ']' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [' || COALESCE(GROUP_CONCAT(s.JSON_LABEL, ', '), '') || ']' AS JSON_LABEL
                        FROM MULTISELECT{column_oid} m
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = m.TABLE{select_table_oid}_OID
                        GROUP BY m.TABLE{table_oid}_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '[...]' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [...]' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            _ => {
                // Skip any other type of column
            }
        }
    }

    // Compile the final CTE
    match column_cte_components
        .into_iter()
        .reduce(|acc, e| format!("{acc} UNION {e}"))
    {
        Some(compiled_column_cte) => {
            compiled_cte.insert(cte_name, compiled_column_cte);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Compiles a CTE to get the label for a particular row in a table.
fn compile_label_cte(
    trans: &Transaction,
    table_oid: i64,
    compiled_cte: &mut HashMap<String, String>,
) -> Result<bool, Error> {
    let cte_name: String = format!("TABLE{table_oid}_LABEL_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] == "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // Ensure that the polymorphism CTE has been compiled
    compile_polymorphism_cte(trans, table_oid, compiled_cte)?;

    // Ensure that the keycolumns CTE has been compiled
    if !compile_keycolumn_cte(trans, table_oid, compiled_cte)? {
        // Again, something is completely fucked, recursion-wise
        return Ok(false);
    }

    let mut label_cte_components: Vec<String> = Vec::new();

    // Get labels of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        if compile_label_cte(trans, inheritor_table_oid, compiled_cte)? {
            label_cte_components.push(format!(
                "
                SELECT
                    i.MASTER{table_oid}_OID AS OID,
                    lbl.TABLE_OID,
                    lbl.ROW_OID,
                    lbl.PLAIN_LABEL,
                    lbl.JSON_LABEL
                FROM TABLE{inheritor_table_oid}_LABEL_CTE lbl
                INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = lbl.OID
                "
            ));
        } // Otherwise, ignore the inheritor labels
    }

    if label_cte_components.len() > 0 {
        compiled_cte.insert(
            cte_name,
            format!(
                "
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                COALESCE(u.PLAIN_LABEL,
                    (
                        SELECT
                            CASE
                                WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                                WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
                                ELSE NULL
                            END
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS PLAIN_LABEL,
                COALESCE(u.JSON_LABEL, 
                    (
                        SELECT 
                            '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' 
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            LEFT JOIN ({}) u ON u.TABLE_OID = p.TABLE_OID AND u.ROW_OID = p.ROW_OID
            ",
                label_cte_components
                    .into_iter()
                    .reduce(|acc, e| format!("{acc} UNION {e}"))
                    .unwrap()
            ),
        );
    } else {
        compiled_cte.insert(
            cte_name,
            format!(
                "
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                CASE
                    WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                    WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
                    ELSE NULL
                END AS PLAIN_LABEL,
                '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{table_oid}_KEYCOLUMNS_CTE k ON k.OID = p.OID
            GROUP BY
                p.OID,
                p.TABLE_OID,
                p.ROW_OID
            "
            ),
        );
    }
    Ok(true)
}

/// Create a view for the label of each row in the table.
fn create_table_label_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    println!("Creating TABLE{table_oid}_LABEL_VIEW...");

    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    let create_sql: String = if compile_label_cte(trans, table_oid, &mut compiled_cte)? {
        format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS
            WITH {} 
            SELECT
                lbl.OID,
                lbl.TABLE_OID,
                lbl.ROW_OID,
                COALESCE(lbl.PLAIN_LABEL, lbl.JSON_LABEL) AS SELECT_LABEL,
                lbl.JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": ' || lbl.JSON_LABEL || '}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_LABEL_CTE lbl
            INNER JOIN METADATA_SCHEMA s ON s.OID = lbl.TABLE_OID
            ",
            compiled_cte.into_iter().map(|(cte_name, cte_definition)| format!("{cte_name} AS ({cte_definition})")).reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(format!("
                TABLE{table_oid}_LABEL_CTE AS (
                    SELECT
                        OID,
                        TABLE_OID,
                        ROW_OID,
                        '...' AS SELECT_LABEL,
                        '{{ ... }}' AS JSON_LABEL
                    FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
                )
            "))
        )
    } else {
        // Create a label view with dummy labels
        format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS 
            SELECT
                p.OID,
                p.TABLE_OID,
                p.ROW_OID,
                '...' AS SELECT_LABEL,
                '{{ ... }}' AS JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{ ... }}}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
            INNER JOIN METADATA_SCHEMA s ON s.OID = p.TABLE_OID
            "
        )
    };
    println!("{create_sql}");
    trans.execute(&create_sql, [])?;
    Ok(())
}

enum SchemaViewColumn {
    TableData {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String,
    },
    Formula {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String,
        param_expr: String,
        param_ord: String,
    },
    Subreport {
        label_expr: String,
        label_ord: String
    }
}

impl SchemaViewColumn {
    /// Compiles the column.
    pub fn compile(&self) -> Result<String, Error> {
        Ok(match self {
            Self::TableData {
                label_expr,
                label_ord,
                value_expr,
                value_ord,
            } => format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}"),
            Self::Formula {
                label_expr,
                label_ord,
                value_expr,
                value_ord,
                param_expr,
                param_ord,
            } => {
                format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}, {param_expr} AS {param_ord}")
            }
            Self::Subreport {
                label_expr,
                label_ord 
            } => {
                format!("{label_expr} AS {label_ord}")
            }
        })
    }
}

#[derive(PartialEq, Eq, Clone)]
struct PrimitiveScalarType(u32);
bitflags! {
    impl PrimitiveScalarType: u32 {
        const Null          = 0b00000000;
        const AnyPrimitive  = 0b11111111;
        const Boolean       = 0b00000001;
        const Integer       = 0b00000010;
        const Number        = 0b00000110;
        const Date          = 0b00001000;
        const Datetime      = 0b00011000;
        const Text          = 0b00100000;
        const JSON          = 0b01000000;

        /// File data is represented as an OID in the METADATA_FILE table.
        const File          = 0b10000000;
    }
}

impl PrimitiveScalarType {
    /// Converts from a scalar type to a string.
    fn to_string(&self) -> String {
        let mut flags: Vec<Self> = self.iter().collect();
        // Reduce flags to minimal set
        let mut k: usize = 0;
        while k < flags.len() {
            // Iterate over each other flag, testing if this flag is contained in the other
            let mut j: usize = 0;
            while j < flags.len() {
                if j != k && flags[j].contains(flags[k].clone()) {
                    flags.remove(k.clone());
                    k -= 1; // Decrement to negate the increment
                    break;
                }
                // Increment the index being compared to
                j += 1;
            }

            // Increment index
            k += 1;
        }
        // Concatenate different types together
        flags
            .into_iter()
            .map(|flag| match flag {
                Self::Null => String::from("null"),
                Self::AnyPrimitive => String::from("primitive"),
                Self::Boolean => String::from("boolean"),
                Self::Integer => String::from("integer"),
                Self::Number => String::from("number"),
                Self::Date => String::from("date"),
                Self::Datetime => String::from("timestamp"),
                Self::Text => String::from("text"),
                Self::JSON => String::from("JSON"),
                Self::File => String::from("file"),
                _ => String::from("unknown"), // This case shouldn't ever happen; if it does, something has gone wrong
            })
            .reduce(|acc, e| format!("{acc} | {e}"))
            .unwrap_or(String::from("null"))
    }
}

#[derive(PartialEq, Eq, Clone)]
enum ExpressionReturnType {
    // Any possible value or reference.
    Any,

    // A more specific type.
    Selected {
        // The type can take on a primitive value.
        primitive: PrimitiveScalarType,

        // The type can take on a reference to a record in one of the indicated tables.
        record_in_table_oid: HashSet<i64>,
    },
}

impl ExpressionReturnType {
    /// Construct a new type representing a primitive value.
    pub fn new_primitive(primitive: PrimitiveScalarType) -> Self {
        Self::Selected {
            primitive,
            record_in_table_oid: HashSet::new(),
        }
    }

    /// Construct a new type representing a reference to a record in the table with the provided OID.
    pub fn new_record(table_oid: i64) -> Self {
        let mut record_in_table_oid: HashSet<i64> = HashSet::new();
        record_in_table_oid.insert(table_oid);
        Self::Selected {
            primitive: PrimitiveScalarType::Null,
            record_in_table_oid,
        }
    }

    /// Returns true if a parameter of this type can accept an argument of the given type.
    /// In other words, returns true if this type is equivalent to or a supertype of the given type.
    pub fn accepts_arg(&self, other: &ExpressionReturnType) -> bool {
        match self {
            Self::Any => true,
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => false,
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => {
                    self_primitive.contains(other_primitive.clone())
                        && self_table_oid.is_superset(other_table_oid)
                }
            },
        }
    }

    /// Returns a type that encompasses both this type and the given type.
    pub fn generalize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => Self::Any,
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => Self::Any,
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => Self::Selected {
                    primitive: self_primitive.clone() | other_primitive.clone(),
                    record_in_table_oid: self_table_oid
                        .union(other_table_oid)
                        .map(|ir| ir.clone())
                        .collect(),
                },
            },
        }
    }

    /// Returns a type that is encompassed by both this type and the given type.
    pub fn specialize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => other.clone(),
            Self::Selected {
                primitive: self_primitive,
                record_in_table_oid: self_table_oid,
            } => match other {
                Self::Any => Self::Selected {
                    primitive: self_primitive.clone(),
                    record_in_table_oid: self_table_oid.clone(),
                },
                Self::Selected {
                    primitive: other_primitive,
                    record_in_table_oid: other_table_oid,
                } => Self::Selected {
                    primitive: self_primitive.clone() & other_primitive.clone(),
                    record_in_table_oid: self_table_oid
                        .intersection(other_table_oid)
                        .map(|ir| ir.clone())
                        .collect(),
                },
            },
        }
    }

    /// Converts the expression return type to a string.
    pub fn to_string(&self, conn: &Connection) -> String {
        match self {
            Self::Any => format!("any"),
            Self::Selected {
                primitive,
                record_in_table_oid,
            } => {
                let mut record_types: Vec<String> = Vec::new();
                if record_in_table_oid.len() > 0 {
                    for table_oid in record_in_table_oid {
                        if let Ok(schema_metadata) =
                            schema::FullMetadata::get(conn, table_oid.clone())
                        {
                            record_types.push(format!("record [{}]", schema_metadata.name));
                        } else {
                            record_types.push(String::from("record [-ERROR-]"));
                        }
                    }

                    if primitive == &PrimitiveScalarType::Null {
                        record_types
                            .into_iter()
                            .reduce(|acc, e| format!("{acc} | {e}"))
                            .unwrap_or(String::from("null"))
                    } else {
                        record_types
                            .into_iter()
                            .fold(primitive.to_string(), |acc, e| format!("{acc} | {e}"))
                    }
                } else {
                    primitive.to_string()
                }
            }
        }
    }
}

#[derive(Clone)]
struct ParamCTEColumnCell {
    /// The OID of the table. May be different from the schema OID indicated by the column, if the cell has a reversed relationship.
    table_oid: i64,

    /// The OID of the column.
    column_oid: i64,

    /// The ordinal of the row OID.
    row_ord: String,
}

#[derive(Clone)]
struct ParamCTEColumn {
    /// The expression for the label.
    /// Always returns a string that is 1-to-1 with its datasource.
    label_expr: String,

    /// The ordinal of the label.
    label_ord: String,

    /// The expression for the value.
    /// Always returns a value that is 1-to-1 with its datasource.
    value_expr: String,

    /// The ordinal of the value.
    value_ord: String,

    /// The expression for the parameter as an argument to a formula.
    /// May return a value that is 1-to-* with its datasource, in the case of reversed Object columns, reversed Select columns, or normal/reversed Multiselect columns.
    arg_expr: String,

    /// The type of the parameter as an argument to a formula.
    arg_type: ExpressionReturnType,

    /// The identifier for the column and row.
    cell: Option<ParamCTEColumnCell>,
}

struct ParamCTE {
    datasource: Datasource,
    child_datasources: HashSet<Datasource>,
    columns: HashMap<String, ParamCTEColumn>,
    is_grouped: bool
}

impl ParamCTE {
    /// Compiles the CTE.
    pub fn compile(self) -> Result<String, Error> {
        let datasource_alias: String = self.datasource.get_alias();
        let datasource_schema_oid: i64 = self.datasource.get_schema_oid()?;

        // If datasource is an Object or Select column with a reversed relationship, make sure that column is included in the CTE
        // If datasource is an inheritor table, make sure the OID of the master table is included in the CTE
        let key: String = match &self.datasource {
            Datasource::InheritorTable {
                parent_datasource, ..
            } => format!(
                ", d.MASTER{}_OID AS {datasource_alias}_KEY",
                parent_datasource.get_schema_oid()?
            ),
            Datasource::Column { column, .. } => {
                match column.column_type {
                    column_type::ColumnType::Object { table_oid, .. }
                    | column_type::ColumnType::Select { table_oid, .. } => {
                        if column.schema.oid == datasource_schema_oid {
                            // Relationship is reversed
                            format!(", d.COLUMN{} AS {datasource_alias}_KEY", column.oid)
                        } else {
                            String::from("")
                        }
                    }
                    _ => String::from(""),
                }
            }
            _ => String::from(""),
        };

        // Select the columns for OIDs/parameters from child datasources and the FROM/JOIN tables/CTEs
        let mut oid_columns_and_datasources_raw: Vec<(String, String)> = Vec::new();
        for child_datasource in self.child_datasources.into_iter() {
            let child_datasource_alias: String = child_datasource.get_alias();
            oid_columns_and_datasources_raw.push((
                format!("{child_datasource_alias}.*"),
                match &child_datasource {
                    Datasource::Table { .. } => {
                        // This case shouldn't happen
                        format!("INNER JOIN {child_datasource_alias}")
                    }
                    Datasource::MasterTable { table_oid, .. } => {
                        format!("INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.MASTER{table_oid}_OID")
                    }
                    Datasource::InheritorTable { .. } => {
                        format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                    }
                    Datasource::Column { column, .. } => {
                        match column.column_type {
                            column_type::ColumnType::Object { table_oid, .. }
                            | column_type::ColumnType::Select { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.COLUMN{}", column.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            column_type::ColumnType::Multiselect { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{table_oid}_OID FROM MULTISELECT{} m WHERE m.TABLE{}_OID = d.OID)", column.oid, column.schema.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{}_OID FROM MULTISELECT{} m WHERE m.TABLE{table_oid}_OID = d.OID)", column.schema.oid, column.oid)
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            _ => {
                                return Err(Error::InvalidDatasourceColumn { 
                                    column_oid: column.oid.clone(), 
                                    column_name: column.name.clone(), 
                                    column_type: column.column_type.to_str()
                                });
                            }
                        }
                    }
                }
            ));
        }
        let (oid_columns, datasources) = oid_columns_and_datasources_raw.into_iter().fold(
            (
                format!("d.OID AS {datasource_alias}_OID{key}"),
                format!("FROM TABLE{datasource_schema_oid} d"),
            ),
            |(acc1, acc2), (e1, e2)| (format!("{acc1}, {e1}"), format!("{acc2} {e2}")),
        );

        // Compile all columns
        let all_columns: String = self
            .columns
            .into_iter()
            .map(|(_, column)| {
                format!(
                    "{} AS {}, {} AS {}",
                    column.label_expr, column.label_ord, column.value_expr, column.value_ord
                )
            })
            .fold(oid_columns, |acc, e| format!("{acc}, {e}"));

        Ok(format!(
            "
            {datasource_alias} AS (
                SELECT 
                    {all_columns} 
                {datasources}
                WHERE NOT d.TRASH
            )
            "
        ))
    }
}

/// Represents an expression returning a scalar value.
#[derive(PartialEq, Eq, Clone)]
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The scalar type returned by the arg_expr SQL expression.
    arg_type: ExpressionReturnType,

    /// The SQL expression resulting in a scalar value representing the true value of the parameter.
    /// This will typically be the same as arg_expr, with the exception that Select/Multiselect/Object columns will have their primary keys
    /// returned by arg_expr and their referenced row OIDs returned by value_expr.
    value_expr: String,

    /// The SQL expression for the label of that scalar value (e.g. primary key of the row referenced by a Select column).
    label_expr: String,

    /// The SQL expression for the parameter returned by the expression, if it returns the value of an unmodified parameter.
    param_expr: String,

    /// True if the expressions are deterministic. False if RANDOM() is invoked.
    deterministic: bool,
}

struct SchemaView {
    param_cte: HashMap<Datasource, ParamCTE>,
    columns: HashMap<i64, SchemaViewColumn>,
    rand_count: usize,
}

impl SchemaView {
    /// Construct an empty schema view object.
    fn new() -> Self {
        Self {
            param_cte: HashMap::new(),
            columns: HashMap::new(),
            rand_count: 0,
        }
    }

    fn compile(self, trans: &Transaction, schema_oid: i64) -> Result<String, Error> {
        // Compile the CTEs and select only from root datasources
        let (with_expr, star_expr, oid_expr, from_expr) = if self.param_cte.len() > 0 {
            let mut with_expr: String = String::from("WITH");
            let mut oid_expr: Vec<String> = Vec::new();
            let mut filter_expr: String = String::from("");
            let mut from_expr: String = String::from("FROM");

            let mut root_datasource: HashSet<Datasource> = HashSet::new();
            let mut all_1_to_1: bool = true;
            for (cte_datasource, cte) in self.param_cte.into_iter() {
                // If the CTE is not being grouped, check if it has a 1-to-* relationship with its root
                if (!cte.is_grouped) {
                    match cte_datasource.seek_basis()? {
                        Datasource::Table { .. } => {},
                        _ => {
                            // If basis datasource is not a root datasource, flag there as being a datasource which is not 1-to-1 with the root
                            all_1_to_1 = false;
                        }
                    }
                }
                
                root_datasource.insert(cte_datasource.seek_root());

                let cte_datasource_alias: String = cte_datasource.get_alias();

                // Compile the CTE
                with_expr = format!(
                    "{with_expr}{} {}",
                    if with_expr == "WITH" { "" } else { "," },
                    cte.compile()?
                );

                // Select OID from the datasource
                oid_expr.push(format!("{cte_datasource_alias}_OID"));
                filter_expr = format!(
                    "{filter_expr}{}{}",
                    if filter_expr == "" { "'" } else { " || '&" },
                    format!("{cte_datasource_alias}=' || CAST({cte_datasource_alias}_OID AS TEXT)")
                );

                // If the datasource is a root, select from it
                if let Datasource::Table { .. } = cte_datasource {
                    from_expr = format!(
                        "{from_expr}{} {}",
                        if from_expr == "FROM" {
                            ""
                        } else {
                            " INNER JOIN"
                        },
                        cte_datasource.get_alias()
                    );
                }
            }
            (
                with_expr,
                {
                    let mut star_columns: Vec<String> = root_datasource
                        .iter()
                        .map(|root_datasource| format!("{}.*", root_datasource.get_alias()))
                        .collect();

                    let mut k: usize = 1;
                    while k <= self.rand_count {
                        star_columns.push(format!("RANDOM() AS RANDOM{k}"));
                        k += 1;
                    }

                    star_columns
                        .into_iter()
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                },
                oid_expr.into_iter().fold(
                    format!(
                        "{} AS QUERY_FILTER{}",
                        if filter_expr == "" {
                            String::from("''")
                        } else {
                            filter_expr
                        },
                        if root_datasource.len() == 1 && all_1_to_1 {
                            let root_datasource: Datasource =
                                root_datasource.into_iter().next().unwrap();
                            format!(
                                ", {} AS TABLE_OID, {}_OID AS OID", 
                                root_datasource.get_schema_oid()?,
                                root_datasource.get_alias()
                            )
                        } else {
                            String::from(", NULL AS TABLE_OID, NULL AS OID")
                        }
                    ),
                    |acc, e| format!("{acc}, {e}"),
                ),
                from_expr,
            )
        } else {
            (
                String::from(""),
                String::from(""),
                String::from("'' AS QUERY_FILTER, NULL AS TABLE_OID, NULL AS OID"),
                String::from(""),
            )
        };

        // Compile the final CTE
        // The purpose of an intermediary CTE before the top-level SELECT is to synchronize random values across value/label/arg/param expressions
        // TODO: Can remove the intermediary CTE if no random values are required?
        let with_expr: String = {
            if with_expr == "" {
                String::from("")
            } else {
                format!(
                    "{with_expr}, {}",
                    format!("FINAL_CTE AS (SELECT {star_expr} {from_expr})")
                )
            }
        };

        // Compile ordering columns
        let mut index_ordering_expr: String = String::from("");
        for row_result in trans.prepare("SELECT COLUMN_OID, SORT_ASCENDING FROM METADATA_SCHEMA_ORDERBY_VIEW WHERE SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)))? {
            let (column_oid, sort_ascending) = row_result?;

            if let Some(c) = self.columns.get(&column_oid) {
                if index_ordering_expr == "" {
                    index_ordering_expr = format!("{} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
                } else {
                    index_ordering_expr = format!("{index_ordering_expr}, {} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
                }
            }
        }

        // Compile each column
        let column_expr: String = {
            let mut column_expr: Vec<String> = Vec::new();
            for (_, c) in self.columns.into_iter() {
                column_expr.push(c.compile()?);
            }
            column_expr
                .into_iter()
                .fold(oid_expr, |acc, e| format!("{acc}, {e}"))
        };

        // Compile the SELECT
        Ok(format!(
            "
            CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_VIEW AS 
            {with_expr}
            SELECT 
                ROW_NUMBER() OVER ({index_ordering_expr}) AS ROW_INDEX, {column_expr}
            {}
            ",
            if with_expr == "" {
                ""
            } else {
                "FROM FINAL_CTE"
            }
        ))
    }

    /// Adds a CTE for params from a datasource.
    fn add_datasource_cte(&mut self, datasource: &Datasource, is_grouped: bool) {
        if !self.param_cte.contains_key(&datasource) {
            // Add the parent datasource
            if let Some(parent_datasource) = datasource.get_parent() {
                self.add_datasource_cte(&parent_datasource, is_grouped.clone());

                // Link the datasource to its parent
                if let Some(parent_datasource_cte) = self.param_cte.get_mut(&parent_datasource) {
                    parent_datasource_cte
                        .child_datasources
                        .insert(datasource.clone());
                }
            }

            // Add a CTE for the datasource
            self.param_cte.insert(
                datasource.clone(),
                ParamCTE {
                    datasource: datasource.clone(),
                    child_datasources: HashSet::new(),
                    columns: HashMap::new(),
                    is_grouped
                },
            );
        } else if let Some(cte) = self.param_cte.get_mut(&datasource) {
            cte.is_grouped = cte.is_grouped && is_grouped;
        }
    }

    ///
    fn add_column(
        &mut self,
        trans: &Transaction,
        root_datasource: &Option<Datasource>,
        datasource_path: String,
        column_metadata: column::FullMetadata,
    ) -> Result<(), Error> {
        match &column_metadata.column_type {
            column_type::ColumnType::Primitive(_)
            | column_type::ColumnType::Object { .. }
            | column_type::ColumnType::Select { .. }
            | column_type::ColumnType::Multiselect { .. } => {
                if let Some(root_datasource) = root_datasource {
                    // Add the primitive column as a param
                    let column_oid: i64 = column_metadata.oid.clone();
                    let column_datasource: Datasource =
                        root_datasource.append_path(datasource_path)?;
                    let (_, access_param) = self.add_param(column_datasource, column_metadata, false)?;

                    // Register the column to the query
                    self.columns.insert(
                        column_oid.clone(),
                        SchemaViewColumn::TableData {
                            label_expr: format!("{}", access_param.label_ord),
                            label_ord: format!("COLUMN{}_LABEL", column_oid),
                            value_expr: format!("{}", access_param.value_ord),
                            value_ord: format!("COLUMN{}_VALUE", column_oid),
                        },
                    );
                } else {
                    return Err(Error::OrphanedDataColumn {
                        column_oid: column_metadata.oid,
                        column_name: column_metadata.name,
                    });
                };
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                let scalar_expression: ScalarExpression = self.compile_formula(
                    trans,
                    column_metadata.schema.oid,
                    match root_datasource {
                        Some(root_datasource) => root_datasource.append_path(datasource_path)?,
                        None => Datasource::from_alias_transact(trans, datasource_path)?,
                    },
                    parsed_formula,
                    false
                )?;

                // Turn into a column
                self.columns.insert(
                    column_metadata.oid.clone(),
                    SchemaViewColumn::Formula {
                        label_expr: scalar_expression.label_expr,
                        label_ord: format!("COLUMN{}_LABEL", column_metadata.oid),
                        value_expr: scalar_expression.value_expr,
                        value_ord: format!("COLUMN{}_VALUE", column_metadata.oid),
                        param_expr: scalar_expression.param_expr,
                        param_ord: format!("COLUMN{}_PARAM", column_metadata.oid),
                    },
                );
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                // Iterate through key columns of the report
                for key_column_result in trans.prepare("SELECT c.OID FROM METADATA_SCHEMA_COLUMN_VIEW sc INNER JOIN METADATA_COLUMN c ON c.OID = sc.COLUMN_OID WHERE sc.IS_REQUIRED AND c.IS_PRIMARY_KEY AND sc.SCHEMA_OID = ?1")?.query_map(params![report_oid], |row| row.get::<_, i64>("OID"))? {
                    let key_column_oid: i64 = key_column_result?;
                    let key_column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, key_column_oid)?;

                    // Compile the formula

                }

                let label_expr: String = format!("COALESCE(GROUP_CONCAT(), '')");

                // Turn into a column
                self.columns.insert(
                    column_metadata.oid.clone(),
                    SchemaViewColumn::Subreport { 
                        label_expr, 
                        label_ord: format!("COLUMN{}_LABEL", column_metadata.oid) 
                    }
                );
            }
            _ => {
                // Ignore other virtual column types
            }
        }
        Ok(())
    }

    /// Adds a data cell to a datasource.
    fn add_param(
        &mut self,
        datasource: Datasource,
        column: column::FullMetadata,
        is_grouped: bool
    ) -> Result<(Datasource, ParamCTEColumn), Error> {
        // Ensure the CTE for the datasource exists
        self.add_datasource_cte(&datasource, is_grouped);

        // Add the column to that CTE
        let column_path: String = format!("{}_COLUMN{}", datasource.get_alias(), column.oid);
        let param = if let Some(datasource_cte) = self.param_cte.get_mut(&datasource) {
            if !datasource_cte.columns.contains_key(&column_path) {
                datasource_cte.columns.insert(column_path.clone(), match column.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::Boolean => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Integer => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Number => ParamCTEColumn { 
                                label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::PlainText => ParamCTEColumn { 
                                label_expr: format!("d.COLUMN{}", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"),  
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::JsonText => ParamCTEColumn { 
                                label_expr: format!("d.COLUMN{}", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::JSON),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Date => ParamCTEColumn { 
                                label_expr: format!("DATE(d.COLUMN{}, 'julianday')", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Date),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::Datetime => ParamCTEColumn { 
                                label_expr: format!("STRFTIME('%FT%TZ', d.COLUMN{}, 'julianday')", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Datetime),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            },
                            column_type::Primitive::File | column_type::Primitive::Image => ParamCTEColumn { 
                                label_expr: format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = d.COLUMN{})", column.oid), 
                                label_ord: format!("{column_path}_LABEL"), 
                                value_expr: format!("d.COLUMN{}", column.oid), 
                                value_ord: format!("{column_path}_VALUE"), 
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::File),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Object { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("(SELECT l.OBJECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("d.COLUMN{}", column.oid),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                        FROM TABLE{table_oid}_LABEL_VIEW l 
                                        WHERE l.COLUMN{} = d.OID
                                    )
                                    ", 
                                    column.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid,
                                    column.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT
                                            s.OID
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = {}_OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid, 
                                    column.oid,
                                    datasource.get_alias()
                                ), 
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Select { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("(SELECT l.SELECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("d.COLUMN{}", column.oid),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("{column_path}_VALUE"), 
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                        FROM TABLE{table_oid}_LABEL_VIEW l 
                                        WHERE l.COLUMN{} = d.OID
                                    )
                                    ", 
                                    column.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid,
                                    column.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT
                                            s.OID
                                        FROM TABLE{} s
                                        WHERE s.COLUMN{} = {}_OID AND NOT s.TRASH
                                    )
                                    ", 
                                    column.schema.oid, 
                                    column.oid,
                                    datasource.get_alias()
                                ), 
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    column_type::ColumnType::Multiselect { table_oid, .. } => {
                        if column.schema.oid == datasource.get_schema_oid()? {
                            // Is normal
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || GROUP_CONCAT(l.JSON_LABEL, ', ') || ']'
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{table_oid}_LABEL_VIEW l ON l.OID = m.TABLE{table_oid}_OID 
                                        WHERE m.TABLE{}_OID = d.OID
                                    )", 
                                    column.oid,
                                    column.schema.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{table_oid} t ON t.OID = m.TABLE{table_oid}_OID 
                                        WHERE m.TABLE{}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT 
                                            TABLE{table_oid}_OID
                                        FROM MULTISELECT{}
                                        WHERE TABLE{}_OID = {}_OID AND TABLE{table_oid}_OID IN (SELECT OID FROM TABLE{table_oid} WHERE NOT TRASH)
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    datasource.get_alias()
                                ),
                                arg_type: ExpressionReturnType::new_record(table_oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid: column.schema.oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        } else {
                            // Is reversed
                            ParamCTEColumn {
                                label_expr: format!("
                                    (
                                        SELECT 
                                            '[' || GROUP_CONCAT(l.JSON_LABEL, ', ') || ']'
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{}_LABEL_VIEW l ON l.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid
                                ),
                                label_ord: format!("{column_path}_LABEL"),
                                value_expr: format!("
                                    (
                                        SELECT 
                                            GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid 
                                ),
                                value_ord: format!("{column_path}_VALUE"),
                                arg_expr: format!("
                                    (
                                        SELECT 
                                            t.OID
                                        FROM MULTISELECT{} m 
                                        INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                        WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                    )", 
                                    column.oid,
                                    column.schema.oid,
                                    column.schema.oid 
                                ),
                                arg_type: ExpressionReturnType::new_record(column.schema.oid),
                                cell: Some(ParamCTEColumnCell {
                                    table_oid,
                                    column_oid: column.oid,
                                    row_ord: format!("{}_OID", datasource.get_alias())
                                })
                            }
                        }
                    }
                    _ => {
                        // Column cannot be added as a parameter
                        return Err(Error::InvalidParameter { 
                            column_oid: column.oid, 
                            column_name: column.name, 
                            column_type: column.column_type.to_str() 
                        });
                    }
                });
            }

            datasource_cte.columns[&column_path].clone()
        } else {
            return Err(Error::InvalidDatasource {
                datasource_alias: datasource.get_alias(),
            });
        };
        Ok((datasource.seek_root(), param))
    }

    /// Compile the formula into SQL.
    fn compile_formula(
        &mut self,
        trans: &Transaction,
        root_oid: i64,
        root_datasource: Datasource,
        formula: Box<Formula>,
        is_grouped: bool
    ) -> Result<ScalarExpression, Error> {
        Ok(match *formula {
            Formula::Null => ScalarExpression {
                arg_expr: String::from("NULL"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Null),
                value_expr: String::from("NULL"),
                label_expr: String::from("NULL"),
                param_expr: String::from("NULL"),
                deterministic: true,
            },
            Formula::LiteralBool(b) => {
                let (value_expr, label_expr) = if b {
                    (String::from("TRUE"), String::from("'True'"))
                } else {
                    (String::from("FALSE"), String::from("'False'"))
                };
                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                    value_expr,
                    label_expr,
                    param_expr: String::from("'boolean'"),
                    deterministic: true,
                }
            }
            Formula::LiteralInt(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("'integer'"),
                deterministic: true,
            },
            Formula::LiteralFloat(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("'number'"),
                deterministic: true,
            },
            Formula::LiteralString(str) => {
                let safe_str: String = format!("'{}'", str.replace("'", "''"));
                ScalarExpression {
                    arg_expr: safe_str.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                    value_expr: safe_str.clone(),
                    label_expr: safe_str.clone(),
                    param_expr: String::from("'textplain'"),
                    deterministic: true,
                }
            }
            Formula::RandomInt => {
                self.rand_count += 1;
                ScalarExpression {
                    arg_expr: format!("RANDOM{}", self.rand_count),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    value_expr: format!("RANDOM{}", self.rand_count),
                    label_expr: format!("CAST(RANDOM{} AS TEXT)", self.rand_count),
                    param_expr: String::from("'integer'"),
                    deterministic: false,
                }
            }
            Formula::Param {
                datasource_alias,
                column_oid,
            } => {
                let column_datasource: Datasource =
                    Datasource::from_alias(datasource_alias.clone())?
                        .substitute_root(root_oid.clone(), root_datasource.clone());
                let column_metadata =
                    column::FullMetadata::get_transact(trans, column_oid.clone())?;
                match &column_metadata.column_type {
                    column_type::ColumnType::Primitive(_)
                    | column_type::ColumnType::Object { .. }
                    | column_type::ColumnType::Select { .. }
                    | column_type::ColumnType::Multiselect { .. } => {
                        let param_name: String = match &column_metadata.column_type {
                            column_type::ColumnType::Primitive(prim) => String::from(match prim {
                                column_type::Primitive::Boolean => "boolean",
                                column_type::Primitive::Integer => "integer",
                                column_type::Primitive::Number => "number",
                                column_type::Primitive::PlainText => "text/plain",
                                column_type::Primitive::JsonText => "text/JSON",
                                column_type::Primitive::File => "file/any",
                                column_type::Primitive::Image => "file/image",
                                column_type::Primitive::Date => "dateonly",
                                column_type::Primitive::Datetime => "datetime",
                            }),
                            column_type::ColumnType::Object { table_oid, .. } => {
                                format!("object{table_oid}")
                            }
                            column_type::ColumnType::Select { table_oid, .. } => {
                                format!("select{table_oid}")
                            }
                            column_type::ColumnType::Multiselect { table_oid, .. } => {
                                format!("multiselect{table_oid}")
                            }
                            _ => String::from("N/A"), // This case shouldn't happen
                        };

                        let (_, param) = self.add_param(column_datasource, column_metadata, is_grouped)?;

                        // Parameter expressions return a string in the form "{TABLE_OID}:{COLUMN_OID}:{ROW_OID}"
                        let param_expr: String = if let Some(param_cell) = param.cell {
                            format!(
                                "('{param_name}:{}:{}:' || CAST({} AS TEXT))",
                                param_cell.table_oid, param_cell.column_oid, param_cell.row_ord
                            )
                        } else {
                            String::from("NULL") // This case shouldn't happen?
                        };

                        // Bubble up the expression to get the parameter
                        ScalarExpression {
                            arg_expr: param.arg_expr,
                            arg_type: param.arg_type,
                            value_expr: param.value_expr,
                            label_expr: param.label_expr,
                            param_expr,
                            deterministic: true,
                        }
                    }
                    column_type::ColumnType::Formula { formula, .. } => {
                        // Parse the formula
                        let parsed_formula: Box<Formula> =
                            Box::new(Formula::parse(formula.clone())?);

                        // Compile the formula into a scalar expression
                        let formula_root_oid: i64 =
                            match Datasource::get_default_datasource_transact(
                                trans,
                                column_datasource.get_schema_oid()?,
                            )? {
                                Datasource::Table { oid, .. } => oid,
                                _ => {
                                    // This case should not ever occur, but just in case...
                                    return Err(Error::AdhocError("get_default_datasource_transact() function did not return a Datasource::Table, which is not allowed."));
                                }
                            };
                        self.compile_formula(
                            trans,
                            formula_root_oid,
                            column_datasource,
                            parsed_formula,
                            is_grouped
                        )?
                    }
                    _ => {
                        // Column type is not allowed to be used as a parameter in a formula
                        return Err(Error::InvalidParameter {
                            column_oid,
                            column_name: column_metadata.name,
                            column_type: column_metadata.column_type.to_str(),
                        });
                    }
                }
            }
            Formula::Coalesce(items) => {
                let mut items_compiled: Vec<ScalarExpression> = Vec::new();
                for item in items {
                    let item_compiled: ScalarExpression = self.compile_formula(
                        trans,
                        root_oid.clone(),
                        root_datasource.clone(),
                        Box::new(item),
                        is_grouped.clone()
                    )?;
                    items_compiled.push(item_compiled);
                }

                let deterministic: bool = items_compiled
                    .iter()
                    .all(|item_compiled| item_compiled.deterministic);
                let arg_type: ExpressionReturnType = items_compiled.iter().fold(
                    ExpressionReturnType::new_primitive(PrimitiveScalarType::Null),
                    |acc, item_compiled| acc.generalize(&item_compiled.arg_type),
                );
                let (label_expr, param_expr) = if items_compiled.len() > 1 {
                    (
                        format!(
                            "{} ELSE {} END",
                            items_compiled.iter().take(items_compiled.len() - 1).fold(
                                String::from("CASE"),
                                |acc, item_compiled| format!(
                                    "{acc} WHEN {} IS NOT NULL THEN {}",
                                    item_compiled.value_expr, item_compiled.label_expr
                                )
                            ),
                            items_compiled[items_compiled.len() - 1].label_expr
                        ),
                        format!(
                            "{} ELSE {} END",
                            items_compiled.iter().take(items_compiled.len() - 1).fold(
                                String::from("CASE"),
                                |acc, item_compiled| format!(
                                    "{acc} WHEN {} IS NOT NULL THEN {}",
                                    item_compiled.value_expr, item_compiled.param_expr
                                )
                            ),
                            items_compiled[items_compiled.len() - 1].param_expr
                        ),
                    )
                } else if items_compiled.len() == 1 {
                    (
                        items_compiled[0].label_expr.clone(),
                        items_compiled[0].param_expr.clone(),
                    )
                } else {
                    (String::from("NULL"), String::from("NULL"))
                };
                let (value_expr, arg_expr) = match items_compiled
                    .into_iter()
                    .map(|item_compiled| (item_compiled.value_expr, item_compiled.arg_expr))
                    .reduce(|(acc_value, acc_arg), (e_value, e_arg)| {
                        (
                            format!("{acc_value}, {e_value}"),
                            format!("{acc_arg}, {e_arg}"),
                        )
                    }) {
                    Some((acc_value, acc_arg)) => (
                        format!("COALESCE({acc_value})"),
                        format!("COALESCE({acc_arg})"),
                    ),
                    None => (String::from("NULL"), String::from("NULL")),
                };

                ScalarExpression {
                    arg_expr,
                    arg_type,
                    value_expr,
                    label_expr,
                    param_expr,
                    deterministic,
                }
            }
            Formula::Abs(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "abs",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("ABS({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: inner_compiled.arg_type,
                    label_expr,
                    value_expr,
                    param_expr: inner_compiled.param_expr,
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Sign(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "sign",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("SIGN({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Floor(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "floor",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("FLOOR({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Ceiling(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "ceil",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("CEILING({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            Formula::Round(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression =
                    self.compile_formula(trans, root_oid, root_datasource, inner, is_grouped)?;
                if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number)
                    .accepts_arg(&inner_compiled.arg_type)
                {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "round",
                        inner_name,
                        expected_type: ExpressionReturnType::new_primitive(
                            PrimitiveScalarType::Number,
                        )
                        .to_string(trans),
                        received_type: inner_compiled.arg_type.to_string(trans),
                    });
                }

                let value_expr: String = format!("ROUND({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                    label_expr,
                    value_expr,
                    param_expr: String::from("'integer'"),
                    deterministic: inner_compiled.deterministic,
                }
            }
            _ => {
                todo!("Function {} is not implemented yet!", formula.to_string());
            }
        })
    }

    /// Compile a subreport label.
    fn compile_subreport_label(
        &mut self,
        trans: &Transaction,
        root_oid: i64,
        root_datasource: Datasource,
        formula: Box<Formula>,
        is_grouped: bool
    ) {
        
    }
}

/// Create a view for the table.
fn create_schema_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    println!("Creating schema view SCHEMA{schema_oid}_VIEW...");

    // Get the root table datasource for the view
    let mut view: SchemaView = SchemaView::new();
    let root_datasource: Option<Datasource> = if let Some(root_datasource_oid) = trans
        .query_one(
            "SELECT OID FROM METADATA_DATASOURCE WHERE TABLE_OID = ?1 LIMIT 1",
            params![schema_oid],
            |row| row.get("OID"),
        )
        .optional()?
    {
        let root_datasource: Datasource = Datasource::get_transact(trans, root_datasource_oid)?;
        view.add_datasource_cte(&root_datasource, false);
        Some(root_datasource)
    } else {
        None
    };

    // Add each column that belongs to the schema
    for row_result in trans.prepare("SELECT DATASOURCE_PATH, COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED")?.query_map(params![schema_oid], |row| Ok((row.get::<_, String>("DATASOURCE_PATH")?, row.get::<_, i64>("COLUMN_OID")?)))? {
        let (datasource_path, column_oid) = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
        view.add_column(trans, &root_datasource, datasource_path, column_metadata)?;
    }

    let create_sql: String = view.compile(trans, schema_oid)?;
    println!("{create_sql}");
    trans.execute(&create_sql, [])?;
    Ok(())
}

/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    let mut view_creation_ordering: Vec<ViewsToCreate> = Vec::new();
    drop_all_views(trans, schema_oid, &mut view_creation_ordering)?;

    // Create all of the polymorphism views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_polymorphism_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_polymorphism_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the label views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_label_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_label_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the schema views
    for v in view_creation_ordering.iter() {
        create_schema_view(trans, v.schema_oid)?;
    }
    Ok(())
}
