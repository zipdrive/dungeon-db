use std::collections::{
    HashMap,
    HashSet
};
use rusqlite::{Connection};
use crate::util::error::Error;
use crate::data::datasource::Datasource;
use crate::data::column_type;


#[derive(Clone)]
pub struct SelectParameterType {
    /// The primitive types that the parameter can conform to.
    primitive_types: HashSet<column_type::Primitive>
}

impl SelectParameterType {
    /// Creates a new type representing a null value.
    pub fn new() -> Self {
        Self {
            primitive_types: HashSet::new()
        }
    }

    /// Creates a new type representing a specific primitive type.
    pub fn from(prim: column_type::Primitive) -> Self {
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
    pub fn generalize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.union(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Constructs a type that represents the most general type that conforms to both this type and the given type.
    pub fn specialize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.intersection(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Returns true if an instance of the given type can always be passed as a value of this type.
    pub fn encompasses(&self, other: &Self) -> bool {
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
    pub fn construct_plain_label_expr(&self, value_expr: &String) -> String {
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
    pub fn construct_json_label_expr(&self, value_expr: &String) -> String {
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
    pub fn to_string(&self) -> String {
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



pub struct SelectParameter {
    pub plain_label_expr_norecursion: String,
    pub plain_label_expr_recursion: String,
    pub json_label_expr_norecursion: String,
    pub json_label_expr_recursion: String,
    pub value_expr_norecursion: String,
    pub value_expr_recursion: String,
    pub cell_expr: String,
    pub isolated_dependency_exprs: HashSet<String>,
    pub full_reload_dependency_exprs: HashSet<String>,
    pub scalar_type: SelectParameterType,
    pub context: SelectParameterContext
}

impl SelectParameter {
    /// Constructs a new scalar parameter with no recursion.
    pub fn new_norecursion(plain_label_expr: String, json_label_expr: String, value_expr: String, cell_expr: String, isolated_dependency_exprs: HashSet<String>, full_reload_dependency_exprs: HashSet<String>, scalar_type: SelectParameterType, context: SelectParameterContext) -> Self {
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
pub enum SelectParameterSlice {
    None,
    NthValue(String)
}



#[derive(Clone)]
pub enum SelectParameterContext {
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
    pub fn wrap_collection(inner_expr: String, slice: &SelectParameterSlice, filter_expr: &Option<String>, order_exprs: &Vec<(String, bool)>, min_depth: &HashMap<i64, Option<Datasource>>) -> String {
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
    pub fn wrap(&self, inner_expr_norecursion: String, inner_expr_recursion: String) -> (String, String) {
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
    pub fn disable_window_changes(&mut self) {
        if let Self::Collection { window_changes_disabled, .. } = self {
            *window_changes_disabled = true;
        }
    }
}




#[derive(Clone)]
pub struct SelectParameterDatasource {
    /// The datasource being selected from.
    datasource: Datasource,

    /// The alias of the CTE being pulled from.
    /// Recursive if not "w".
    alias: String 
}

impl SelectParameterDatasource {
    /// Constructs a new non-recursive datasource.
    fn new(datasource: Datasource) -> Self {
        Self {
            datasource,
            alias: String::from("w")
        }
    }

    /// Constructs a new non-recursive datasource that branches from this datasource.
    /// i.e. 
    fn branch_norecursion(&self, conn: &Connection, datasource: Datasource) -> Result<Self, Error> {
        Ok(Self {
            datasource: datasource.substitute_root(
                {
                    let table_oid: i64 = self.datasource.get_table_oid()?;
                    Datasource::get_default_datasource_oid_transact(conn, table_oid)?
                }, 
                self.datasource.clone()
            ),
            alias: String::from("w")
        })
    }

    fn branch_recursion(&self, datasource: Datasource) -> Self {
        Self {

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