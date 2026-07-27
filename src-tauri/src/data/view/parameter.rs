use std::collections::{
    HashMap,
    HashSet
};
use rusqlite::{Connection};
use crate::util::error::Error;
use crate::data::datasource::Datasource;
use crate::data::column_type;







#[derive(Clone)]
pub enum SelectParameterSlice {
    None,
    NthValue(String)
}

#[derive(Clone)]
pub struct SelectParameterDatasource {
    /// The datasource where the cell or formula lives.
    /// None if a formula on a report.
    datasource: Datasource,

    /// The alias of the CTE to retrieve data from that datasource.
    cte_alias: String 
}

impl SelectParameterDatasource {
    /// Constructs a new, non-recursive datasource.
    pub fn new(datasource: Datasource) -> Self {
        Self {
            datasource,
            cte_alias: String::from("w")
        }
    }



    /// Constructs a new, recursive datasource.
    pub fn new_recursive(datasource: Datasource, cte_alias: String) -> Self {
        Self {
            datasource,
            cte_alias
        }
    }

    /// Gets the underlying datasource.
    pub fn get_datasource<'a>(&'a self) -> &'a Datasource {
        &self.datasource
    }

    /// Constructs an expression to get the OID of the datasource.
    fn get_oid_expr(&self) -> String {
        format!("{}.{}_OID", self.cte_alias, self.datasource.get_alias())
    }

    /// Constructs an expression to get the table OID (accounting for inheritance) of the datasource.
    fn get_inheritor_table_expr(&self) -> String {
        format!("{}.{}_INHERITOR", self.cte_alias, self.datasource.get_alias())
    }

    /// Constructs an expression to get the row OID in the lowest inheritor table.
    fn get_inheritor_row_expr(&self) -> String {
        format!("{}.{}_INHERITOR_ROW", self.cte_alias, self.datasource.get_alias())
    }

    /// Gets the value of a primitive column.
    fn get_value_expr(&self, value_ord: String) -> String {
        format!("{}.{value_ord}", self.cte_alias)
    }
}

#[derive(Clone)]
pub enum SelectParameterContext {
    /// A scalar value.
    Scalar {
        /// The table where the cell or formula lives.
        datasource: Option<SelectParameterDatasource>
    },

    /// A collection.
    /// Induced by aggregate functions, IN operators, and literal Lists.
    Collection {
        /// The table where the formula lives.
        /// None if the formula is on a report.
        datasource: Option<SelectParameterDatasource>, 

        /// How the collection is sliced, if at all.
        slice: SelectParameterSlice,

        /// The expression used to filter the collection.
        filter_expr: Option<String>,

        /// The first item in the tuple is the expression that is sorted over.
        /// The second item in the tuple is true if the order is ascending, and false if descending.
        order_exprs: Vec<(String, bool)>,

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
    pub fn wrap(&self, inner_expr: String) -> String {
        match self {
            Self::Scalar => inner_expr,
            Self::Collection { slice, filter_expr, order_exprs, min_depth, .. } => 
                Self::wrap_collection(inner_expr, slice, filter_expr, order_exprs, min_depth)
        }
    }

    /// Disables changes to the window.
    pub fn disable_window_changes(&mut self) {
        if let Self::Collection { window_changes_disabled, .. } = self {
            *window_changes_disabled = true;
        }
    }
}


struct SelectParameter {
    /// The expression for the value of the parameter.
    pub value_expr: String,

    /// The scalar type of the parameter.
    pub scalar_type: SelectParameterType,

    /// The context enclosing the parameter.
    pub context: SelectParameterContext
}



#[derive(Clone)]
pub struct SelectExpressions {
    /// The expression for a value.
    pub value_expr: String,

    /// The expression for a plaintext label.
    pub plain_label_expr: String,

    /// The expression for a label that conforms to the JSON format.
    pub json_label_expr: String,

    /// The expression for the associated cell, in the form '{TYPE}:{TABLE_OID}:{COLUMN_OID}:{ROW_OID}'.
    pub cell_expr: String,

    /// The expressions for the cells that the expressions have a 1-to-1 dependency on.
    /// Each item in the set is a comma-separated list of cells. ROW_OID may be a '*' character, indicating the dependency is on every cell in the column.
    pub isolated_dependency_exprs: HashSet<String>,

    /// The expressions for the cells that the expressions have a 1-to-* dependency on.
    /// Each item in the set is a comma-separated list of cells. ROW_OID may be a '*' character, indicating the dependency is on every cell in the column.
    pub full_reload_dependency_exprs: HashSet<String>,

    /// The context of the parameter.
    pub context: SelectParameterContext
}



pub trait SelectParameter {
    /// Constructs a new parameter that is guaranteed to be non-recursive.
    fn new(exprs: SelectExpressions, scalar_type: SelectParameterType) -> Self;

    /// Maps the expressions to a different set of expressions.
    fn map<F>(&self, f: F, scalar_type: SelectParameterType) -> Self where F : Fn(&SelectExpressions) -> SelectExpressions;

    /// Gets the scalar type of the parameter.
    fn get_scalar_type<'a>(&'a self) -> &'a SelectParameterType;
}

pub struct SelectMainParameter {
    /// The expressions.
    exprs: SelectExpressions,

    /// The scalar type of the parameter.
    scalar_type: SelectParameterType
}

impl SelectParameter for SelectMainParameter {
    fn new(exprs: SelectExpressions, scalar_type: SelectParameterType) -> Self {
        Self {
            exprs,
            scalar_type
        }
    }

    fn map<F>(&self, f: F, scalar_type: SelectParameterType) -> Self where F : Fn(&SelectExpressions) -> SelectExpressions {
        Self {
            exprs: f(&self.exprs),
            scalar_type
        }
    }

    fn get_scalar_type<'a>(&'a self) -> &'a SelectParameterType {
        &self.scalar_type
    }
}

pub struct SelectLabelParameter {
    /// The expressions for the base case of the recursive CTE, or for the case of self-reference.
    nonrecursive_exprs: SelectExpressions,

    /// The expressions for the recursive case of the recursive CTE.
    recursive_exprs: SelectExpressions,

    /// The scalar type of the parameter.
    scalar_type: SelectParameterType
}

impl SelectParameter for SelectLabelParameter {
    fn new(exprs: SelectExpressions, scalar_type: SelectParameterType) -> Self {
        Self {
            nonrecursive_exprs: exprs.clone(),
            recursive_exprs: exprs,
            scalar_type
        }    
    }

    fn map<F>(&self, f: F, scalar_type: SelectParameterType) -> Self where F : Fn(&SelectExpressions) -> SelectExpressions {
        Self {
            nonrecursive_exprs: f(&self.nonrecursive_exprs),
            recursive_exprs: f(&self.recursive_exprs),
            scalar_type
        }
    }

    fn get_scalar_type<'a>(&'a self) -> &'a SelectParameterType {
        &self.scalar_type
    }
}
