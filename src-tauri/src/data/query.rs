use rusqlite::{Connection, Transaction, params};
use crate::data::datasource::Datasource;
use crate::data::{self, column, column_type, datasource, report, schema, table};
use crate::util::formula::Formula;
use crate::util::db;
use crate::util::error::Error;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet, VecDeque};
use bitflags::bitflags;


#[derive(PartialEq, Eq, Clone)]
struct ScalarType(u32);
bitflags! {
    impl ScalarType: u32 {
        const Null          = 0b00000000;
        const Any           = 0b11111111;
        const Boolean       = 0b00000001;
        const Integer       = 0b00000010;
        const Number        = 0b00000110;
        const Date          = 0b00001000;
        const Datetime      = 0b00011000;
        const Text          = 0b00100000;
        const JSON          = 0b01100000;
        const Blob          = 0b10000000;
    }
}

impl ScalarType {
    /// Converts from a scalar type to a string.
    fn to_string(&self) -> String {
        let mut flags: Vec<ScalarType> = self.iter().collect();
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
        flags.into_iter().map(|flag| match flag {
            Self::Null => String::from("null"),
            Self::Any => String::from("any"),
            Self::Boolean => String::from("boolean"),
            Self::Integer => String::from("integer"),
            Self::Number => String::from("number"),
            Self::Date => String::from("date"),
            Self::Datetime => String::from("timestamp"),
            Self::Text => String::from("text"),
            Self::JSON => String::from("JSON"),
            Self::Blob => String::from("file"),
            _ => String::from("unknown") // This case shouldn't ever happen; if it does, something has gone wrong
        }).reduce(|acc, e| format!("{acc} | {e}")).unwrap_or(String::from("null"))
    }
}

/// Represents an expression returning a scalar value.
#[derive(PartialEq, Eq, Clone)]
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The scalar type returned by the arg_expr SQL expression.
    arg_return_type: ScalarType,

    /// The SQL expression resulting in a scalar value representing the true value of the parameter.
    /// This will typically be the same as arg_expr, with the exception that Select/Multiselect/Object columns will have their primary keys 
    /// returned by arg_expr and their referenced row OIDs returned by value_expr.
    value_expr: String,

    /// The SQL expression for the label of that scalar value (e.g. primary key of the row referenced by a Select column).
    label_expr: String,

    /// The SQL expression for the parameter returned by the expression, if it returns the value of an unmodified parameter.
    param_expr: String,

    /// True if the expressions are deterministic. False if RANDOM() is invoked.
    deterministic: bool 
}



#[derive(PartialEq, Eq, Clone)]
enum TableOrSubquery {
    RootDatasource {
        datasource: Datasource,
        alias: String
    },
    DerivativeDatasource {
        datasource: Datasource,
        alias: String,
        on_clause: String
    },
    Array {
        values: Vec<ScalarExpression>,
        alias: String
    },
    Subquery {
        subquery: String,
        alias: String,
        on_clause: String
    }
}

impl Hash for TableOrSubquery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let alias_ref: &String = self.borrow();
        alias_ref.hash(state)
    }
}

impl Borrow<String> for TableOrSubquery {
    /// Borrows the alias of the table or subquery.
    fn borrow(&self) -> &String {
        match self {
            Self::RootDatasource { alias, .. }
            | Self::DerivativeDatasource { alias, .. }
            | Self::Array { alias, .. }
            | Self::Subquery { alias, .. }  => alias
        }
    }
}



pub enum QueryBuilderColumn {
    Primitive {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The column's primitive type.
        primitive_type: column_type::Primitive,

        /// The ordinal pointing to the String value of the primitive.
        label_ord: String,

        /// The SQL expression returning the String value of the primitive.
        label_expr: String,

        /// The SQL expression returning the raw primitive value.
        value_expr: String 
    },
    File {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The ordinal pointing to the String value of the primitive.
        label_ord: String,

        /// The SQL expression returning the String value of the primitive.
        label_expr: String,

        /// The ordinal pointing to the file OID.
        file_ord: String,

        /// The SQL expression returning the file OID.
        file_expr: String 
    },
    Object {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The ordinal pointing to the label of the Object.
        label_ord: String,

        /// The SQL expression returning the label of the Object.
        label_expr: String,

        /// The SQL expression returning the primary key of the Object as a JSON.
        json_expr: String,

        /// The schema OID of the Object.
        object_schema_oid: i64,

        /// The ordinal pointing to the row OID of the Object.
        object_query_string_ord: String,

        /// The SQL expression returning the row OID of the Object.
        object_query_string_expr: String 
    },
    Select {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The ordinal pointing to the label of the Select.
        label_ord: String,

        /// The SQL expression returning the label of the Select.
        label_expr: String,

        /// The SQL expression returning the primary key of the Object as a JSON.
        json_expr: String,

        /// The schema OID of the Select.
        select_schema_oid: i64,

        /// The ordinal pointing to the row OID of the Select.
        select_row_ord: String,

        /// The SQL expression returning the row OID of the Select.
        select_row_expr: String 
    },
    Multiselect {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The ordinal pointing to the label of the Multiselect.
        label_ord: String,

        /// The SQL expression returning the label of the Multiselect.
        label_expr: String,

        /// The schema OID of the Multiselect.
        select_schema_oid: i64,

        /// The ordinal pointing to the row OID of the Multiselect.
        select_row_ord: String,

        /// The SQL expression returning the row OID of the Multiselect.
        select_row_expr: String 
    },
    Formula {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The ordinal pointing to a possible '{DATASOURCE}:{COLUMN}' String 
        /// indicating the column that this Formula directly maps to.
        param_ord: String,

        /// The SQL expression returning a possible '{DATASOURCE}:{COLUMN}' String 
        /// indicating the column that this Formula directly maps to.
        param_expr: String,

        /// The ordinal pointing to the String value of the Formula.
        label_ord: String,

        /// The SQL expression returning the String value of the Formula.
        label_expr: String,

        /// The ordinal pointing to the true value of the Formula.
        value_ord: String,

        /// The SQL expression returning the true value of the Formula.
        value_expr: String,

        /// The SQL expression returning a value of the Formula that is to be used as an argument to functions and operators.
        arg_expr: String,

        /// The ScalarType returned by the arg_expr SQL expression.
        arg_return_type: ScalarType,

        /// True if the formula is deterministic. False if RANDOM() is invoked.
        deterministic: bool 
    },
    Subreport {
        schema_oid: i64,
        schema_row_ord: String,
        column_oid: i64,

        /// The metadata of the subreport.
        subreport_metadata: report::FullMetadata,
    }
}



pub struct QueryBuilder<'a> {
    /// The query wrapping this one, if this query is not the top-level query.
    parent_query: Option<&'a mut QueryBuilder<'a>>,

    /// The tables and subqueries that the SELECT statement pulls data from.
    tables_and_subqueries: VecDeque<TableOrSubquery>,

    /// The columns of the query.
    pub columns: Vec<QueryBuilderColumn>,

    /// Precompiled filter SQL expressions, applied before GROUP BY (if one exists).
    pregroup_filters: Vec<String>,

    /// The column indices to group the returned rows from the query by.
    group_by_indices: Vec<usize>,

    /// Precompiled filter SQL expressions, applied after GROUP BY (if one exists).
    postgroup_filters: Vec<String>,

    /// The column indices to order the query by.
    order_by_indices: Vec<(usize, bool)>
}

impl<'a> QueryBuilder<'a> {
    /// Creates a new top-level query.
    pub fn new(initial_datasources: Vec<Datasource>) -> Self {
        let mut query: Self = Self {
            parent_query: None,
            tables_and_subqueries: VecDeque::new(),
            columns: Vec::new(),
            pregroup_filters: Vec::new(),
            group_by_indices: Vec::new(),
            postgroup_filters: Vec::new(),
            order_by_indices: Vec::new()
        };
        for initial_datasource in initial_datasources {
            query.insert_datasource(initial_datasource);
        }
        return query;
    }

    /// Creates a new subquery.
    fn new_subquery(query: &'a mut Self) -> Self {
        Self {
            parent_query: Some(query),
            tables_and_subqueries: VecDeque::new(),
            columns: Vec::new(),
            pregroup_filters: Vec::new(),
            group_by_indices: Vec::new(),
            postgroup_filters: Vec::new(),
            order_by_indices: Vec::new()
        }
    }

    /// Compiles the query into SQL.
    /// The first element of the returned tuple is the SQL for the query.
    /// The second element of the returned tuple is the columns returned by the query.
    /// The third element of the returned tuple is a list of aliases for each datasource used by the query.
    pub fn compile(mut self) -> Result<Option<(String, Vec<QueryBuilderColumn>, Vec<String>)>, Error> {
        // Compile list of datasource aliases
        let datasource_aliases: Vec<String> = self.tables_and_subqueries.iter()
            .filter_map(|table_or_subquery| match table_or_subquery {
                TableOrSubquery::RootDatasource { alias, .. }
                | TableOrSubquery::DerivativeDatasource { alias, .. } => Some(alias.clone()),
                _ => None 
            })
            .collect();

        // Compile ORDER BY expression
        let orderby_expression: String = if self.order_by_indices.len() > 0 {
            self.order_by_indices.into_iter()
                .filter_map(|(e, sort_ascending)| match &self.columns[e] {
                    QueryBuilderColumn::Primitive { value_expr, .. }
                    | QueryBuilderColumn::Formula { value_expr, .. } => Some((value_expr.clone(), sort_ascending)),
                    QueryBuilderColumn::File { file_expr, .. } => Some((format!("LENGTH({file_expr})"), sort_ascending)),
                    QueryBuilderColumn::Object { label_expr, .. }
                    | QueryBuilderColumn::Select { label_expr, .. }
                    | QueryBuilderColumn::Multiselect { label_expr, .. } => Some((label_expr.clone(), sort_ascending)),
                    QueryBuilderColumn::Subreport { .. } => None
                })
                .fold(
                    String::from("ORDER BY"),
                    |acc, (value_expr, sort_ascending)| {
                        if acc == "ORDER BY" { 
                            format!("ORDER BY {value_expr} {} NULLS LAST", if sort_ascending { "ASC" } else { "DESC" }) 
                        } else { 
                            format!("{acc}, {value_expr} {} NULLS LAST", if sort_ascending { "ASC" } else { "DESC" }) 
                        }
                    }
                )
        } else {
            String::from("")
        };

        Ok(Some((
            format!(
                "SELECT ROW_NUMBER() OVER ({orderby_expression}) AS ROW_INDEX, {} FROM {} {} {} {} ORDER BY 1",

                // Column expressions
                {
                    let compiled_cols = self.columns.iter()
                        .filter_map(|col| match col {
                            QueryBuilderColumn::Primitive { label_ord, label_expr, .. } => 
                                Some(format!("{label_expr} AS {label_ord}")),
                            QueryBuilderColumn::File { label_ord, label_expr, file_ord, file_expr, .. } =>
                                Some(format!("{label_expr} AS {label_ord}, {file_expr} AS {file_ord}")),
                            QueryBuilderColumn::Object { label_ord, label_expr, object_query_string_ord, object_query_string_expr, .. } =>
                                Some(format!("{label_expr} AS {label_ord}, {object_query_string_expr} AS {object_query_string_ord}")),
                            QueryBuilderColumn::Select { label_ord, label_expr, select_row_ord, select_row_expr, .. } 
                            | QueryBuilderColumn::Multiselect { label_ord, label_expr, select_row_ord, select_row_expr, .. } =>
                                Some(format!("{label_expr} AS {label_ord}, {select_row_expr} AS {select_row_ord}")),
                            QueryBuilderColumn::Formula { param_ord, param_expr, label_ord, label_expr, value_ord, value_expr, .. } => 
                                Some(format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}, {param_expr} AS {param_ord}")),
                            QueryBuilderColumn::Subreport { .. } => None
                        });
                    match self.tables_and_subqueries.iter()
                        .filter_map(|table_or_subquery| match table_or_subquery {
                            TableOrSubquery::RootDatasource { alias, .. }
                            | TableOrSubquery::DerivativeDatasource { alias, .. } => Some(format!("{alias}.OID AS {alias}_OID")),
                            _ => None 
                        })
                        .reduce(|acc, e| format!("{acc}, {e}")) {
                        
                        Some(oid_columns) => compiled_cols.fold(oid_columns, |acc, e| format!("{acc}, {e}")),
                        None => compiled_cols.reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(String::from("NULL AS COLUMN1"))
                    }
                },

                // Table and subquery expressions
                {
                    let mut compiled_tables_and_subqueries: String = match self.tables_and_subqueries.pop_front() {
                        Some(TableOrSubquery::RootDatasource { datasource, alias }) => format!("TABLE{} {alias}", datasource.get_schema_oid()?),
                        Some(TableOrSubquery::DerivativeDatasource { datasource, alias, on_clause }) => {
                            self.pregroup_filters.push(on_clause);
                            format!("TABLE{} {alias}", datasource.get_schema_oid()?)
                        }
                        Some(TableOrSubquery::Subquery { subquery, alias, on_clause }) => {
                            self.pregroup_filters.push(on_clause);
                            format!("({subquery}) {alias}")
                        }
                        Some(TableOrSubquery::Array { values, alias }) => format!("VALUES({}) {alias}", todo!("Arrays are not implemented yet!")),
                        None => { return Ok(None); }
                    };
                    for table_or_subquery in self.tables_and_subqueries.into_iter() {
                        compiled_tables_and_subqueries = format!("{compiled_tables_and_subqueries} {}",
                            match table_or_subquery {
                                TableOrSubquery::RootDatasource { datasource, alias } => format!("INNER JOIN TABLE{} {alias}", datasource.get_schema_oid()?),
                                TableOrSubquery::DerivativeDatasource { datasource, alias, on_clause } => format!("LEFT JOIN TABLE{} {alias} ON {on_clause}", datasource.get_schema_oid()?),
                                TableOrSubquery::Subquery { subquery, alias, on_clause } => format!("LEFT JOIN ({subquery}) {alias} ON {on_clause}"),
                                TableOrSubquery::Array { values, alias } => format!("INNER JOIN VALUES({}) {alias}", todo!("Arrays are not yet implemented!"))
                            }
                        )
                    }
                    compiled_tables_and_subqueries
                },

                // WHERE expression
                if self.pregroup_filters.len() > 0 {
                    format!("WHERE {}",
                        self.pregroup_filters.into_iter().reduce(|acc, e| format!("{acc} AND {e}")).unwrap_or(String::from("TRUE"))
                    )
                } else {
                    String::from("")
                },

                // GROUP BY expression
                if self.group_by_indices.len() > 0 {
                    format!("GROUP BY {}",
                        {
                            let mut group_expr: Vec<String> = Vec::new();
                            for i in self.group_by_indices {
                                group_expr.push(match &self.columns[i] {
                                    QueryBuilderColumn::Primitive { value_expr, .. }
                                    | QueryBuilderColumn::File { file_expr: value_expr, .. }
                                    | QueryBuilderColumn::Object { object_query_string_expr: value_expr, .. }
                                    | QueryBuilderColumn::Select { select_row_expr: value_expr, .. }
                                    | QueryBuilderColumn::Multiselect { select_row_expr: value_expr, .. } => value_expr.clone(),
                                    QueryBuilderColumn::Formula { value_expr, deterministic, .. } => {
                                        if *deterministic {
                                            value_expr.clone()
                                        } else {
                                            return Err(Error::AdhocError("You cannot group a report by a nondeterministic column!"));
                                        }
                                    }
                                    QueryBuilderColumn::Subreport { .. } => {
                                        return Err(Error::AdhocError("You cannot group a report by a subreport column!"));
                                    }
                                });
                            }
                            group_expr.into_iter().reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                        }
                    )
                } else {
                    String::from("")
                },

                // HAVING expression
                if self.postgroup_filters.len() > 0 {
                    format!("HAVING {}",
                        self.postgroup_filters.into_iter().reduce(|acc, e| format!("{acc} AND {e}")).unwrap_or(String::from("TRUE"))
                    )
                } else {
                    String::from("")
                }
            ),
            self.columns,
            datasource_aliases
        )))
    }

    /// Compiles the datasources of the query into SQL.
    /// This compilation excludes the columns, but includes FROM, JOIN, WHERE, GROUP BY, and ORDER BY clauses.
    pub fn compile_datasources(mut self) -> Result<Option<(String, Vec<String>)>, Error> {
        // Compile list of datasource aliases
        let datasource_aliases: Vec<String> = self.tables_and_subqueries.iter()
            .filter_map(|table_or_subquery| match table_or_subquery {
                TableOrSubquery::RootDatasource { alias, .. }
                | TableOrSubquery::DerivativeDatasource { alias, .. } => Some(alias.clone()),
                _ => None 
            })
            .collect();

        // Compile ORDER BY expression
        let orderby_expression: String = if self.order_by_indices.len() > 0 {
            self.order_by_indices.into_iter()
                .filter_map(|(e, sort_ascending)| match &self.columns[e] {
                    QueryBuilderColumn::Primitive { value_expr, .. }
                    | QueryBuilderColumn::Formula { value_expr, .. } => Some((value_expr.clone(), sort_ascending)),
                    QueryBuilderColumn::File { file_expr, .. } => Some((format!("LENGTH({file_expr})"), sort_ascending)),
                    QueryBuilderColumn::Object { label_expr, .. }
                    | QueryBuilderColumn::Select { label_expr, .. }
                    | QueryBuilderColumn::Multiselect { label_expr, .. } => Some((label_expr.clone(), sort_ascending)),
                    QueryBuilderColumn::Subreport { .. } => None
                })
                .fold(
                    String::from("ORDER BY"),
                    |acc, (value_expr, sort_ascending)| {
                        if acc == "ORDER BY" { 
                            format!("ORDER BY {value_expr} {} NULLS LAST", if sort_ascending { "ASC" } else { "DESC" }) 
                        } else { 
                            format!("{acc}, {value_expr} {} NULLS LAST", if sort_ascending { "ASC" } else { "DESC" }) 
                        }
                    }
                )
        } else {
            String::from("")
        };

        Ok(Some((
            format!(
                ", ROW_NUMBER() OVER ({orderby_expression}) AS ROW_INDEX FROM {} {} {} {}",

                // Table and subquery expressions
                {
                    let mut compiled_tables_and_subqueries: String = match self.tables_and_subqueries.pop_front() {
                        Some(TableOrSubquery::RootDatasource { datasource, alias }) => format!("TABLE{} {alias}", datasource.get_schema_oid()?),
                        Some(TableOrSubquery::DerivativeDatasource { datasource, alias, on_clause }) => {
                            self.pregroup_filters.push(on_clause);
                            format!("TABLE{} {alias}", datasource.get_schema_oid()?)
                        }
                        Some(TableOrSubquery::Subquery { subquery, alias, on_clause }) => {
                            self.pregroup_filters.push(on_clause);
                            format!("({subquery}) {alias}")
                        }
                        Some(TableOrSubquery::Array { values, alias }) => format!("VALUES({}) {alias}", todo!("Arrays are not implemented yet!")),
                        None => { return Ok(None); }
                    };
                    for table_or_subquery in self.tables_and_subqueries.into_iter() {
                        compiled_tables_and_subqueries = format!("{compiled_tables_and_subqueries} {}",
                            match table_or_subquery {
                                TableOrSubquery::RootDatasource { datasource, alias } => format!("INNER JOIN TABLE{} {alias}", datasource.get_schema_oid()?),
                                TableOrSubquery::DerivativeDatasource { datasource, alias, on_clause } => format!("LEFT JOIN TABLE{} {alias} ON {on_clause}", datasource.get_schema_oid()?),
                                TableOrSubquery::Subquery { subquery, alias, on_clause } => format!("LEFT JOIN ({subquery}) {alias} ON {on_clause}"),
                                TableOrSubquery::Array { values, alias } => format!("INNER JOIN VALUES({}) {alias}", todo!("Arrays are not yet implemented!"))
                            }
                        )
                    }
                    compiled_tables_and_subqueries
                },

                // WHERE expression
                if self.pregroup_filters.len() > 0 {
                    format!("WHERE {}",
                        self.pregroup_filters.into_iter().reduce(|acc, e| format!("{acc} AND {e}")).unwrap_or(String::from("TRUE"))
                    )
                } else {
                    String::from("")
                },

                // GROUP BY expression
                if self.group_by_indices.len() > 0 {
                    format!("GROUP BY {}",
                        {
                            let mut group_expr: Vec<String> = Vec::new();
                            for i in self.group_by_indices {
                                group_expr.push(match &self.columns[i] {
                                    QueryBuilderColumn::Primitive { value_expr, .. }
                                    | QueryBuilderColumn::File { file_expr: value_expr, .. }
                                    | QueryBuilderColumn::Object { object_query_string_expr: value_expr, .. }
                                    | QueryBuilderColumn::Select { select_row_expr: value_expr, .. }
                                    | QueryBuilderColumn::Multiselect { select_row_expr: value_expr, .. } => value_expr.clone(),
                                    QueryBuilderColumn::Formula { value_expr, deterministic, .. } => {
                                        if *deterministic {
                                            value_expr.clone()
                                        } else {
                                            return Err(Error::AdhocError("You cannot group a report by a nondeterministic column!"));
                                        }
                                    }
                                    QueryBuilderColumn::Subreport { .. } => {
                                        return Err(Error::AdhocError("You cannot group a report by a subreport column!"));
                                    }
                                });
                            }
                            group_expr.into_iter().reduce(|acc, e| format!("{acc}, {e}")).unwrap()
                        }
                    )
                } else {
                    String::from("")
                },

                // HAVING expression
                if self.postgroup_filters.len() > 0 {
                    format!("HAVING {}",
                        self.postgroup_filters.into_iter().reduce(|acc, e| format!("{acc} AND {e}")).unwrap_or(String::from("TRUE"))
                    )
                } else {
                    String::from("")
                }
            ),
            datasource_aliases
        )))
    }

    /// Checks if the query already has a table or subquery with the given alias.
    pub fn has_table_or_subquery_alias(&self, alias: &String) -> bool {
        if self.tables_and_subqueries.iter().any(|table_or_subquery| <TableOrSubquery as Borrow<String>>::borrow(table_or_subquery) == alias) {
            true
        } else if let Some(parent_query) = &self.parent_query {
            parent_query.has_table_or_subquery_alias(alias)
        } else {
            false
        }
    }

    /// Inserts the datasource into the query.
    /// Returns the alias of the datasource.
    fn insert_datasource(&mut self, datasource: Datasource) -> Result<String, Error> {
        let alias: String = datasource.get_alias();
        let schema_oid: i64 = datasource.get_schema_oid()?;

        // Check to make sure not double-inserting datasource
        if self.has_table_or_subquery_alias(&alias) {
            // Return the alias of the datasource
            return Ok(alias);
        } else if let Some(parent_query) = &mut self.parent_query {
            // Test if the parent query is already pulling from a datasource that has a 1-to-1 relationship with this datasource
            let deep_parent: Datasource = datasource.seek_basis()?;
            let deep_alias: String = deep_parent.get_alias();
            if parent_query.has_table_or_subquery_alias(&deep_alias) {
                // If it does, then insert the datasource into the parent instead
                return parent_query.insert_datasource(datasource);
            }
        }

        // Branch based on datasource type
        let table_or_subquery = match &datasource {
            Datasource::Table { .. } => TableOrSubquery::RootDatasource { datasource, alias },
            Datasource::MasterTable { parent_datasource, .. } => {
                let parent_alias: String = self.insert_datasource(*parent_datasource.clone())?;
                TableOrSubquery::DerivativeDatasource { 
                    on_clause: format!(
                        "{alias}.OID = {parent_alias}.MASTER{schema_oid}_OID"
                    ),
                    datasource, 
                    alias
                }
            },
            Datasource::InheritorTable { parent_datasource, .. } => {
                let parent_schema_oid: i64 = parent_datasource.get_schema_oid()?;
                let parent_alias: String = self.insert_datasource(*parent_datasource.clone())?;
                TableOrSubquery::DerivativeDatasource { 
                    on_clause: format!(
                        "{alias}.MASTER{parent_schema_oid}_OID = {parent_alias}.OID"
                    ),
                    datasource, 
                    alias
                }
            },
            Datasource::Column { parent_datasource, column } => {
                match &column.column_type {
                    column_type::ColumnType::Object { .. } 
                    | column_type::ColumnType::Select { .. } => {
                        let parent_schema_oid: i64 = parent_datasource.get_schema_oid()?;
                        let parent_alias: String = self.insert_datasource(*parent_datasource.clone())?;
                        TableOrSubquery::DerivativeDatasource { 
                            on_clause: if parent_schema_oid == column.schema.oid {
                                format!(
                                    "{alias}.OID = {parent_alias}.COLUMN{}",
                                    column.oid
                                )
                            } else {
                                format!(
                                    "{alias}.COLUMN{} = {parent_alias}.OID",
                                    column.oid
                                )
                            },
                            datasource, 
                            alias
                        }
                    }
                    column_type::ColumnType::Multiselect { .. } => {
                        let parent_schema_oid: i64 = parent_datasource.get_schema_oid()?;
                        let parent_alias: String = self.insert_datasource(*parent_datasource.clone())?;
                        TableOrSubquery::DerivativeDatasource { 
                            on_clause: format!(
                                "{alias}.OID IN (SELECT TABLE{schema_oid}_OID FROM MULTISELECT{} WHERE TABLE{parent_schema_oid}_OID = {parent_alias}.OID)",
                                column.oid
                            ),
                            datasource, 
                            alias
                        }
                    }
                    _ => {
                        return Err(Error::AdhocError("Only columns of types Object, Select, and Multiselect can be used as links to a datasource."));
                    }
                }
            }
        };

        // Return the alias of the datasource
        let alias = {
            let alias_ref: &String = table_or_subquery.borrow();
            alias_ref.clone()    
        };
        self.tables_and_subqueries.push_back(table_or_subquery);
        return Ok(alias);
    }

    /// Inserts a column definition.
    pub fn insert_column(&mut self, column_datasource: Datasource, column_metadata: column::FullMetadata) -> Result<(), Error> {
        let column: QueryBuilderColumn = self.compile_column(column_datasource, column_metadata)?;
        self.columns.push(column);
        Ok(())
    }

    /// Compiles the SQL expressions for a column.
    pub fn compile_column(&mut self, column_datasource: Datasource, column_metadata: column::FullMetadata) -> Result<QueryBuilderColumn, Error> {
        println!("COLUMN{} type = {:?}", column_metadata.oid, column_metadata.column_type);
        Ok(match column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let datasource_alias: String = self.insert_datasource(column_datasource)?;
                let primitive_value_alias: String = format!(
                    "{}.COLUMN{}", 
                    datasource_alias,
                    column_metadata.oid
                );
                let label_ord: String = format!("LABEL{}", self.columns.len());
                match &prim {
                    column_type::Primitive::File 
                    | column_type::Primitive::Image => QueryBuilderColumn::File {
                        label_expr: format!("(SELECT LABEL FROM METADATA_FILE_VIEW WHERE OID = {primitive_value_alias})"),
                        file_expr: primitive_value_alias,
                        label_ord,
                        file_ord: format!("VALUE{}", self.columns.len()),
                        schema_oid: column_metadata.schema.oid,
                        schema_row_ord: format!("{datasource_alias}_OID"),
                        column_oid: column_metadata.oid
                    },
                    column_type::Primitive::Text
                    | column_type::Primitive::JSON => QueryBuilderColumn::Primitive { 
                        label_expr: format!("{primitive_value_alias}"),
                        value_expr: primitive_value_alias,
                        primitive_type: prim, 
                        label_ord,
                        schema_oid: column_metadata.schema.oid,
                        schema_row_ord: format!("{datasource_alias}_OID"),
                        column_oid: column_metadata.oid
                    },
                    column_type::Primitive::Integer
                    | column_type::Primitive::Number 
                    | column_type::Primitive::Checkbox => QueryBuilderColumn::Primitive { 
                        label_expr: format!("CAST({primitive_value_alias} AS TEXT)"),
                        value_expr: primitive_value_alias,
                        primitive_type: prim, 
                        label_ord,
                        schema_oid: column_metadata.schema.oid,
                        schema_row_ord: format!("{datasource_alias}_OID"),
                        column_oid: column_metadata.oid
                    },
                    column_type::Primitive::Date => QueryBuilderColumn::Primitive {
                        label_expr: format!("DATE({primitive_value_alias}, 'julianday')"),
                        value_expr: primitive_value_alias,
                        primitive_type: prim, 
                        label_ord,
                        schema_oid: column_metadata.schema.oid,
                        schema_row_ord: format!("{datasource_alias}_OID"),
                        column_oid: column_metadata.oid
                    },
                    column_type::Primitive::Datetime => QueryBuilderColumn::Primitive {
                        label_expr: format!("STRFTIME('%FT%TZ', {primitive_value_alias}, 'julianday')"),
                        value_expr: primitive_value_alias,
                        primitive_type: prim, 
                        label_ord,
                        schema_oid: column_metadata.schema.oid,
                        schema_row_ord: format!("{datasource_alias}_OID"),
                        column_oid: column_metadata.oid
                    }
                }
            }
            column_type::ColumnType::Object { table_oid, .. } => {
                let datasource_alias: String = self.insert_datasource(column_datasource)?;
                let primitive_value_alias: String = format!(
                    "{}.COLUMN{}", 
                    datasource_alias,
                    column_metadata.oid
                );
                let object_query_string_ord: String = format!("VALUE{}", self.columns.len());
                let label_ord: String = format!("LABEL{}", self.columns.len());
                QueryBuilderColumn::Object { 
                    label_expr: format!("(SELECT LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {primitive_value_alias})"),
                    json_expr: format!("(SELECT JSON_LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {primitive_value_alias})"),
                    object_query_string_expr: format!("'{datasource_alias}=' || FORMAT('%d', {primitive_value_alias})"),
                    label_ord, 
                    object_schema_oid: table_oid,
                    object_query_string_ord,
                    schema_oid: column_metadata.schema.oid,
                    schema_row_ord: format!("{datasource_alias}_OID"),
                    column_oid: column_metadata.oid
                }
            }
            column_type::ColumnType::Select { table_oid, .. } => {
                let datasource_alias: String = self.insert_datasource(column_datasource)?;
                let primitive_value_alias: String = format!(
                    "{}.COLUMN{}", 
                    datasource_alias,
                    column_metadata.oid
                );
                let select_row_ord: String = format!("VALUE{}", self.columns.len());
                let label_ord: String = format!("LABEL{}", self.columns.len());
                QueryBuilderColumn::Select { 
                    label_expr: format!("(SELECT LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {primitive_value_alias})"),
                    json_expr: format!("(SELECT JSON_LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {primitive_value_alias})"),
                    select_row_expr: primitive_value_alias,
                    label_ord, 
                    select_schema_oid: table_oid,
                    select_row_ord,
                    schema_oid: column_metadata.schema.oid,
                    schema_row_ord: format!("{datasource_alias}_OID"),
                    column_oid: column_metadata.oid
                }
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                // Check if the normal direction of the Multiselect column needs to be inverted
                // This will be true if the datasource is the schema that the Multiselect column normally points to
                let inverted: bool = column_datasource.get_schema_oid()? != column_metadata.schema.oid;

                // Construct the SQL expression
                let datasource_alias: String = self.insert_datasource(column_datasource)?;
                let primitive_value_alias: String = format!(
                    "{}.OID", 
                    datasource_alias
                );
                let select_row_ord: String = format!("VALUE{}", self.columns.len());
                let label_ord: String = format!("LABEL{}", self.columns.len());
                QueryBuilderColumn::Multiselect { 
                    label_expr: if inverted {
                        format!("(
                                SELECT '[' || GROUP_CONCAT(a.JSON_LABEL) || ']' 
                                FROM MULTISELECT{} m
                                INNER JOIN TABLE{}_SURROGATE a ON m.TABLE{}_OID = a.OID
                                WHERE m.TABLE{table_oid}_OID = {primitive_value_alias}
                            )",
                            column_metadata.oid,
                            column_metadata.schema.oid,
                            column_metadata.schema.oid
                        )
                    } else {
                        format!("(
                                SELECT '[' || GROUP_CONCAT(a.JSON_LABEL) || ']' 
                                FROM MULTISELECT{} m 
                                INNER JOIN TABLE{table_oid}_SURROGATE a ON m.TABLE{table_oid}_OID = a.OID
                                WHERE m.TABLE{}_OID = {primitive_value_alias}
                            )",
                            column_metadata.oid,
                            column_metadata.schema.oid
                        )
                    },
                    select_row_expr: if inverted {
                        format!("(
                                SELECT GROUP_CONCAT(CAST(TABLE{}_OID AS TEXT)) 
                                FROM MULTISELECT{} 
                                WHERE TABLE{table_oid}_OID = {primitive_value_alias}
                            )",
                            column_metadata.schema.oid,
                            column_metadata.oid
                        )
                    } else {
                        format!("(
                                SELECT GROUP_CONCAT(CAST(TABLE{table_oid}_OID AS TEXT)) 
                                FROM MULTISELECT{} 
                                WHERE TABLE{}_OID = {primitive_value_alias}
                            )",
                            column_metadata.oid,
                            column_metadata.schema.oid
                        )
                    },
                    label_ord, 
                    select_schema_oid: if inverted { column_metadata.schema.oid } else { table_oid },
                    select_row_ord,
                    schema_oid: if inverted { table_oid } else { column_metadata.schema.oid },
                    schema_row_ord: format!("{datasource_alias}_OID"),
                    column_oid: column_metadata.oid
                }
            }
            column_type::ColumnType::Formula { formula, .. } => {
                let datasource_alias: String = self.insert_datasource(column_datasource)?;

                // Construct the ordinals for this column
                let value_ord: String = format!("VALUE{}", self.columns.len());
                let label_ord: String = format!("LABEL{}", self.columns.len());
                let param_ord: String = format!("PARAM{}", self.columns.len());

                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula)?);
                // Compile the formula into a scalar SQL value
                let scalar_sql: ScalarExpression = self.compile_scalar_formula(parsed_formula)?;
                // Construct column
                QueryBuilderColumn::Formula { 
                    schema_oid: column_metadata.schema.oid, 
                    schema_row_ord: format!("{datasource_alias}_OID"),
                    column_oid: column_metadata.oid, 
                    param_ord, 
                    param_expr: scalar_sql.param_expr, 
                    label_ord, 
                    label_expr: scalar_sql.label_expr, 
                    value_ord, 
                    value_expr: scalar_sql.value_expr, 
                    arg_expr: scalar_sql.arg_expr,
                    arg_return_type: scalar_sql.arg_return_type,
                    deterministic: scalar_sql.deterministic
                }
            }
            column_type::ColumnType::Subreport { report_oid, .. } => {
                let datasource_alias: String = self.insert_datasource(column_datasource)?;
                let subreport_metadata: report::FullMetadata = report::FullMetadata::get(report_oid)?;
                QueryBuilderColumn::Subreport { 
                    schema_oid: column_metadata.schema.oid, 
                    schema_row_ord: format!("{datasource_alias}_OID"),
                    column_oid: column_metadata.oid, 
                    subreport_metadata
                }
            }
        })
    }

    /// Applies a filter to the rows returned by the query.
    pub fn insert_filter(&mut self, formula: String) -> Result<(), Error> {
        // Parse the formula for the filter
        let parsed_formula: Formula = Formula::parse(formula)?;
        let parsed_formula_name: String = parsed_formula.to_string();
        // Compile the formula into SQL
        let compiled_formula: ScalarExpression = self.compile_scalar_formula(Box::new(parsed_formula))?;
        
        // Confirm that the compiled formula is a boolean
        if !ScalarType::Boolean.contains(compiled_formula.arg_return_type.clone()) {
            return Err(Error::FormulaTypeValidationError { 
                outer_name: "REPORT FILTER", 
                inner_name: parsed_formula_name, 
                expected_type: ScalarType::Boolean.to_string(), 
                received_type: compiled_formula.arg_return_type.to_string()
            });
        }

        // Apply the filter after GROUP BY
        self.postgroup_filters.push(compiled_formula.arg_expr);
        Ok(())
    }

    /// Applies a filter to return a specific row from a datasource.
    pub fn insert_row_filter(&mut self, table_or_subquery_alias: String, table_or_subquery_row_oid: i64) {
        if self.has_table_or_subquery_alias(&table_or_subquery_alias) {
            self.pregroup_filters.push(format!("{table_or_subquery_alias}.OID = {table_or_subquery_row_oid}"));
        }
    }

    /// Groups the rows returned by the query based on a column of the query.
    pub fn insert_grouping(&mut self, column_oid: i64) -> Result<(), Error> {
        if let Some(idx) = self.columns.iter().position(|col| match col {
            QueryBuilderColumn::Primitive { column_oid: c, .. }
            | QueryBuilderColumn::File { column_oid: c, .. }
            | QueryBuilderColumn::Object { column_oid: c, .. }
            | QueryBuilderColumn::Select { column_oid: c, .. }
            | QueryBuilderColumn::Multiselect { column_oid: c, .. }
            | QueryBuilderColumn::Formula { column_oid: c, .. }
            | QueryBuilderColumn::Subreport { column_oid: c, .. } => column_oid == *c
        }) {
            self.group_by_indices.push(idx);
            return Ok(());
        } else {
            return Err(Error::AdhocError("The report is grouped by a column that does not exist in the report!"));
        }
    }

    /// Groups the rows returned by the query based on a column of the query.
    pub fn insert_ordering(&mut self, column_oid: i64, sort_ascending: bool) -> Result<(), Error> {
        if let Some(idx) = self.columns.iter().position(|col| match col {
            QueryBuilderColumn::Primitive { column_oid: c, .. }
            | QueryBuilderColumn::File { column_oid: c, .. }
            | QueryBuilderColumn::Object { column_oid: c, .. }
            | QueryBuilderColumn::Select { column_oid: c, .. }
            | QueryBuilderColumn::Multiselect { column_oid: c, .. }
            | QueryBuilderColumn::Formula { column_oid: c, .. }
            | QueryBuilderColumn::Subreport { column_oid: c, .. } => column_oid == *c
        }) {
            self.order_by_indices.push((idx, sort_ascending));
            return Ok(());
        } else {
            return Err(Error::AdhocError("The report is sorted by a column that does not exist in the report!"));
        }
    }



    fn compile_scalar_formula(&mut self, formula: Box<Formula>) -> Result<ScalarExpression, Error> {
        Ok(match *formula {
            Formula::Null => ScalarExpression {
                arg_expr: String::from("NULL"),
                arg_return_type: ScalarType::Null,
                value_expr: String::from("NULL"),
                label_expr: String::from("NULL"),
                param_expr: String::from("NULL"),
                deterministic: true
            },
            Formula::LiteralBool(b) => {
                let (value_expr, label_expr) = if b {
                    (String::from("TRUE"), String::from("'True'"))
                } else {
                    (String::from("FALSE"), String::from("'False'"))
                };
                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: ScalarType::Boolean,
                    value_expr,
                    label_expr,
                    param_expr: String::from("NULL"),
                    deterministic: true
                }
            }
            Formula::LiteralInt(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_return_type: ScalarType::Integer,
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("NULL"),
                deterministic: true
            },
            Formula::LiteralFloat(num) => ScalarExpression {
                arg_expr: format!("{num}"),
                arg_return_type: ScalarType::Number,
                value_expr: format!("{num}"),
                label_expr: format!("'{num}'"),
                param_expr: String::from("NULL"),
                deterministic: true
            },
            Formula::LiteralString(str) => {
                let safe_str: String = format!("'{}'", str.replace("'", "''"));
                ScalarExpression {
                    arg_expr: safe_str.clone(),
                    arg_return_type: ScalarType::JSON,
                    value_expr: safe_str.clone(),
                    label_expr: safe_str.clone(),
                    param_expr: String::from("NULL"),
                    deterministic: true
                }
            }
            Formula::RandomInt => ScalarExpression {
                arg_expr: format!("RANDOM()"),
                arg_return_type: ScalarType::Integer,
                value_expr: format!("RANDOM()"),
                label_expr: format!("CAST(RANDOM() AS TEXT)"),
                param_expr: String::from("NULL"),
                deterministic: false
            },
            Formula::Param { datasource_path, column_oid } => {
                let column_datasource: Datasource = Datasource::from_path(datasource_path.clone())?;
                let column_metadata = column::FullMetadata::get(column_oid.clone())?;
                let param_expr: String = format!("'{}:{column_oid}'", column_datasource.get_alias());
                match self.compile_column(column_datasource, column_metadata)? {
                    QueryBuilderColumn::Primitive { primitive_type, label_expr, value_expr, .. } => ScalarExpression { 
                        arg_expr: value_expr.clone(), 
                        arg_return_type: match primitive_type {
                            column_type::Primitive::Text => ScalarType::Text,
                            column_type::Primitive::JSON => ScalarType::JSON,
                            column_type::Primitive::Integer => ScalarType::Integer,
                            column_type::Primitive::Number => ScalarType::Number,
                            column_type::Primitive::Checkbox => ScalarType::Boolean,
                            column_type::Primitive::Date => ScalarType::Date,
                            column_type::Primitive::Datetime => ScalarType::Datetime,
                            column_type::Primitive::File 
                            | column_type::Primitive::Image => ScalarType::Blob
                        }, 
                        value_expr, 
                        label_expr, 
                        param_expr,
                        deterministic: false
                    },
                    QueryBuilderColumn::File { label_expr, file_expr, .. } => ScalarExpression {
                        arg_expr: file_expr.clone(), 
                        arg_return_type: ScalarType::Blob, 
                        value_expr: file_expr, 
                        label_expr, 
                        param_expr,
                        deterministic: false
                    },
                    QueryBuilderColumn::Object { label_expr, json_expr, object_query_string_expr: object_datasource_row_expr, .. } => ScalarExpression {
                        arg_expr: json_expr,
                        arg_return_type: ScalarType::JSON,
                        value_expr: object_datasource_row_expr,
                        label_expr,
                        param_expr,
                        deterministic: false
                    },
                    QueryBuilderColumn::Select { label_expr, json_expr, select_row_expr, .. } => ScalarExpression {
                        arg_expr: json_expr,
                        arg_return_type: ScalarType::JSON,
                        value_expr: select_row_expr,
                        label_expr,
                        param_expr,
                        deterministic: false
                    },
                    QueryBuilderColumn::Multiselect { label_expr, select_row_expr, .. } => ScalarExpression {
                        arg_expr: label_expr.clone(),
                        arg_return_type: ScalarType::JSON,
                        value_expr: select_row_expr,
                        label_expr,
                        param_expr,
                        deterministic: false
                    },
                    QueryBuilderColumn::Formula { param_expr, label_expr, value_expr, arg_expr, arg_return_type, deterministic, .. } => ScalarExpression {
                        arg_expr,
                        arg_return_type,
                        value_expr,
                        label_expr,
                        param_expr,
                        deterministic
                    },
                    QueryBuilderColumn::Subreport { .. } => {
                        return Err(Error::AdhocError("A subreport cannot be a parameter to a formula!"));
                    }
                }
            }
            Formula::Coalesce(items) => {
                let mut items_compiled: Vec<ScalarExpression> = Vec::new();
                for item in items {
                    let item_compiled: ScalarExpression = self.compile_scalar_formula(Box::new(item))?;
                    items_compiled.push(item_compiled);
                }

                let deterministic: bool = items_compiled.iter().all(|item_compiled| item_compiled.deterministic);
                let arg_return_type: ScalarType = items_compiled.iter().fold(ScalarType::Null, |acc, item_compiled| acc | item_compiled.arg_return_type.clone());
                let (label_expr, param_expr) = if items_compiled.len() > 1 {
                    (
                        format!("{} ELSE {} END",
                            items_compiled.iter()
                                .take(items_compiled.len() - 1)
                                .fold(String::from("CASE"), |acc, item_compiled| format!("{acc} WHEN {} IS NOT NULL THEN {}", item_compiled.value_expr, item_compiled.label_expr)),
                            items_compiled[items_compiled.len() - 1].label_expr
                        ),
                        format!("{} ELSE {} END",
                            items_compiled.iter()
                                .take(items_compiled.len() - 1)
                                .fold(String::from("CASE"), |acc, item_compiled| format!("{acc} WHEN {} IS NOT NULL THEN {}", item_compiled.value_expr, item_compiled.param_expr)),
                            items_compiled[items_compiled.len() - 1].param_expr
                        )
                    )
                } else if items_compiled.len() == 1 {
                    (items_compiled[0].label_expr.clone(), items_compiled[0].param_expr.clone())
                } else {
                    (String::from("NULL"), String::from("NULL"))
                };
                let (value_expr, arg_expr) = match items_compiled.into_iter()
                    .map(|item_compiled| (item_compiled.value_expr, item_compiled.arg_expr))
                    .reduce(|(acc_value, acc_arg), (e_value, e_arg)| (format!("{acc_value}, {e_value}"), format!("{acc_arg}, {e_arg}"))) {
                    
                    Some((acc_value, acc_arg)) => (format!("COALESCE({acc_value})"), format!("COALESCE({acc_arg})")),
                    None => (String::from("NULL"), String::from("NULL"))
                };

                ScalarExpression {
                    arg_expr,
                    arg_return_type,
                    value_expr,
                    label_expr,
                    param_expr,
                    deterministic
                }
            }
            Formula::Abs(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression = self.compile_scalar_formula(inner)?;
                if !ScalarType::Number.contains(inner_compiled.arg_return_type.clone()) {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "abs", 
                        inner_name, 
                        expected_type: ScalarType::Number.to_string(), 
                        received_type: inner_compiled.arg_return_type.to_string() 
                    });
                }

                let value_expr: String = format!("ABS({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: inner_compiled.arg_return_type,
                    label_expr,
                    value_expr,
                    param_expr: String::from("NULL"),
                    deterministic: inner_compiled.deterministic
                }
            }
            Formula::Sign(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression = self.compile_scalar_formula(inner)?;
                if !ScalarType::Number.contains(inner_compiled.arg_return_type.clone()) {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "sign", 
                        inner_name, 
                        expected_type: ScalarType::Number.to_string(), 
                        received_type: inner_compiled.arg_return_type.to_string() 
                    });
                }

                let value_expr: String = format!("SIGN({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: ScalarType::Integer,
                    label_expr,
                    value_expr,
                    param_expr: String::from("NULL"),
                    deterministic: inner_compiled.deterministic
                }
            }
            Formula::Floor(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression = self.compile_scalar_formula(inner)?;
                if !ScalarType::Number.contains(inner_compiled.arg_return_type.clone()) {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "floor", 
                        inner_name, 
                        expected_type: ScalarType::Number.to_string(), 
                        received_type: inner_compiled.arg_return_type.to_string() 
                    });
                }

                let value_expr: String = format!("FLOOR({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: ScalarType::Integer,
                    label_expr,
                    value_expr,
                    param_expr: String::from("NULL"),
                    deterministic: inner_compiled.deterministic
                }
            }
            Formula::Ceiling(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression = self.compile_scalar_formula(inner)?;
                if !ScalarType::Number.contains(inner_compiled.arg_return_type.clone()) {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "ceil", 
                        inner_name, 
                        expected_type: ScalarType::Number.to_string(), 
                        received_type: inner_compiled.arg_return_type.to_string() 
                    });
                }

                let value_expr: String = format!("CEILING({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: ScalarType::Integer,
                    label_expr,
                    value_expr,
                    param_expr: String::from("NULL"),
                    deterministic: inner_compiled.deterministic
                }
            }
            Formula::Round(inner) => {
                let inner_name: String = inner.to_string();
                let inner_compiled: ScalarExpression = self.compile_scalar_formula(inner)?;
                if !ScalarType::Number.contains(inner_compiled.arg_return_type.clone()) {
                    return Err(Error::FormulaTypeValidationError { 
                        outer_name: "round", 
                        inner_name, 
                        expected_type: ScalarType::Number.to_string(), 
                        received_type: inner_compiled.arg_return_type.to_string() 
                    });
                }

                let value_expr: String = format!("ROUND({})", inner_compiled.arg_expr);
                let label_expr: String = format!("CAST({value_expr} AS TEXT)");

                ScalarExpression {
                    arg_expr: value_expr.clone(),
                    arg_return_type: ScalarType::Integer,
                    label_expr,
                    value_expr,
                    param_expr: String::from("NULL"),
                    deterministic: inner_compiled.deterministic
                }
            },
            _ => {
                todo!("Function {} is not implemented yet!", formula.to_string());
            }
        })
    }
}
