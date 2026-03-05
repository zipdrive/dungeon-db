use rusqlite::{Connection, Transaction, params};
use crate::data::{column, column_type, datasource, parameter, schema, table};
use crate::util::formula::Formula;
use crate::util::db;
use crate::util::error::Error;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::collections::{HashSet,HashMap};
use bitflags::bitflags;



bitflags! {
    struct ScalarType: u32 {
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
        let flags = self.iter().collect();
        // Reduce flags to minimal set
        let mut k: usize = 0;
        while k < flags.len() {
            // Iterate over each other flag, testing if this flag is contained in the other
            let mut j: usize = 0;
            while j < flags.len() {
                if j != k && flags[j].contains(flags[k]) {
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
        flags.iter().map(|flag| match flag {
            Self::Null => String::from("null"),
            Self::Any => String::from("any"),
            Self::Boolean => String::from("boolean"),
            Self::Integer => String::from("integer"),
            Self::Number => String::from("number"),
            Self::Date => String::from("date"),
            Self::Datetime => String::from("timestamp"),
            Self::Text => String::from("text"),
            Self::JSON => String::from("JSON"),
            Self::Blob => String::from("file")
        }).reduce(|acc, e| format!("{acc} | {e}")).unwrap_or(String::from("null"))
    }
}

/// Represents an expression returning a scalar value.
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The SQL expression resulting in a scalar value representing the true value of the parameter.
    /// This will typically be the same as arg_expr, with the exception that Select/Multiselect/Object columns will have their primary keys 
    /// returned by arg_expr and their referenced row OIDs returned by value_expr.
    value_expr: String,

    /// The SQL expression for the label of that scalar value (e.g. primary key of the row referenced by a Select column).
    label_expr: String,

    /// The SQL expression for the parameter returned by the expression, if it returns the value of an unmodified parameter.
    param_expr: String,

    /// The scalar type returned by the expression.
    return_type: ScalarType
}



#[derive(PartialEq, Eq)]
enum Join {
    Root(datasource::Datasource),
    Precompiled {
        datasource: datasource::Datasource,
        join_clause: String
    }
}

impl Hash for Join {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Root(datasource)
            | Self::Precompiled { datasource, .. } => {
                datasource.hash(state)
            }
        }
    }
}

impl Borrow<datasource::Datasource> for Join {
    fn borrow(&self) -> &datasource::Datasource {
        match self {
            Self::Root(datasource)
            | Self::Precompiled { datasource, .. } => {
                datasource
            }
        }
    }
}



trait QueryBuilder {
    /// Wraps the QueryBuilder in a FormulaWrapper.
    fn wrap<'a>(&'a self) -> FormulaWrapper<'a>;

    /// Consumes the QueryBuilder and returns an SQL statement.
    fn compile(mut self) -> Self;

    /// Ensure that a datasource is being queried.
    fn insert_datasource(&mut self, datasource: &datasource::Datasource);

    /// Checks if the query queries from a datasource.
    fn contains_datasource(&self, datasource: &datasource::Datasource) -> bool;

    /// Retrieves the alias associated with a datasource.
    /// If the datasource isn't already being queried, ensures that it will be.
    fn get_datasource_alias(&mut self, datasource: &datasource::Datasource) -> String;

    fn get_datasource_row_alias(&mut self, datasource: &datasource::Datasource) -> String;

    fn get_parameter_alias(&mut self, param: &parameter::Parameter) -> String;

    fn compile_parameter(&self, param: parameter::Parameter) -> Result<Option<ScalarExpression>, Error> {
        let param_alias: String = self.get_parameter_alias(&param.datasource);
        Ok(match param.column.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let (label_expr, return_type) = match prim {
                    column_type::Primitive::Text => (
                        param_alias.clone(),
                        ScalarType::Text
                    ),
                    column_type::Primitive::JSON => (
                        param_alias.clone(),
                        ScalarType::JSON
                    ),
                    column_type::Primitive::Integer => (
                        format!("CAST({param_alias} AS TEXT)"),
                        ScalarType::Integer 
                    ),
                    column_type::Primitive::Number => (
                        format!("CAST({param_alias} AS TEXT)"),
                        ScalarType::Number 
                    ),
                    column_type::Primitive::Boolean => (
                        format!("IF({param_alias}, 'True', 'False')"),
                        ScalarType::Boolean 
                    ),
                    column_type::Primitive::Date => (
                        format!("DATE({param_alias}, 'julianday')"),
                        ScalarType::Date 
                    ),
                    column_type::Primitive::Datetime => (
                        format!("STRFTIME('%FT%TZ', {param_alias}, 'julianday')"),
                        ScalarType::Datetime 
                    ),
                    column_type::Primitive::File 
                    | column_type::Primitive::Image => (
                        format!("CASE 
                                    WHEN {param_alias} IS NULL THEN NULL 
                                    WHEN LENGTH({param_alias}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({param_alias}) * 0.000000001)
                                    WHEN LENGTH({param_alias}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({param_alias}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({param_alias}) * 0.001)
                                END"),
                        ScalarType::Blob
                    )
                };
                Some(ScalarExpression {
                    arg_expr: param_alias.clone(),
                    value_expr: param_alias,
                    label_expr,
                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                    return_type
                })
            }
            column_type::ColumnType::Object { table_oid, .. }
            | column_type::ColumnType::Select { table_oid, .. } => {
                Some(ScalarExpression {
                    arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{table_oid}_SURROGATE WHERE OID = {param_alias})"),
                    label_expr: format!("(SELECT LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {param_alias})"),
                    value_expr: param_alias,
                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                    return_type: ScalarType::JSON
                })
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                let datasource_row_alias: String = self.get_datasource_row_alias(&param.datasource);
                let datasource_schema_oid: schema::Metadata = param.datasource.get_schema().oid;
                let label_expr: String = format!(
                    "(SELECT '[' || GROUP_CONCAT(a.JSON_STRINGIFY) || ']' FROM TABLE{table_oid}_SURROGATE a INNER JOIN MULTISELECT{} m ON m.TABLE{table_oid}_OID = a.OID WHERE TABLE{}_OID = {datasource_row_alias}))",
                    param.column.oid,
                    datasource_schema_oid
                );
                Some(ScalarExpression {
                    arg_expr: label_expr.clone(),
                    label_expr,
                    value_expr: format!(
                        "(SELECT GROUP_CONCAT(CAST(TABLE{table_oid}_OID AS TEXT)) FROM MULTISELECT{} WHERE TABLE{}_OID = {datasource_row_alias})",
                        param.column.oid,
                        datasource_schema_oid
                    ),
                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                    return_type: ScalarType::JSON
                })
            }
            column_type::ColumnType::Formula { formula, .. } => {
                let parsed_formula: Formula = Formula::parse(formula)?;
                Some(parsed_formula.compile(self.wrap())?)
            }
            column_type::ColumnType::Subreport { .. } => {
                None // No values are associated with a subreport
            }
        })
    }
}



#[derive(PartialEq, Eq, Clone)]
pub enum SimpleQueryBuilderColumn {
    Primitive {
        /// The parameter.
        param: parameter::Parameter,

        /// The ordinal of the primitive value.
        /// This value is of the type specified by param.column.column_type.
        value_ord: String 
    },

    Object {
        /// The parameter.
        param: parameter::Parameter,

        /// The ordinal of the referenced row(s)' OID.
        /// This value is of type String.
        value_ord: String,

        /// The ordinal of the referenced row(s)' primary key.
        /// This value is of type String.
        label_ord: String 
    },

    Select {
        /// The parameter.
        param: parameter::Parameter,

        /// The ordinal of the referenced row(s)' OID.
        /// This value is of type String.
        value_ord: String
    },

    Multiselect {
        /// The parameter.
        param: parameter::Parameter,

        /// The ordinal of the referenced row(s)' OID.
        /// This value is of type String.
        value_ord: String,

        /// The ordinal of the referenced row(s)' primary key.
        /// This value is of type String.
        label_ord: String 
    },

    Formula {
        /// The parameter.
        param: parameter::Parameter,

        /// The formula.
        formula: String
    },

    Virtual {
        /// The parameter for a virtual column.
        param: parameter::Parameter
    }
}

impl Hash for SimpleQueryBuilderColumn {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let param: &parameter::Parameter = self.borrow();
        param.hash(state)
    }
}

impl Borrow<parameter::Parameter> for SimpleQueryBuilderColumn {
    fn borrow(&self) -> &parameter::Parameter {
        match self {
            Self::Primitive { param, .. }
            | Self::Object { param, .. }
            | Self::Select { param, .. }
            | Self::Multiselect { param, .. }
            | Self::Formula { param, .. }
            | Self::Virtual { param } => param
        }
    }
}

struct SimpleQueryBuilder {
    /// The datasources for the query.
    datasources: HashSet<Join>,

    /// The parameters selected by the query.
    params: HashSet<SimpleQueryBuilderColumn>,

    /// The column definitions.
    cmd_cols: Vec<String>
}

impl SimpleQueryBuilder {
    /// Creates a new simple statement.
    fn new(datasources: Vec<datasource::Datasource>) -> Self {
        // Construct empty query builder
        let mut query_builder: Self = Self {
            datasources: HashSet::new(),
            params: HashSet::new(),
            cmd_cols: Vec::new()
        };
        // Add each datasource to the query builder
        for datasource in datasources.into_iter() {
            query_builder.insert_datasource(datasource);
        }
        return query_builder;
    }

    /// Add a raw column definition to the query builder.
    fn insert_col_definition(&mut self, col_definition: String) {
        self.cmd_cols.push(col_definition);
    }
    
    /// Add a parameter selected by the query.
    fn insert_param(&mut self, param: parameter::Parameter) -> Result<&SimpleQueryBuilderColumn, Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.params.contains(&param) {
            return Ok(self.params.get(&param).expect(""));
        }

        // Add a column for the parameter, and record the true vs display ordinals
        let source_alias: String = self.get_datasource_alias(&param.datasource);
        let column_oid: i64 = param.column.oid;
        let column_alias: String = format!("{source_alias}COLUMN{column_oid}");

        let qparam = match &param.column.column_type {
            column_type::ColumnType::Primitive(_) => {
                self.insert_col_definition(format!("{source_alias}.COLUMN{column_oid} AS {column_alias}"));
                SimpleQueryBuilderColumn::Primitive { 
                    param: param.clone(), 
                    value_ord: column_alias
                }
            },
            column_type::ColumnType::Object { table_oid, .. } => {
                self.insert_col_definition(format!(
                    "{source_alias}.COLUMN{column_oid} AS {column_alias}"
                ));
                self.insert_col_definition(format!(
                    "(SELECT LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {source_alias}.COLUMN{column_oid}) AS {column_alias}_LABEL"
                ));
                SimpleQueryBuilderColumn::Object { 
                    param: param.clone(), 
                    label_ord: format!("{column_alias}_LABEL"), 
                    value_ord: column_alias
                }
            },
            column_type::ColumnType::Select { .. } => {
                self.insert_col_definition(format!(
                    "{source_alias}.COLUMN{column_oid} AS {column_alias}"
                ));
                SimpleQueryBuilderColumn::Select { 
                    param: param.clone(), 
                    value_ord: column_alias
                }
            },
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                let datasource_schema_metadata: schema::Metadata = param.datasource.get_schema();
                self.insert_col_definition(format!(
                    "
                    (
                        SELECT 
                            GROUP_CONCAT(CAST(m.TABLE{table_oid}_OID AS TEXT)) 
                        FROM MULTISELECT{column_oid} m
                        WHERE m.TABLE{}_OID = {source_alias}.OID
                    ) AS {column_alias}
                    ",
                    datasource_schema_metadata.oid
                ));
                self.insert_col_definition(format!(
                    "
                    (
                        SELECT 
                            '[' || GROUP_CONCAT(t.JSON_STRINGIFY, ', ') || ']' 
                        FROM MULTISELECT{column_oid} m
                        INNER JOIN TABLE{table_oid}_SURROGATE t ON t.OID = m.TABLE{table_oid}_OID
                        WHERE m.TABLE{}_OID = {source_alias}.OID
                    ) AS {column_alias}_LABEL
                    ",
                    datasource_schema_metadata.oid
                ));
                SimpleQueryBuilderColumn::Multiselect { 
                    param: param.clone(), 
                    label_ord: format!("{column_alias}_LABEL"), 
                    value_ord: column_alias
                }
            },
            column_type::ColumnType::Formula { formula, .. } => {
                // Virtual parameter, substitute formula wherever it occurs
                SimpleQueryBuilderColumn::Formula { 
                    param: param.clone(), 
                    formula: formula.clone()
                }
            },
            column_type::ColumnType::Subreport { .. } => {
                // Virtual parameter, do nothing in this query
                SimpleQueryBuilderColumn::Virtual { 
                    param: param.clone()
                }
            }
        };
        self.params.insert(qparam);
        return Ok(self.params.get(&param).expect(""));
    }
}

impl QueryBuilder for SimpleQueryBuilder {
    fn compile(mut self) -> String {
        format!("SELECT {} FROM {} {}",
            // Raw columns
            self.cmd_cols.iter().map(|e| e.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap(),
            // Root tables
            self.datasources.iter()
                .filter_map(|join| {
                    if let Join::Root(datasource) = join {
                        let datasource_oid: i64 = datasource.get_oid();
                        let schema_metadata: schema::Metadata = datasource.get_schema();
                        Some(format!("TABLE{} d{datasource_oid}", schema_metadata.oid))
                    } else {
                        None
                    }
                })
                .reduce(|acc, e| format!("{acc} INNER JOIN {e}"))
                .unwrap(),
            // Linked tables
            self.datasources.into_iter()
                .filter_map(|join| {
                    if let Join::Precompiled { join_clause, .. } = join {
                        Some(join_clause)
                    } else {
                        None
                    }
                })
                .reduce(|acc, e| format!("{acc} {e}"))
                .unwrap_or(String::from("")),
        )
    }

    fn get_datasource_alias(&mut self, datasource: &datasource::Datasource) -> String {
        self.insert_datasource(datasource);
        format!("d{}", param.datasource.get_oid())
    }

    fn get_datasource_row_alias(&mut self, datasource: &datasource::Datasource) -> String {
        let datasource_alias: String = self.get_datasource_alias(datasource);
        format!("{datasource_alias}_OID")
    }

    fn get_parameter_alias(&mut self, param: &parameter::Parameter) -> String {
        let datasource_alias: String = self.get_datasource_alias(datasource);
        format!("{datasource_alias}.COLUMN{}", param.column.oid)
    }

    fn contains_datasource(&self, datasource: &datasource::Datasource) -> bool {
        self.datasources.contains(datasource)
    }
    
    fn insert_datasource(&mut self, datasource: &datasource::Datasource) {
        // First, make sure an existing datasource is not being duplicated
        if self.contains_datasource(datasource) {
            return;
        }

        let datasource_alias: String = format!("d{}", datasource.get_oid());
        match &datasource {
            datasource::Datasource::Table { .. } => {
                self.datasources.insert(Join::Root(datasource.clone()));
            },
            datasource::Datasource::Inheritance { parent_datasource, table, .. } => {
                // Check whether the datasource is inheriting from or inherited by the parent datasource
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: if table.master_tables.contains(&parent_schema_metadata) {
                        format!(
                            "LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.MASTER{}_OID = {parent_datasource_alias}.OID",
                            table.schema.oid,
                            parent_schema_metadata.oid
                        )
                    } else {
                        format!(
                            "INNER JOIN TABLE{} {datasource_alias} ON {datasource_alias}.OID = {parent_datasource_alias}.MASTER{}_OID",
                            table.schema.oid,
                            table.schema.oid
                        )
                    },
                    datasource: datasource.clone()
                });
            },
            datasource::Datasource::Object { parent_datasource, column, .. } 
            | datasource::Datasource::Select { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: if column.schema.oid == parent_schema_metadata.oid {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {parent_datasource_alias}.COLUMN{} = {datasource_alias}.OID", schema_metadata.oid, column.oid)
                    } else {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.COLUMN{} = {parent_datasource_alias}.OID", schema_metadata.oid, column.oid)
                    },
                    datasource: datasource.clone()
                });
            },
            datasource::Datasource::Multiselect { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: format!("
                        LEFT JOIN MULTISELECT{} {datasource_alias}m ON {datasource_alias}m.TABLE{}_OID = {parent_datasource_alias}.OID
                        LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}m.TABLE{}_OID = {datasource_alias}.OID
                        ", 
                        column.oid,
                        parent_schema_metadata.oid,
                        schema_metadata.oid, 
                        schema_metadata.oid
                    ),
                    datasource
                });
            }
        }

        self.insert_col_definition(format!("{datasource_alias}.OID AS {datasource_alias}_OID"));
    }
}



struct InlineQueryBuilder<'a> {
    /// CTE to select the parameters
    param_cte: &'a SimpleQueryBuilder,

    /// Additional datasources for the inline query.
    datasources: HashSet<Join>
}

impl<'a> InlineQueryBuilder<'a> {
    pub fn new(param_cte: &'a SimpleQueryBuilder) -> Self {
        Self {
            param_cte,
            datasources: HashSet::new()
        }
    }

    /// 
    fn compile_parameter(&self, param: parameter::Parameter) -> (String, String) {
        let datasource_alias: String = self.get_datasource_alias(&param.datasource);

        match &param.column.column_type {
            
        }
    }
}

impl<'a> QueryBuilder for InlineQueryBuilder<'a>  {
    fn wrap(&self) -> FormulaWrapper<'a> {
        FormulaWrapper::Inline(self)
    }

    fn compile(mut self) -> String {
        todo!("Compilation of inline queries not implemented yet!")
    }

    fn get_datasource_alias(&mut self, datasource: &datasource::Datasource) -> String {
        let deep_datasource: datasource::Datasource = datasource.get_deep_relationship();
        if self.param_cte.contains_datasource(deep_datasource) {
            self.param_cte.insert_datasource(datasource);
            String::from("p")
        } else {
            self.insert_datasource(datasource);
            format!("d{}", param.datasource.get_oid())
        }
    }

    fn get_datasource_row_alias(&mut self, datasource: &datasource::Datasource) -> String {
        let deep_datasource: datasource::Datasource = datasource.get_deep_relationship();
        if self.param_cte.contains_datasource(deep_datasource) {
            let datasource_alias: String = self.param_cte.get_datasource_alias(datasource);
            format!("p.{datasource_alias}_OID")
        } else {
            self.insert_datasource(datasource);
            format!("d{}.OID", param.datasource.get_oid())
        }
    }

    fn get_parameter_alias(&mut self, param: &parameter::Parameter) -> String {
        let deep_datasource: datasource::Datasource = datasource.get_deep_relationship();
        if self.param_cte.contains_datasource(deep_datasource) {
            self.param_cte.insert_param(param);
            format!("p.d{}COLUMN{}", param.datasource.get_oid(), param.column.oid)
        } else {
            self.insert_datasource(datasource);
            format!("d{}.COLUMN{}", param.column.oid)
        }
    }

    fn contains_datasource(&self, datasource: &datasource::Datasource) -> bool {
        self.param_cte.contains_datasource(datasource) || self.datasources.contains(datasource)
    }
    
    fn insert_datasource(&mut self, datasource: &datasource::Datasource) {
        // First, make sure an existing datasource is not being duplicated
        if self.contains_datasource(datasource) {
            return;
        }

        let datasource_alias: String = format!("d{}", datasource.get_oid());
        match &datasource {
            datasource::Datasource::Table { .. } => {
                self.datasources.insert(Join::Root(datasource.clone()));
            },
            datasource::Datasource::Inheritance { parent_datasource, table, .. } => {
                // Check whether the datasource is inheriting from or inherited by the parent datasource
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: if table.master_tables.contains(&parent_schema_metadata) {
                        format!(
                            "LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.MASTER{}_OID = {parent_datasource_alias}.OID",
                            table.schema.oid,
                            parent_schema_metadata.oid
                        )
                    } else {
                        format!(
                            "INNER JOIN TABLE{} {datasource_alias} ON {datasource_alias}.OID = {parent_datasource_alias}.MASTER{}_OID",
                            table.schema.oid,
                            table.schema.oid
                        )
                    },
                    datasource: datasource.clone()
                });
            },
            datasource::Datasource::Object { parent_datasource, column, .. } 
            | datasource::Datasource::Select { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: if column.schema.oid == parent_schema_metadata.oid {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {parent_datasource_alias}.COLUMN{} = {datasource_alias}.OID", schema_metadata.oid, column.oid)
                    } else {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.COLUMN{} = {parent_datasource_alias}.OID", schema_metadata.oid, column.oid)
                    },
                    datasource: datasource.clone()
                });
            },
            datasource::Datasource::Multiselect { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = self.get_datasource_alias(parent_datasource);
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: format!("
                        LEFT JOIN MULTISELECT{} {datasource_alias}m ON {datasource_alias}m.TABLE{}_OID = {parent_datasource_alias}.OID
                        LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}m.TABLE{}_OID = {datasource_alias}.OID
                        ", 
                        column.oid,
                        parent_schema_metadata.oid,
                        schema_metadata.oid, 
                        schema_metadata.oid
                    ),
                    datasource
                });
            }
        }
    }
}



pub enum TopLevelQueryBuilderColumn {
    /// Results that may either be readonly or a reference to another cell.
    Formula {
        /// The metadata for the column.
        column: column::Metadata,

        /// The ordinal that gives the cell's value.
        value_ord: String,

        /// The ordinal that gives the cell's label.
        label_ord: String,

        /// The ordinal that gives the stringified parameter of the referenced cell.
        /// This stringified parameter is expected to be of the form "d{datasource_oid}_OID:{column_oid}". It will be NULL if the cell should be a readonly value.
        /// The row OID of the referenced cell is then represented by "d{datasource_oid}_OID".
        param_ord: String,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    },

    Primitive {
        /// The metadata for the column.
        column: column::Metadata,

        /// The ordinal that gives the cell's label.
        label_ord: String,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    },

    Object {
        /// The metadata for the column.
        column: column::Metadata,

        /// The ordinal that gives the cell's value.
        /// This value will be Option<i64>.
        value_ord: String,

        /// The ordinal that gives the cell's label.
        /// This value will be Option<String>.
        label_ord: String,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    },

    Select {
        /// The metadata for the column.
        column: column::Metadata,

        /// The ordinal that gives the cell's value.
        /// This value will be Option<i64>.
        value_ord: String,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    },

    Multiselect {
        /// The metadata for the column.
        column: column::Metadata,

        /// The ordinal that gives the cell's value.
        /// This value will be Option<String>, and is expected to be a comma-separated sequence of integers.
        value_ord: String,

        /// The ordinal that gives the cell's label.
        /// This value will be Option<String>.
        label_ord: String,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    },

    Subreport {
        /// The metadata for the column.
        column: column::Metadata,

        /// The OID of the report being linked to.
        report_oid: i64,

        /// The ordinals for each datasource row OID passed in to the subreport.
        datasource_ords: Vec<(datasource::Datasource, String)>,

        /// The ordinal that gives the cell's row OID.
        row_ord: String 
    }
}

pub struct TopLevelQueryBuilder {
    /// CTE to select the parameters
    param_cte: SimpleQueryBuilder,

    /// The columns selected for the final query
    cmd_cols: HashSet<String>,

    /// Expressions used to group the rows in the final query
    cmd_groupby: Vec<String>,

    /// Expressions used to order the rows in the final query
    cmd_orderby: Vec<String>
}

impl TopLevelQueryBuilder {
    /// Creates a new query.
    pub fn new(datasources: Vec<datasource::Datasource>) -> Self {
        let mut query: Self = Self {
            param_cte: SimpleQueryBuilder::new(datasources),
            cmd_cols: HashSet::new(),
            cmd_groupby: Vec::new(),
            cmd_orderby: Vec::new()
        };
        if query.param_cte.datasources.len() == 1 {
            if let Some(join) = query.param_cte.datasources.iter().last() {
                let prime_datasource: &datasource::Datasource = join.borrow();
                query.insert_col_definition(format!("d{}_OID AS OID", prime_datasource.get_oid()));
            }
        }
        return query;
    }

    /// Add a raw column definition to the query builder.
    fn insert_col_definition(&mut self, col_definition: String) {
        self.cmd_cols.insert(col_definition);
    }

    /// Converts a formula into a scalar SQL expression.
    fn formula_to_scalar_expression(&self, formula: Formula) -> Result<ScalarExpression, Error> {
        Ok(match formula {
            Formula::Null => ScalarExpression { 
                arg_expr: String::from("NULL"), 
                value_expr: String::from("NULL"), 
                label_expr: String::from("NULL"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Null 
            },
            Formula::LiteralInt(num) => ScalarExpression { 
                arg_expr: format!("{num}"), 
                value_expr: format!("{num}"), 
                label_expr: format!("CAST({num} AS TEXT)"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Integer 
            },
            Formula::LiteralFloat(num) => ScalarExpression { 
                arg_expr: format!("{num}"), 
                value_expr: format!("{num}"), 
                label_expr: format!("CAST({num} AS TEXT)"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Number 
            },
            Formula::LiteralBool(b) => ScalarExpression { 
                arg_expr: format!("{}", if b { "TRUE" } else { "FALSE" }),
                value_expr: format!("{}", if b { "TRUE" } else { "FALSE" }), 
                label_expr: format!("{}", if b { "'True'" } else { "'False'" }),
                param_expr: String::from("NULL"), 
                return_type: ScalarType::Boolean 
            },
            Formula::LiteralString(text) => ScalarExpression { 
                arg_expr: format!("'{}'", text.replace("'", "''")),  
                value_expr: format!("'{}'", text.replace("'", "''")), 
                label_expr: format!("'{}'", text.replace("'", "''")), 
                param_expr: String::from("NULL"), 
                return_type: ScalarType::Text 
            },
            Formula::Param { datasource_oid, column_oid } => {
                let datasource: datasource::Datasource = datasource::Datasource::get(datasource_oid)?;
                let column_metadata: column::Metadata = column::Metadata::get(column_oid)?;
                match self.param_cte.insert_param(parameter::Parameter {
                    datasource,
                    column: column_metadata.clone()
                })? {
                    SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                        let (label_expr, return_type) = match param.column.column_type {
                            column_type::ColumnType::Primitive(prim) => match prim {
                                column_type::Primitive::Text => (value_ord.clone(), ScalarType::Text),
                                column_type::Primitive::JSON => (value_ord.clone(), ScalarType::JSON),
                                column_type::Primitive::Integer => (format!("CAST({value_ord} AS TEXT)"), ScalarType::Integer),
                                column_type::Primitive::Number => (format!("CAST({value_ord} AS TEXT)"), ScalarType::Number),
                                column_type::Primitive::Checkbox => (format!("IF({value_ord}, 'True', 'False')"), ScalarType::Boolean),
                                column_type::Primitive::Date => (format!("DATE({value_ord}, 'julianday')"), ScalarType::Date),
                                column_type::Primitive::Datetime => (format!("STRFTIME('%FT%TZ', {value_ord}, 'julianday')"), ScalarType::Datetime),
                                column_type::Primitive::File
                                | column_type::Primitive::Image => (
                                    format!("
                                    CASE 
                                        WHEN {value_ord} IS NULL THEN NULL 
                                        WHEN LENGTH({value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({value_ord}) * 0.000000001)
                                        WHEN LENGTH({value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({value_ord}) * 0.000001)
                                        ELSE FORMAT('%.1f KB', LENGTH({value_ord}) * 0.001)
                                    END
                                    "), 
                                    ScalarType::Blob
                                )
                            },
                            _ => { return Err(Error::AdhocError("Expected primitive column type.")); }
                        };
                        ScalarExpression {
                            arg_expr: value_ord.clone(),
                            value_expr: value_ord.clone(),
                            label_expr,
                            param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                            return_type
                        }
                    },
                    SimpleQueryBuilderColumn::Select { param, value_ord } => {
                        let referenced_table_oid: i64 = 0;
                        ScalarExpression {
                            arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                            value_expr: value_ord.clone(),
                            label_expr: format!("(SELECT LABEL FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                            param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                            return_type: ScalarType::JSON
                        }
                    },
                    SimpleQueryBuilderColumn::Multiselect { param, value_ord, label_ord } => {
                        let referenced_table_oid: i64 = 0;
                        ScalarExpression {
                            arg_expr: label_ord.clone(),
                            value_expr: value_ord.clone(),
                            label_expr: label_ord.clone(),
                            param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                            return_type: ScalarType::JSON
                        }
                    },
                    SimpleQueryBuilderColumn::Object { param, value_ord, label_ord } => {
                        let referenced_table_oid: i64 = 0;
                        ScalarExpression {
                            arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                            value_expr: value_ord.clone(),
                            label_expr: label_ord.clone(),
                            param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                            return_type: ScalarType::JSON
                        }
                    }
                    SimpleQueryBuilderColumn::Formula { param, formula } => {
                        let parsed_formula: Formula = Formula::parse(formula.clone())?;
                        let inner_expression: ScalarExpression = self.formula_to_scalar_expression(parsed_formula)?;
                        ScalarExpression {
                            arg_expr: format!("({})", inner_expression.arg_expr),
                            value_expr: format!("({})", inner_expression.value_expr),
                            label_expr: format!("({})", inner_expression.label_expr),
                            param_expr: format!("({})", inner_expression.param_expr),
                            return_type: inner_expression.return_type
                        }
                    }
                    SimpleQueryBuilderColumn::Virtual { param } => {
                        return Err(Error::AdhocError("A subreport cannot be used as a parameter to a formula!"));
                    }
                }
            },
            Formula::Wrap(inner) => {
                let inner_expression = self.formula_to_scalar_expression(*inner)?;
                ScalarExpression {
                    arg_expr: format!("({})", inner_expression.arg_expr),
                    value_expr: format!("({})", inner_expression.value_expr),
                    label_expr: format!("({})", inner_expression.label_expr),
                    param_expr: format!("({})", inner_expression.param_expr),
                    return_type: inner_expression.return_type
                }
            },
            Formula::Or(lhs, rhs) => {
                let lhs_expression = self.formula_to_scalar_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_scalar_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("{} OR {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} OR {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} OR {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::And(lhs, rhs) => {
                let lhs_expression = self.formula_to_scalar_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_scalar_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("{} AND {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} AND {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} AND {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::Not(inner) => {
                let inner_expression = self.formula_to_scalar_expression(*inner)?;
                if !ScalarType::Boolean.contains(inner_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "not",
                        inner_name: inner.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: inner_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("NOT ({})", inner_expression.arg_expr),
                    value_expr: format!("NOT ({})", inner_expression.arg_expr),
                    label_expr: format!("IF({}, 'False', 'True')", inner_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::Eq(lhs, rhs) => {
                let lhs_expression = self.formula_to_scalar_expression(*lhs)?;
                let rhs_expression = self.formula_to_scalar_expression(*rhs)?;
                ScalarExpression {
                    arg_expr: format!("{} IS {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} IS {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} IS {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::In { value, collection } => {
                let lhs_expression = self.formula_to_scalar_expression(*value)?;
                let rhs_expression = self.formula_to_scalar_expression(*collection)?;
                ScalarExpression {
                    arg_expr: format!("{} IN {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} IN {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} IN {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            }
            /*,
            Formula::Or(lhs, rhs) => format!("{} OR {}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::And(lhs, rhs) => format!("{} AND {}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Not(inner) => format!("NOT ({})", self.formula_to_expression(*inner)?),
            Formula::Add(lhs, rhs) => format!("{}+{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Subtract(lhs, rhs) => format!("{}-{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Multiply(lhs, rhs) => format!("{}*{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Divide(lhs, rhs) => format!("{}/{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Modulo(lhs, rhs) => format!("{}%{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?),
            Formula::Concat(lhs, rhs) => format!("{}||{}", self.formula_to_expression(*lhs)?, self.formula_to_expression(*rhs)?)
            */
        })
    }

    /// Converts a formula into an SQL expression that returns a collection of items (e.g. for the right-hand side of an IN operator).
    fn formula_to_collection_expression(&self, formula: Formula) -> Result<CollectionExpression, Error> {
        Ok(match formula {
            Formula::Null => CollectionExpression {
                query: SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p")),
                item: ScalarExpression { 
                    arg_expr: String::from("NULL"), 
                    value_expr: String::from("NULL"), 
                    label_expr: String::from("NULL"),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Null 
                }
            },
            Formula::LiteralInt(num) => CollectionExpression {
                query: SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p")),
                item: ScalarExpression { 
                    arg_expr: format!("{num}"), 
                    value_expr: format!("{num}"), 
                    label_expr: format!("CAST({num} AS TEXT)"),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Integer 
                }
            },
            Formula::LiteralFloat(num) => CollectionExpression {
                query: SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p")),
                item: ScalarExpression { 
                    arg_expr: format!("{num}"), 
                    value_expr: format!("{num}"), 
                    label_expr: format!("CAST({num} AS TEXT)"),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Number 
                }
            },
            Formula::LiteralBool(b) => CollectionExpression {
                query: SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p")),
                item: ScalarExpression { 
                    arg_expr: format!("{}", if b { "TRUE" } else { "FALSE" }),
                    value_expr: format!("{}", if b { "TRUE" } else { "FALSE" }), 
                    label_expr: format!("{}", if b { "'True'" } else { "'False'" }),
                    param_expr: String::from("NULL"), 
                    return_type: ScalarType::Boolean 
                }
            },
            Formula::LiteralString(text) => CollectionExpression {
                query: SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p")),
                item: ScalarExpression { 
                    arg_expr: format!("'{}'", text.replace("'", "''")),  
                    value_expr: format!("'{}'", text.replace("'", "''")), 
                    label_expr: format!("'{}'", text.replace("'", "''")), 
                    param_expr: String::from("NULL"), 
                    return_type: ScalarType::Text 
                }
            },
            Formula::Param { datasource_oid, column_oid } => {
                let query: SimpleQueryBuilder = SimpleQueryBuilder::new_derivative(&self.param_cte.datasources, String::from("p"));
                let datasource: datasource::Datasource = datasource::Datasource::get(datasource_oid)?;
                let column_metadata: column::Metadata = column::Metadata::get(column_oid)?;
                match query.insert_param(parameter::Parameter {
                    datasource,
                    column: column_metadata.clone()
                })? {
                    SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                        let (label_expr, return_type) = match param.column.column_type {
                            column_type::ColumnType::Primitive(prim) => match prim {
                                column_type::Primitive::Text => (value_ord.clone(), ScalarType::Text),
                                column_type::Primitive::JSON => (value_ord.clone(), ScalarType::JSON),
                                column_type::Primitive::Integer => (format!("CAST({value_ord} AS TEXT)"), ScalarType::Integer),
                                column_type::Primitive::Number => (format!("CAST({value_ord} AS TEXT)"), ScalarType::Number),
                                column_type::Primitive::Checkbox => (format!("IF({value_ord}, 'True', 'False')"), ScalarType::Boolean),
                                column_type::Primitive::Date => (format!("DATE({value_ord}, 'julianday')"), ScalarType::Date),
                                column_type::Primitive::Datetime => (format!("STRFTIME('%FT%TZ', {value_ord}, 'julianday')"), ScalarType::Datetime),
                                column_type::Primitive::File
                                | column_type::Primitive::Image => (
                                    format!("
                                    CASE 
                                        WHEN {value_ord} IS NULL THEN NULL 
                                        WHEN LENGTH({value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({value_ord}) * 0.000000001)
                                        WHEN LENGTH({value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({value_ord}) * 0.000001)
                                        ELSE FORMAT('%.1f KB', LENGTH({value_ord}) * 0.001)
                                    END
                                    "), 
                                    ScalarType::Blob
                                )
                            },
                            _ => { return Err(Error::AdhocError("Expected primitive column type.")); }
                        };
                        CollectionExpression {
                            query,
                            item: ScalarExpression {
                                arg_expr: value_ord.clone(),
                                value_expr: value_ord.clone(),
                                label_expr,
                                param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                return_type
                            }
                        }
                    },
                    SimpleQueryBuilderColumn::Select { param, value_ord } => {
                        let referenced_table_oid: i64 = 0;
                        CollectionExpression {
                            query,
                            item: ScalarExpression {
                                arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                                value_expr: value_ord.clone(),
                                label_expr: format!("(SELECT LABEL FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                                param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                return_type: ScalarType::JSON
                            }
                        }
                    },
                    SimpleQueryBuilderColumn::Multiselect { param, value_ord, label_ord } => {
                        let referenced_table_oid: i64 = 0;
                        CollectionExpression {
                            query,
                            item: ScalarExpression {
                                arg_expr: label_ord.clone(),
                                value_expr: value_ord.clone(),
                                label_expr: label_ord.clone(),
                                param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                return_type: ScalarType::JSON
                            }
                        }
                    },
                    SimpleQueryBuilderColumn::Object { param, value_ord, label_ord } => {
                        let referenced_table_oid: i64 = 0;
                        CollectionExpression {
                            query,
                            item: ScalarExpression {
                                arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                                value_expr: value_ord.clone(),
                                label_expr: label_ord.clone(),
                                param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                return_type: ScalarType::JSON
                            }
                        }
                    }
                    SimpleQueryBuilderColumn::Formula { param, formula } => {
                        let parsed_formula: Formula = Formula::parse(formula.clone())?;
                        let inner_expression: ScalarExpression = self.formula_to_scalar_expression(parsed_formula)?;
                        CollectionExpression {
                            query,
                            item: ScalarExpression {
                                arg_expr: format!("({})", inner_expression.arg_expr),
                                value_expr: format!("({})", inner_expression.value_expr),
                                label_expr: format!("({})", inner_expression.label_expr),
                                param_expr: format!("({})", inner_expression.param_expr),
                                return_type: inner_expression.return_type
                            }
                        }
                    }
                    SimpleQueryBuilderColumn::Virtual { param } => {
                        return Err(Error::AdhocError("A subreport cannot be used as a parameter to a formula!"));
                    }
                }
            },
            Formula::Wrap(inner) => {
                let inner_expression = self.formula_to_collection_expression(*inner)?;
                CollectionExpression {
                    query: inner_expression.query,
                    item: ScalarExpression {
                        arg_expr: format!("({})", inner_expression.item.arg_expr),
                        value_expr: format!("({})", inner_expression.item.value_expr),
                        label_expr: format!("({})", inner_expression.item.label_expr),
                        param_expr: format!("({})", inner_expression.item.param_expr),
                        return_type: inner_expression.item.return_type
                    }
                }
            },
            Formula::Or(lhs, rhs) => {
                let lhs_expression = self.formula_to_collection_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.item.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.item.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_collection_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.item.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.item.return_type.to_string()
                    });
                }

                CollectionExpression {
                    query: SimpleQueryBuilder::merge(lhs_expression.query, rhs_expression.query),
                    item: ScalarExpression {
                        arg_expr: format!("{} OR {}", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        value_expr: format!("{} OR {}", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        label_expr: format!("IF({} OR {}, 'True', 'False')", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        param_expr: String::from("NULL"),
                        return_type: ScalarType::Boolean
                    }
                }
            },
            Formula::And(lhs, rhs) => {
                let lhs_expression = self.formula_to_collection_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.item.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.item.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_collection_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.item.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.item.return_type.to_string()
                    });
                }

                CollectionExpression {
                    query: SimpleQueryBuilder::merge(lhs_expression.query, rhs_expression.query),
                    item: ScalarExpression {
                        arg_expr: format!("{} AND {}", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        value_expr: format!("{} AND {}", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        label_expr: format!("IF({} AND {}, 'True', 'False')", lhs_expression.item.arg_expr, rhs_expression.item.arg_expr),
                        param_expr: String::from("NULL"),
                        return_type: ScalarType::Boolean
                    }
                }
            },
            Formula::Not(inner) => {
                let inner_expression = self.formula_to_collection_expression(*inner)?;
                if !ScalarType::Boolean.contains(inner_expression.item.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "not",
                        inner_name: inner.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: inner_expression.item.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("NOT ({})", inner_expression.item.arg_expr),
                    value_expr: format!("NOT ({})", inner_expression.item.arg_expr),
                    label_expr: format!("IF({}, 'False', 'True')", inner_expression.item.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::Eq(lhs, rhs) => {
                let lhs_expression = self.formula_to_collection_expression(*lhs)?;
                let rhs_expression = self.formula_to_collection_expression(*rhs)?;
                ScalarExpression {
                    arg_expr: format!("{} IS {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} IS {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} IS {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
        })
    }

    /// Inserts a column associated with a datasource.
    pub fn column(&mut self, column: parameter::Parameter) -> Result<TopLevelQueryBuilderColumn, Error> {
        let datasource_alias: String = self.get_datasource_alias(&column.datasource);
        let qparam: SimpleQueryBuilderColumn = self.param_cte.insert_param(column)?.clone();
        match qparam {
            SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                match &param.column.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::Text
                            | column_type::Primitive::JSON => {
                                self.insert_col_definition(format!("{datasource_alias}.{value_ord} AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Integer
                            | column_type::Primitive::Number
                            | column_type::Primitive::Checkbox => {
                                // Cast number to text
                                self.insert_col_definition(format!("CAST({datasource_alias}.{value_ord} AS TEXT) AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Date => {
                                // Cast date to UTF Date string
                                self.insert_col_definition(format!("DATE({datasource_alias}.{value_ord}, 'julianday') AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Datetime => {
                                // Cast datetime to UTF Datetime string
                                self.insert_col_definition(format!("STRFTIME('%FT%TZ', {datasource_alias}.{value_ord}, 'julianday') AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::File
                            | column_type::Primitive::Image => {
                                // Label is size of file
                                self.insert_col_definition(format!("
                                CASE 
                                    WHEN {datasource_alias}.{value_ord} IS NULL THEN NULL 
                                    WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({datasource_alias}.{value_ord}) * 0.000000001)
                                    WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({datasource_alias}.{value_ord}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({datasource_alias}.{value_ord}) * 0.001)
                                END AS {value_ord}_LABEL
                                "));
                            }
                        }
                        return Ok(TopLevelQueryBuilderColumn::Primitive {
                            column: param.column, 
                            label_ord: format!("{value_ord}_LABEL"),
                            row_ord: format!("d{}_OID", param.datasource.get_oid())
                        });
                    }
                    _ => {
                        // Throw error
                        return Err(Error::AdhocError("Parameter does not match column type."));
                    }
                }
            }
            SimpleQueryBuilderColumn::Object { param, value_ord, label_ord } => {
                self.insert_col_definition(format!("{datasource_alias}.{value_ord}"));
                self.insert_col_definition(format!("{datasource_alias}.{label_ord}"));
                return Ok(TopLevelQueryBuilderColumn::Object { 
                    column: param.column, 
                    value_ord, 
                    label_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Select { param, value_ord } => {
                self.insert_col_definition(format!("{datasource_alias}.{value_ord}"));
                return Ok(TopLevelQueryBuilderColumn::Select { 
                    column: param.column, 
                    value_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Multiselect { param, value_ord, label_ord } => {
                self.insert_col_definition(format!("{datasource_alias}.{value_ord}"));
                self.insert_col_definition(format!("{datasource_alias}.{label_ord}"));
                return Ok(TopLevelQueryBuilderColumn::Multiselect { 
                    column: param.column, 
                    value_ord, 
                    label_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Formula { param, formula } => {
                todo!("Parse formula into column expression");
            }
            SimpleTopLevelQueryBuilderColumn::Virtual { param } => {
                // Do not insert anything for subreports, which are a virtual column
                match param.column.column_type {
                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        return Ok(TopLevelQueryBuilderColumn::Subreport { 
                            column: param.column, 
                            report_oid,
                            datasource_ords: self.param_cte.datasources.iter()
                                .filter_map(|join| {
                                    let datasource: &datasource::Datasource = join.borrow();
                                    match datasource {
                                        datasource::Datasource::Table { .. } => {
                                            // Always include root tables in the filter of a subreport
                                            Some((datasource.clone(), format!("d{}_OID", datasource.get_oid())))
                                        }
                                        _ => {
                                            // Include if the datasource has a direct 1-to-N relationship with its parent datasource
                                            if datasource::Relationship::Many == datasource.get_relationship() {
                                                Some((datasource.clone(), format!("d{}_OID", datasource.get_oid())))
                                            } else {
                                                None
                                            }
                                        }
                                    }
                                })
                                .collect(),
                            row_ord: format!("d{}_OID", param.datasource.get_oid())
                        });
                    }
                    _ => {
                        // Throw error
                        return Err(Error::AdhocError("Parameter does not match column type."));
                    }
                }
            }
        }
    }

    /// Inserts a clause for filtering rows from the returned query.
    pub fn filter(&mut self, formula: String) -> Result<(), Error> {
        todo!("Implement filtering by formula");
    }

    /// Inserts a clause for explicitly grouping rows from the returned query.
    pub fn group_by(&mut self, column: parameter::Parameter) -> Result<(), Error> {
        let qparam: SimpleQueryBuilderColumn = self.param_cte.insert_param(column)?.clone();
        match qparam {
            SimpleQueryBuilderColumn::Primitive { value_ord, .. } 
            | SimpleQueryBuilderColumn::Object { value_ord, .. }
            | SimpleQueryBuilderColumn::Select { value_ord, .. }
            | SimpleQueryBuilderColumn::Multiselect { value_ord, .. } => {
                self.cmd_groupby.push(value_ord);
                return Ok(());
            }
            SimpleQueryBuilderColumn::Formula { param, formula } => {
                todo!("Parse formula into column expression");
            }
            SimpleQueryBuilderColumn::Virtual { .. } => {
                return Err(Error::AdhocError("Unable to sort by a subreport."));
            }
        }
    }

    /// Inserts a clause for ordering the returned query.
    pub fn order_by(&mut self, column: parameter::Parameter, sort_ascending: bool) -> Result<(), Error> {
        let asc = if sort_ascending { "ASC" } else { "DESC" };
        let qparam: SimpleQueryBuilderColumn = self.param_cte.insert_param(column)?.clone();
        match qparam {
            SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                match &param.column.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::Text
                            | column_type::Primitive::JSON 
                            | column_type::Primitive::Integer
                            | column_type::Primitive::Number
                            | column_type::Primitive::Checkbox
                            | column_type::Primitive::Date
                            | column_type::Primitive::Datetime => {
                                // Sort by raw value
                                self.cmd_orderby.push(format!("{value_ord} {asc}"));
                            }
                            column_type::Primitive::File
                            | column_type::Primitive::Image => {
                                // Sort by size of file
                                self.cmd_orderby.push(format!("LENGTH({value_ord}) {asc}"));
                            }
                        }
                        return Ok(());
                    }
                    _ => {
                        // Throw error
                        return Err(Error::AdhocError("Parameter does not match column type."));
                    }
                }
            }
            SimpleQueryBuilderColumn::Object { label_ord, .. }
            | SimpleQueryBuilderColumn::Multiselect { label_ord, .. } => {
                self.cmd_orderby.push(format!("{label_ord} {asc}"));
                return Ok(());
            }
            SimpleQueryBuilderColumn::Select { value_ord, .. } => {
                self.cmd_orderby.push(format!("{value_ord} {asc}")); // TODO instead of ordering by the OID of the referenced row, order by the ordering of the referenced schema?
                return Ok(());
            }
            SimpleQueryBuilderColumn::Formula { param, formula } => {
                todo!("Parse formula into column expression");
            }
            SimpleQueryBuilderColumn::Virtual { .. } => {
                return Err(Error::AdhocError("Unable to sort by a subreport."));
            }
        }
    }
}

impl QueryBuilder for TopLevelQueryBuilder {
    fn wrap<'a>(&'a self) -> FormulaWrapper<'a> {
        FormulaWrapper::TopLevel(self)
    }

    fn compile(mut self) -> String {
        // Compile GROUP BY expression, if one exists
        let groupby: String = if self.cmd_groupby.len() > 0 {
            self.cmd_groupby.iter().fold(String::from("GROUP BY "), |acc, e| format!("{acc}, {e}"))
        } else {
            String::from("")
        };

        // Compile ORDER BY expression, if one exists
        let orderby: String = if self.cmd_orderby.len() > 0 {
            self.cmd_orderby.iter().fold(String::from("ORDER BY "), |acc, e| format!("{acc}, {e}"))
        } else {
            String::from("")
        };

        // Add column for the row's index
        self.insert_col_definition(format!("ROW_NUMBER() OVER ({orderby}) AS ROW_INDEX"));

        // Put it all together in an SQLite SELECT statement
        format!(
            "WITH PARAM_CTE AS ({}) SELECT {} FROM PARAM_CTE p {} {}",
            self.param_cte.compile(),
            self.cmd_cols.iter().map(|e| e.clone()).reduce(|acc, e| format!("{acc}, {e}")).unwrap(),
            groupby,
            orderby
        )
    }

    fn get_datasource_alias(&mut self, datasource: &datasource::Datasource) -> String {
        self.insert_datasource(datasource)
        String::from("p")
    }

    fn get_datasource_row_alias(&mut self, datasource: &datasource::Datasource) -> String {
        self.insert_datasource(datasource);
        format!("p.d{}_OID", datasource.get_oid())
    }

    fn get_parameter_alias(&mut self, param: &parameter::Parameter) -> String {
        self.param_cte.insert_param(param);
        format!("")
    }

    fn contains_datasource(&self, datasource: &datasource::Datasource) -> bool {
        self.param_cte.contains_datasource(datasource)
    }

    fn insert_datasource(&mut self, datasource: &datasource::Datasource) {
        self.param_cte.insert_datasource(datasource)
    }
}



struct CollectionExpression {
    /// Used to query from datasources.
    query: SimpleQueryBuilder,

    /// The scalar expression for each item in the collection.
    item: ScalarExpression
}



#[derive(Clone)]
enum FormulaWrapper<'a> {
    TopLevel(&'a TopLevelQueryBuilder),
    Inline(&'a InlineQueryBuilder)
}



impl Formula {
    /// Converts the formula to a scalar SQL expression.
    pub fn compile(self, wrapper: FormulaWrapper) -> Result<ScalarExpression, Error> {
        Ok(match self {
            Formula::Null => ScalarExpression { 
                arg_expr: String::from("NULL"), 
                value_expr: String::from("NULL"), 
                label_expr: String::from("NULL"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Null 
            },
            Formula::LiteralInt(num) => ScalarExpression { 
                arg_expr: format!("{num}"), 
                value_expr: format!("{num}"), 
                label_expr: format!("CAST({num} AS TEXT)"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Integer 
            },
            Formula::LiteralFloat(num) => ScalarExpression { 
                arg_expr: format!("{num}"), 
                value_expr: format!("{num}"), 
                label_expr: format!("CAST({num} AS TEXT)"),
                param_expr: String::from("NULL"),
                return_type: ScalarType::Number 
            },
            Formula::LiteralBool(b) => ScalarExpression { 
                arg_expr: format!("{}", if b { "TRUE" } else { "FALSE" }),
                value_expr: format!("{}", if b { "TRUE" } else { "FALSE" }), 
                label_expr: format!("{}", if b { "'True'" } else { "'False'" }),
                param_expr: String::from("NULL"), 
                return_type: ScalarType::Boolean 
            },
            Formula::LiteralString(text) => ScalarExpression { 
                arg_expr: format!("'{}'", text.replace("'", "''")),  
                value_expr: format!("'{}'", text.replace("'", "''")), 
                label_expr: format!("'{}'", text.replace("'", "''")), 
                param_expr: String::from("NULL"), 
                return_type: ScalarType::Text 
            },
            Formula::Param { datasource_oid, column_oid } => {
                let datasource: datasource::Datasource = datasource::Datasource::get(datasource_oid)?;
                let column_metadata: column::Metadata = column::Metadata::get(column_oid)?;

                match &wrapper {
                    FormulaWrapper::TopLevel(query) => {
                        let datasource_alias: String = query.get_datasource_alias(&datasource);
                        match query.insert_param(parameter::Parameter {
                            datasource,
                            column: column_metadata.clone()
                        })? {
                            SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                                let (label_expr, return_type) = match param.column.column_type {
                                    column_type::ColumnType::Primitive(prim) => match prim {
                                        column_type::Primitive::Text => (format!("{datasource_alias}.{value_ord}"), ScalarType::Text),
                                        column_type::Primitive::JSON => (format!("{datasource_alias}.{value_ord}"), ScalarType::JSON),
                                        column_type::Primitive::Integer => (format!("CAST({datasource_alias}.{value_ord} AS TEXT)"), ScalarType::Integer),
                                        column_type::Primitive::Number => (format!("CAST({datasource_alias}.{value_ord} AS TEXT)"), ScalarType::Number),
                                        column_type::Primitive::Checkbox => (format!("IF({datasource_alias}.{value_ord}, 'True', 'False')"), ScalarType::Boolean),
                                        column_type::Primitive::Date => (format!("DATE({datasource_alias}.{value_ord}, 'julianday')"), ScalarType::Date),
                                        column_type::Primitive::Datetime => (format!("STRFTIME('%FT%TZ', {datasource_alias}.{value_ord}, 'julianday')"), ScalarType::Datetime),
                                        column_type::Primitive::File
                                        | column_type::Primitive::Image => (
                                            format!("
                                            CASE 
                                                WHEN {datasource_alias}.{value_ord} IS NULL THEN NULL 
                                                WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({datasource_alias}.{value_ord}) * 0.000000001)
                                                WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({datasource_alias}.{value_ord}) * 0.000001)
                                                ELSE FORMAT('%.1f KB', LENGTH({datasource_alias}.{value_ord}) * 0.001)
                                            END
                                            "), 
                                            ScalarType::Blob
                                        )
                                    },
                                    _ => { return Err(Error::AdhocError("Expected primitive column type.")); }
                                };
                                ScalarExpression {
                                    arg_expr: format!("{datasource_alias}.{value_ord}"),
                                    value_expr: format!("{datasource_alias}.{value_ord}"),
                                    label_expr,
                                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                    return_type
                                }
                            },
                            SimpleQueryBuilderColumn::Select { param, value_ord } => {
                                let referenced_table_oid: i64 = 0;
                                ScalarExpression {
                                    arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = {datasource_alias}.{value_ord})"),
                                    value_expr: format!("{datasource_alias}.{value_ord}"),
                                    label_expr: format!("(SELECT LABEL FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = {datasource_alias}.{value_ord})"),
                                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                    return_type: ScalarType::JSON
                                }
                            },
                            SimpleQueryBuilderColumn::Multiselect { param, value_ord, label_ord } => {
                                let referenced_table_oid: i64 = 0;
                                ScalarExpression {
                                    arg_expr: format!("{datasource_alias}.{label_ord}"),
                                    value_expr: format!("{datasource_alias}.{value_ord}"),
                                    label_expr: format!("{datasource_alias}.{label_ord}"),
                                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                    return_type: ScalarType::JSON
                                }
                            },
                            SimpleQueryBuilderColumn::Object { param, value_ord, label_ord } => {
                                let referenced_table_oid: i64 = 0;
                                ScalarExpression {
                                    arg_expr: format!("(SELECT JSON_STRINGIFY FROM TABLE{referenced_table_oid}_SURROGATE WHERE OID = p.{value_ord})"),
                                    value_expr: format!("{datasource_alias}.{value_ord}"),
                                    label_expr: format!("{datasource_alias}.{label_ord}"),
                                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                    return_type: ScalarType::JSON
                                }
                            }
                            SimpleQueryBuilderColumn::Formula { param, formula } => {
                                let parsed_formula: Formula = Formula::parse(formula.clone())?;
                                let inner_expression: ScalarExpression = parsed_formula.compile(wrapper)?;
                                ScalarExpression {
                                    arg_expr: format!("({})", inner_expression.arg_expr),
                                    value_expr: format!("({})", inner_expression.value_expr),
                                    label_expr: format!("({})", inner_expression.label_expr),
                                    param_expr: format!("({})", inner_expression.param_expr),
                                    return_type: inner_expression.return_type
                                }
                            }
                            SimpleQueryBuilderColumn::Virtual { param } => {
                                return Err(Error::AdhocError("A subreport cannot be used as a parameter to a formula!"));
                            }
                        }
                    },
                    FormulaWrapper::Inline(query) => {
                        let datasource_alias: String = query.get_datasource_alias(&datasource);
                        match &column_metadata.column_type {
                            column_type::ColumnType::Primitive(prim) => {
                                let (label_expr, return_type) = match param.column.column_type {
                                    column_type::ColumnType::Primitive(prim) => match prim {
                                        column_type::Primitive::Text => (format!("{datasource_alias}.{value_ord}"), ScalarType::Text),
                                        column_type::Primitive::JSON => (format!("{datasource_alias}.{value_ord}"), ScalarType::JSON),
                                        column_type::Primitive::Integer => (format!("CAST({datasource_alias}.{value_ord} AS TEXT)"), ScalarType::Integer),
                                        column_type::Primitive::Number => (format!("CAST({datasource_alias}.{value_ord} AS TEXT)"), ScalarType::Number),
                                        column_type::Primitive::Checkbox => (format!("IF({datasource_alias}.{value_ord}, 'True', 'False')"), ScalarType::Boolean),
                                        column_type::Primitive::Date => (format!("DATE({datasource_alias}.{value_ord}, 'julianday')"), ScalarType::Date),
                                        column_type::Primitive::Datetime => (format!("STRFTIME('%FT%TZ', {datasource_alias}.{value_ord}, 'julianday')"), ScalarType::Datetime),
                                        column_type::Primitive::File
                                        | column_type::Primitive::Image => (
                                            format!("
                                            CASE 
                                                WHEN {datasource_alias}.{value_ord} IS NULL THEN NULL 
                                                WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({datasource_alias}.{value_ord}) * 0.000000001)
                                                WHEN LENGTH({datasource_alias}.{value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({datasource_alias}.{value_ord}) * 0.000001)
                                                ELSE FORMAT('%.1f KB', LENGTH({datasource_alias}.{value_ord}) * 0.001)
                                            END
                                            "), 
                                            ScalarType::Blob
                                        )
                                    },
                                    _ => { return Err(Error::AdhocError("Expected primitive column type.")); }
                                };
                                ScalarExpression {
                                    arg_expr: format!("{datasource_alias}.{value_ord}"),
                                    value_expr: format!("{datasource_alias}.{value_ord}"),
                                    label_expr,
                                    param_expr: format!("'{}:{}'", param.datasource.get_oid(), param.column.oid),
                                    return_type
                                }
                            }
                        }
                    }
                }
            },
            Formula::Wrap(inner) => {
                let inner_expression = self.formula_to_scalar_expression(*inner)?;
                ScalarExpression {
                    arg_expr: format!("({})", inner_expression.arg_expr),
                    value_expr: format!("({})", inner_expression.value_expr),
                    label_expr: format!("({})", inner_expression.label_expr),
                    param_expr: format!("({})", inner_expression.param_expr),
                    return_type: inner_expression.return_type
                }
            },
            Formula::Or(lhs, rhs) => {
                let lhs_expression = self.formula_to_scalar_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_scalar_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "or(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("{} OR {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} OR {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} OR {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::And(lhs, rhs) => {
                let lhs_expression = self.formula_to_scalar_expression(*lhs)?;
                if !ScalarType::Boolean.contains(lhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(lhs, _)",
                        inner_name: lhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: lhs_expression.return_type.to_string()
                    });
                }

                let rhs_expression = self.formula_to_scalar_expression(*rhs)?;
                if !ScalarType::Boolean.contains(rhs_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "and(_, rhs)",
                        inner_name: rhs.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: rhs_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("{} AND {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    value_expr: format!("{} AND {}", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    label_expr: format!("IF({} AND {}, 'True', 'False')", lhs_expression.arg_expr, rhs_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
            Formula::Not(inner) => {
                let inner_expression = self.formula_to_scalar_expression(*inner)?;
                if !ScalarType::Boolean.contains(inner_expression.return_type) {
                    return Err(Error::FormulaTypeValidationError {
                        outer_name: "not",
                        inner_name: inner.to_string(),
                        expected_type: ScalarType::Boolean.to_string(),
                        received_type: inner_expression.return_type.to_string()
                    });
                }

                ScalarExpression {
                    arg_expr: format!("NOT ({})", inner_expression.arg_expr),
                    value_expr: format!("NOT ({})", inner_expression.arg_expr),
                    label_expr: format!("IF({}, 'False', 'True')", inner_expression.arg_expr),
                    param_expr: String::from("NULL"),
                    return_type: ScalarType::Boolean
                }
            },
        })
    }
}