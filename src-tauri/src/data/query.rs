use rusqlite::{Connection, Transaction, params};
use crate::data::{column, column_type, datasource, parameter, schema, table};
use crate::util::formula::Formula;
use crate::util::db;
use crate::util::error::Error;
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::collections::{HashSet,HashMap};



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



pub enum QueryBuilderColumn {
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

    fn compile(self) -> String {
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

    /// Add a raw column definition to the query builder.
    fn insert_col_definition(&mut self, col_definition: String) {
        self.cmd_cols.push(col_definition);
    }

    /// Add a datasource to the query builder.
    fn insert_datasource(&mut self, datasource: datasource::Datasource) {
        // First, make sure an existing datasource is not being duplicated
        if self.datasources.contains(&datasource) {
            return;
        }

        let datasource_alias: String = format!("d{}", datasource.get_oid());
        match &datasource {
            datasource::Datasource::Table { .. } => {
                self.datasources.insert(Join::Root(datasource));
            },
            datasource::Datasource::Inheritance { parent_datasource, table, .. } => {
                // Check whether the datasource is inheriting from or inherited by the parent datasource
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
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
                    datasource
                });
            },
            datasource::Datasource::Object { parent_datasource, column, .. } 
            | datasource::Datasource::Select { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    join_clause: if column.schema.oid == parent_schema_metadata.oid {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {parent_datasource_alias}.COLUMN{} = {datasource_alias}.OID", schema_metadata.oid, column.oid)
                    } else {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.COLUMN{} = {parent_datasource_alias}.OID", schema_metadata.oid, column.oid)
                    },
                    datasource
                });
            },
            datasource::Datasource::Multiselect { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
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
    
    /// Add a parameter selected by the query.
    fn insert_param(&mut self, param: parameter::Parameter) -> Result<&SimpleQueryBuilderColumn, Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.params.contains(&param) {
            return Ok(self.params.get(&param).expect(""));
        }

        // Make sure the datasource is in the query
        self.insert_datasource(param.datasource.clone());

        // Add a column for the parameter, and record the true vs display ordinals
        let source_alias: String = format!("d{}", param.datasource.get_oid());
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



pub struct QueryBuilder {
    /// CTE to select the parameters
    param_cte: SimpleQueryBuilder,

    /// The columns selected for the final query
    cmd_cols: HashSet<String>,

    /// Expressions used to group the rows in the final query
    cmd_groupby: Vec<String>,

    /// Expressions used to order the rows in the final query
    cmd_orderby: Vec<String>
}

impl QueryBuilder {
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

    /// Compiles the final query statement.
    pub fn compile(mut self) -> String {
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

    /// Add a raw column definition to the query builder.
    fn insert_col_definition(&mut self, col_definition: String) {
        self.cmd_cols.insert(col_definition);
    }

    fn formula_to_expression(&self, formula: Formula) -> Result<String, Error> {
        Ok(match formula {
            Formula::Wrap(inner) => format!("({})", self.formula_to_expression(*inner)),
            Formula::Null => String::from("NULL"),
            Formula::LiteralInt(num) => format!("{num}"),
            Formula::LiteralFloat(num) => format!("{num}"),
            Formula::LiteralBool(b) => format!("{}", if b { "TRUE" } else { "FALSE" }),
            Formula::LiteralString(text) => format!("'{}'", text.replace("'", "''")),
            Formula::Param { datasource_oid, column_oid } => {
                let datasource: datasource::Datasource = datasource::Datasource::get(datasource_oid)?;
                format!("{}+{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs))
            },
            Formula::Or(lhs, rhs) => format!("{} OR {}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::And(lhs, rhs) => format!("{} AND {}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Not(inner) => format!("NOT ({})", self.formula_to_expression(*inner)),
            Formula::Add(lhs, rhs) => format!("{}+{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Subtract(lhs, rhs) => format!("{}-{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Multiply(lhs, rhs) => format!("{}*{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Divide(lhs, rhs) => format!("{}/{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Modulo(lhs, rhs) => format!("{}%{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs)),
            Formula::Concat(lhs, rhs) => format!("{}||{}", self.formula_to_expression(*lhs), self.formula_to_expression(*rhs))
        })
    }

    /// Inserts a column associated with a datasource.
    pub fn column(&mut self, column: parameter::Parameter) -> Result<QueryBuilderColumn, Error> {
        let qparam: SimpleQueryBuilderColumn = self.param_cte.insert_param(column)?.clone();
        match qparam {
            SimpleQueryBuilderColumn::Primitive { param, value_ord } => {
                match &param.column.column_type {
                    column_type::ColumnType::Primitive(prim) => {
                        match prim {
                            column_type::Primitive::Text
                            | column_type::Primitive::JSON => {
                                self.insert_col_definition(format!("{value_ord} AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Integer
                            | column_type::Primitive::Number
                            | column_type::Primitive::Checkbox => {
                                // Cast number to text
                                self.insert_col_definition(format!("CAST({value_ord} AS TEXT) AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Date => {
                                // Cast date to UTF Date string
                                self.insert_col_definition(format!("DATE({value_ord}, 'julianday') AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::Datetime => {
                                // Cast datetime to UTF Datetime string
                                self.insert_col_definition(format!("STRFTIME('%FT%TZ', {value_ord}, 'julianday') AS {value_ord}_LABEL"));
                            }
                            column_type::Primitive::File
                            | column_type::Primitive::Image => {
                                // Label is size of file
                                self.insert_col_definition(format!("
                                CASE 
                                    WHEN {value_ord} IS NULL THEN NULL 
                                    WHEN LENGTH({value_ord}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({value_ord}) * 0.000000001)
                                    WHEN LENGTH({value_ord}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({value_ord}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({value_ord}) * 0.001)
                                END AS {value_ord}_LABEL
                                "));
                            }
                        }
                        return Ok(QueryBuilderColumn::Primitive {
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
                self.insert_col_definition(value_ord.clone());
                self.insert_col_definition(label_ord.clone());
                return Ok(QueryBuilderColumn::Object { 
                    column: param.column, 
                    value_ord, 
                    label_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Select { param, value_ord } => {
                self.insert_col_definition(value_ord.clone());
                return Ok(QueryBuilderColumn::Select { 
                    column: param.column, 
                    value_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Multiselect { param, value_ord, label_ord } => {
                self.insert_col_definition(value_ord.clone());
                self.insert_col_definition(label_ord.clone());
                return Ok(QueryBuilderColumn::Multiselect { 
                    column: param.column, 
                    value_ord, 
                    label_ord,
                    row_ord: format!("d{}_OID", param.datasource.get_oid())
                });
            }
            SimpleQueryBuilderColumn::Formula { param, formula } => {
                todo!("Parse formula into column expression");
            }
            SimpleQueryBuilderColumn::Virtual { param } => {
                // Do not insert anything for subreports, which are a virtual column
                match param.column.column_type {
                    column_type::ColumnType::Subreport { report_oid, .. } => {
                        return Ok(QueryBuilderColumn::Subreport { 
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