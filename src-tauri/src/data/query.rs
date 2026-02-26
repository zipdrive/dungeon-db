use rusqlite::{Connection, Transaction, params};
use crate::data::{column, column_type, datasource, parameter, table};
use crate::util::db;
use crate::util::error::Error;
use std::collections::HashMap;



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
                self.datasource.hash(state)
            }
        }
    }
}

impl Borrow<datasource::Datasource> for Join {
    fn borrow(&self) -> &datasource::Datasource {
        match self {
            Self::Root(datasource)
            | Self::Precompiled { datasource, .. } => {
                &self.datasource
            }
        }
    }
}



struct QueryParameter {
    /// The parameter.
    param: parameter::Parameter,

    /// The ordinal of the display value.
    /// This value is always of type TEXT.
    display_value_ord: Option<String>,

    /// The ordinal of the true value.
    /// This value can be of any type.
    true_value_ord: String
}

impl Hash for QueryParameter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.param.hash(state)
    }
}

impl Borrow<parameter::Parameter> for QueryParameter {
    fn borrow(&self) -> &parameter::Parameter {
        &self.param
    }
}




#[derive(Clone)]
enum Relationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
    }
}



struct SimpleQueryBuilder {
    /// The datasources for the query.
    datasources: HashSet<Join>,

    /// The parameters selected by the query.
    params: HashSet<QueryParameter>,

    /// The column definitions.
    cmd_cols: Vec<String>
}

impl SimpleQueryBuilder {
    /// Creates a new simple statement.
    fn new(datasources: Vec<datasource::Datasource>) -> Self {
        if datasources.len() == 0 {
            return Err(Error::AdhocError("Cannot create a query with no root datasources!"));
        }

        // Construct empty query builder
        let mut query_builder: Self = Self {
            datasources: HashSet::new(),
            params: HashSet::new(),
            cmd_cols: Vec::new()
        };
        // Add each datasource to the query builder
        for datasource in datasources.iter() {
            query_builder.insert_datasource(datasource);
        }
        return query_builder;
    }

    fn compile(&self) -> String {
        format!("SELECT {}{} FROM {} {}",
            // Raw columns
            self.cmd_cols,
            // Parameter columns
            self.params.iter()
                .map(|param| {
                    match param.column.column_type {

                    }
                })
                .fold(String::from(""), |acc, e| format!("{acc}, {e}")),
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
            self.datasources.iter()
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
    fn insert_col(&mut self, col_definition: String) {
        self.cmd_cols.push(col_definition);
    }

    /// Add a datasource to the query builder.
    fn insert_datasource(&mut self, conn: &Connection, datasource: datasource::Datasource) -> Result<(), Error> {
        // First, make sure an existing datasource is not being duplicated
        if self.datasources.contains(&datasource) {
            return Ok(())
        }

        let datasource_alias: String = format!("d{}", datasource.get_oid());
        match datasource {
            datasource::Datasource::Table { .. } => {
                self.datasources.insert(Join::Root(datasource));
            },
            datasource::Datasource::Inheritance { parent_datasource, table, .. } => {
                // Check whether the datasource is inheriting from or inherited by the parent datasource
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    datasource,
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
                    }
                });
            },
            datasource::Datasource::Object { parent_datasource, column, .. } 
            | datasource::Datasource::Select { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    datasource,
                    join_clause: if column.schema.oid == parent_schema_metadata.oid {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {parent_datasource_alias}.COLUMN{} = {datasource_alias}.OID", schema_metadata.oid, column.oid)
                    } else {
                        format!("LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}.COLUMN{} = {parent_datasource_alias}.OID", schema_metadata.oid, column.oid)
                    }
                });
            },
            datasource::Datasource::Multiselect { parent_datasource, column, .. } => {
                let schema_metadata: schema::Metadata = datasource.get_schema();
                let parent_datasource_alias: String = format!("d{}", parent_datasource.get_oid());
                let parent_schema_metadata: schema::Metadata = parent_datasource.get_schema();
                self.datasources.insert(Join::Precompiled {
                    datasource,
                    join_clause: format!("
                        LEFT JOIN MULTISELECT{} {datasource_alias}m ON {datasource_alias}m.TABLE{}_OID = {parent_datasource_alias}.OID
                        LEFT JOIN TABLE{} {datasource_alias} ON {datasource_alias}m.TABLE{}_OID = {datasource_alias}.OID
                        ", 
                        column.oid,
                        parent_schema_metadata.oid,
                        schema_metadata.oid, 
                        schema_metadata.oid
                    )
                });
            }
        }

        insert_col(format!("{datasource_alias}.OID AS {datasource_alias}_OID"));
    }
    
    /// Add a parameter selected by the query.
    fn insert_param(&mut self, param: parameter::Parameter) -> Result<(), Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.params.contains(&param) {
            return Ok(());
        }

        // Make sure the datasource is in the query
        self.insert_datasource(param.datasource)?;

        // Add a column for the parameter, and record the true vs display ordinals
        let source_alias: String = format!("d{}", param.datasource.oid);
        let column_oid: i64 = param.column.oid;
        let column_alias: String = format!("{source_alias}COLUMN{column_oid}");

        let (true_value_ord, display_value_ord) = match param.column.column_type {
            column_type::ColumnType::Primitive(_) => {
                self.insert_col(format!("{source_alias}.COLUMN{column_oid} AS {column_alias}"));
                (column_alias, None)
            },
            column_type::ColumnType::Object { oid, table_oid } 
            | column_type::ColumnType::Select { oid, table_oid } => {
                self.insert_col(format!(
                    "CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS {column_alias}"
                ));
                self.insert_col(format!(
                    "(SELECT LABEL FROM TABLE{table_oid}_SURROGATE WHERE OID = {source_alias}.COLUMN{column_oid}) AS {column_alias}_LABEL"
                ));
                (column_alias, Some(format!("{column_alias}_LABEL")))
            },
            column_type::ColumnType::Multiselect { oid, table_oid } => {
                let datasource_schema_metadata: schema::Metadata = param.datasource.get_schema();
                self.insert_col(format!(
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
                self.insert_col(format!(
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
                (column_alias, Some(format!("{column_alias}_LABEL")))
            },
            column_type::ColumnType::Formula { oid, .. } => {
                // Select the value of the formula from the formula view
                self.insert_col(format!("(SELECT VALUE FROM FORMULA{oid} WHERE OID = {source_alias}.OID) AS {column_alias}"));

                // Return the display value as the true value alias
                (column_alias, None) 
            },
            column_type::ColumnType::Subreport { oid, report_oid } => {
                self.insert_col(format!("'oid={report_oid}' AS {column_alias}"));
                (column_alias, None)
            }
        };
        self.params.insert(QueryParameter {
            param,
            true_value_ord,
            display_value_ord
        });
        return Ok(());
    }
}

struct QueryBuilder {
    /// CTE to select the parameters
    param_cte: SimpleQueryBuilder,

    /// The columns selected for the final query
    cmd_cols: String,
}

impl QueryBuilder {
    /// Creates a new query.
    fn new(datasources: Vec<datasource::Datasource>) -> Self {
        
    }

    /// Compiles the final query statement.
    fn compile(&self) -> String {
        format!(
            "WITH PARAM_CTE AS ({}) SELECT {} FROM PARAM_CTE p {}",
            self.param_cte.compile(),
            self.cmd_cols,
            String::from("")
        )
    }
}