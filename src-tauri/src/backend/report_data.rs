use crate::backend::{data_type, db};
use crate::util::{error, formula::Formula};
use rusqlite::{params, named_params, Transaction};
use serde::Serialize;
use std::collections::{HashMap};

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Cell {
    RowStart {
        row_oid: i64,
        row_index: i64,
    },
    ColumnValue {
        column_oid: i64,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    Subreport {
        subreport_oid: i64,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists(bool),
    ColumnValue {
        column_oid: i64,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    ReadOnlyValue {
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>,
    },
    Subreport {
        subreport_oid: i64,
    },
}

struct ColumnParam {
    /// The count of rows for this param.
    count: TableParamRelationship,

    /// The OID of the table where this param comes from.
    table_oid: i64,

    /// The OID of the column where this param comes from.
    column_oid: i64,

    /// The ordinal that can be pulled to determine the OID of the row where this param comes from.
    row_ord: String,

    /// The type of the column.
    column_type: data_type::MetadataColumnType,

    /// The ordinal that can be pulled to get the true value of the column.
    true_ord: Option<String>
}

#[derive(Clone)]
enum TableParamRelationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
    }
}

enum Column {
    Formula {
        /// The OID of the report column.
        column_oid: i64,

        /// The name of the report column.
        column_name: String,

        /// Ordinal to retrieve a frontend displayed value of the cell.
        display_ord: String,

        /// Ordinal to retrieve a backend value of the cell.
        true_ord: Option<String>,

        /// Ordinal to retrieve the table OID of the cell.
        table_oid_ord: String,

        /// Ordinal to retrieve the row OID of the cell.
        row_oid_ord: String,

        /// Ordinal to retrieve the column OID of the cell.
        column_oid_ord: String,
    },
    Subreport {
        /// The OID of the report column.
        column_oid: i64,

        /// The name of the report column.
        column_name: String,

        /// The OID of the subreport.
        subreport_oid: i64,
    },
}


struct SelectParamsStatement {
    base_table_oid: i64,

    /// Stores the report parameters that are selected as a column in this statement.
    param_cols: HashMap<i64, ColumnParam>,
    /// Stores the report parameters that are joined to the query via LEFT JOIN clauses.
    param_tbls: HashMap<i64, TableParamRelationship>,

    /// The portion of the statement after "SELECT.." and before the FROM clause defined by cmd_tbls
    cmd_cols: String,

    /// The portion of the statement defining the FROM and JOIN clauses, after cmd_cols
    cmd_tbls: String
}

impl SelectParamsStatement {
    fn new(base_table_oid: i64) -> Self {
        Self {
            base_table_oid,
            cmd_cols: String::from("t.OID AS t_OID"),
            param_cols: HashMap::new(),
            cmd_tbls: format!("FROM TABLE{base_table_oid} t"),
            param_tbls: HashMap::new()
        }
    }

    /// Compiles the statement into executable SQL.
    fn compile(&self) -> String {
        format!("SELECT {} {}", self.cmd_cols, self.cmd_tbls)
    }

    /// Add a parameter column to the query.
    fn insert_col(&mut self, col_definition: String) {
        self.cmd_cols = format!("{}, {col_definition}", self.cmd_cols);
    }

    /// Add a joined table to the params query.
    fn insert_join(&mut self, tbl_definition: String) {
        self.cmd_tbls = format!("{} {tbl_definition}", self.cmd_tbls);
    }

    fn insert_param(&mut self, param_oid: i64) -> Result<(), error::Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // First, check to make sure the parameter hasn't already been added
        if self.param_cols.contains_key(&param_oid) {
            return Ok(());
        }

        // Then, make sure to add any parameter it is dependent on
        let (
            table_oid,
            column_oid,
            column_type_oid, 
            column_mode, 
            dependency_param_oid
        ) = trans.query_one(
            "
            WITH COLUMN_QUERY (RPT_PARAMETER_OID, COLUMN_OID, TABLE_OID, TYPE_OID, MODE, DEPENDENCY_RPT_PARAMETER_OID) AS (
                -- Links through a column in the base table
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.RPT_PARAMETER_OID AS COLUMN_OID,
                    c.TABLE_OID,
                    c.TYPE_OID,
                    typ.MODE,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID
                FROM METADATA_TABLE_COLUMN c
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID

                UNION

                -- Chained link that terminates in a column
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.RPT_PARAMETER_OID AS COLUMN_OID,
                    c.TABLE_OID,
                    c.TYPE_OID,
                    typ.MODE,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID 
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID 
            )
            SELECT
                TABLE_OID,
                COLUMN_OID, 
                TYPE_OID, 
                MODE, 
                DEPENDENCY_RPT_PARAMETER_OID
            FROM COLUMN_QUERY
            WHERE RPT_PARAMETER_OID = ?1
            ",
            params![param_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("TABLE_OID")?,
                    row.get::<_, i64>("COLUMN_OID")?,
                    row.get::<_, i64>("TYPE_OID")?,
                    row.get::<_, i64>("MODE")?,
                    row.get::<_, Option<i64>>("DEPENDENCY_RPT_PARAMETER_OID")?
                ))
            }
        )?;
        
        // Make sure any parameter this one is dependent on is added to the query
        let relationship: TableParamRelationship;
        let source_alias: String = if let Some(o) = dependency_param_oid {
            relationship = self.insert_join_param(&trans, o)?;
            format!("p{o}")
        } else if table_oid == self.base_table_oid {
            relationship = TableParamRelationship::One;
            String::from("t")
        } else {
            return Err(error::Error::AdhocError("A report parameter does not source back to the base table."));
        };

        // Get the column type
        let column_type = data_type::MetadataColumnType::from_database(column_type_oid, column_mode);

        // Construct the query used to retrieve the parameter data
        match &column_type {
            data_type::MetadataColumnType::Primitive(prim) => {
                match prim {
                    data_type::Primitive::Any
                    | data_type::Primitive::Boolean
                    | data_type::Primitive::Integer
                    | data_type::Primitive::Number
                    | data_type::Primitive::Text
                    | data_type::Primitive::JSON => {
                        self.insert_col(
                            format!("CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Date => {
                        self.insert_col(
                            format!("
                            DATE({source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Timestamp => {
                        self.insert_col(
                            format!("STRFTIME('%FT%TZ', {source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::File => {
                        self.insert_col(
                            format!("
                            CASE 
                            WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL 
                            ELSE 
                                CASE 
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000000001)
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.001)
                                END
                            END AS PARAM{param_oid}
                            ")
                        );
                    }
                    data_type::Primitive::Image => {
                        self.insert_col(
                            format!("CASE WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS PARAM{param_oid}")
                        );
                    }
                }

                self.param_cols.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::SingleSelectDropdown(_) => {
                self.insert_col(format!("p{param_oid}.VALUE AS PARAM{param_oid}"));
                self.insert_col(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_join(format!("LEFT JOIN TABLE{column_type_oid} p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                self.param_cols.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("_PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::MultiSelectDropdown(_) => {
                self.insert_col(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(b.VALUE) || ']' 
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS PARAM{param_oid}")
                );
                self.insert_col(
                    format!("(
                    SELECT 
                        GROUP_CONCAT(CAST(b.OID AS TEXT))
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS _PARAM{param_oid}")
                );
                
                self.param_cols.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("_PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::Reference(_)
            | data_type::MetadataColumnType::ChildObject(_) => {
                self.insert_col(format!("p{param_oid}.DISPLAY_VALUE AS PARAM{param_oid}"));
                self.insert_col(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_join(format!("LEFT JOIN TABLE{column_type_oid}_SURROGATE p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                self.param_cols.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("_PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::ChildTable(_) => {
                self.insert_col(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' 
                    FROM TABLE{column_type_oid}_SURROGATE a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.OID 
                    WHERE b.PARENT_OID = {source_alias}.OID 
                    GROUP BY b.PARENT_OID
                    ) AS PARAM{param_oid}")
                );

                self.param_cols.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: None,
                    count: relationship
                });
            }
        }

        return Ok(());
    }

    fn insert_join_param(&mut self, trans: &Transaction, param_oid: i64) -> Result<TableParamRelationship, error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.param_tbls.contains_key(&param_oid) {
            return Ok(self.param_tbls[&param_oid].clone());
        }

        // Then, make sure to add any parameter it is dependent on
        let (table_oid, dependency_param_oid, join_statement, is_many) = trans.query_one(
            "
            WITH RECURSIVE JOIN_STATEMENTS (RPT_PARAMETER_OID, TABLE_OID, DEPENDENCY_RPT_PARAMETER_OID, JOIN_STATEMENT, IS_MANY) AS (
                -- Links through a column in the base table
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.TYPE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TYPE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' ON ' || 
                        CASE 
                            WHEN typ.MODE = 5 THEN 'p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.PARENT_OID = t.OID'
                            ELSE 't.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.OID'
                        END
                    AS JOIN_STATEMENT,
                    CASE WHEN typ.MODE = 5 THEN 1 ELSE 0 END AS IS_MANY
                FROM METADATA_TABLE_COLUMN c
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID
                WHERE c.TABLE_OID = :base_table_oid AND typ.MODE IN (3,4,5)

                UNION

                -- Links through a reference to the base table
                SELECT
                    c.RPT_PARAMETER_OID,
                    c.TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TABLE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = t.OID'
                    AS JOIN_STATEMENT,
                    1 AS IS_MANY
                FROM METADATA_TABLE_COLUMN c
                WHERE c.TYPE_OID = :base_table_oid

                UNION 

                -- Links through inheritance from base table
                SELECT
                    inh.RPT_PARAMETER_OID,
                    inh.INHERITOR_TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.INHERITOR_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_TABLE_INHERITANCE inh
                WHERE inh.MASTER_TABLE_OID = :base_table_oid

                UNION 

                -- Links through inheritance by base table
                SELECT
                    inh.RPT_PARAMETER_OID,
                    inh.MASTER_TABLE_OID AS TABLE_OID,
                    NULL AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.MASTER_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.OID = t.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_TABLE_INHERITANCE inh
                WHERE inh.INHERITOR_TABLE_OID = :base_table_oid

                UNION

                -- Chained link that terminates in a column
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.TYPE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TYPE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' ON ' || 
                        CASE 
                            WHEN typ.MODE = 5 THEN 'p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.PARENT_OID = t.OID'
                            ELSE 't.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.OID'
                        END
                    AS JOIN_STATEMENT,
                    CASE WHEN typ.MODE = 5 THEN 1 ELSE 0 END AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID AND c.TABLE_OID = j.TABLE_OID
                INNER JOIN METADATA_TYPE typ ON typ.OID = c.TYPE_OID AND typ.MODE IN (3,4,5)

                UNION

                -- Chained link that terminates in the table being referenced by another
                SELECT
                    ch.RPT_PARAMETER_OID,
                    c.TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', c.TABLE_OID) || ' p' || FORMAT('%d', c.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', c.RPT_PARAMETER_OID) || '.COLUMN' || FORMAT('%d', c.RPT_PARAMETER_OID) || ' = t.OID'
                    AS JOIN_STATEMENT,
                    1 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = ch.DEF_RPT_PARAMETER_OID AND c.TYPE_OID = j.TABLE_OID

                UNION 

                -- Chained link that terminates in inheritance from the table
                SELECT
                    ch.RPT_PARAMETER_OID,
                    inh.INHERITOR_TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.INHERITOR_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_INHERITANCE inh ON inh.MASTER_TABLE_OID = j.TABLE_OID

                UNION 

                -- Chained link that terminates in inheritance by the table
                SELECT
                    ch.RPT_PARAMETER_OID,
                    inh.MASTER_TABLE_OID AS TABLE_OID,
                    ch.REF_RPT_PARAMETER_OID AS DEPENDENCY_RPT_PARAMETER_OID,
                    'LEFT JOIN TABLE' || FORMAT('%d', inh.MASTER_TABLE_OID) || ' p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || 
                        ' ON p' || FORMAT('%d', inh.RPT_PARAMETER_OID) || '.OID = t.MASTER' || FORMAT('%d', inh.MASTER_TABLE_OID) || '_OID = t.OID'
                    AS JOIN_STATEMENT,
                    0 AS IS_MANY
                FROM METADATA_RPT_PARAMETER__CHAIN ch
                INNER JOIN JOIN_STATEMENTS j ON j.RPT_PARAMETER_OID = ch.REF_RPT_PARAMETER_OID
                INNER JOIN METADATA_TABLE_INHERITANCE inh ON inh.INHERITOR_TABLE_OID = j.TABLE_OID
            )
            SELECT
                TABLE_OID,
                DEPENDENCY_RPT_PARAMETER_OID,
                JOIN_STATEMENT,
                IS_MANY
            FROM JOIN_STATEMENTS
            WHERE RPT_PARAMETER_OID = :rpt_param_oid
            ",
            named_params! { ":base_table_oid": self.base_table_oid, ":rpt_param_oid": param_oid },
            |row| {
                Ok((
                    row.get::<_, i64>("TABLE_OID")?,
                    row.get::<_, Option<i64>>("DEPENDENCY_RPT_PARAMETER_OID")?,
                    row.get::<_, String>("JOIN_STATEMENT")?,
                    row.get::<_, bool>("IS_MANY")?
                ))
            }
        )?;
        
        // Make sure any parameter this one is dependent on is added to the query
        let dependent_relationship: TableParamRelationship = if let Some(o) = dependency_param_oid {
            self.insert_join_param(trans, o)?
        } else {
            TableParamRelationship::One
        };
        let this_relationship: TableParamRelationship = if is_many {
            match dependent_relationship {
                TableParamRelationship::One => {
                    TableParamRelationship::Many {
                        intermediate_param_oid: Vec::new(),
                        final_param_oid: param_oid.clone()
                    }
                }
                TableParamRelationship::Many { mut intermediate_param_oid, final_param_oid } => {
                    intermediate_param_oid.push(final_param_oid);
                    TableParamRelationship::Many { 
                        intermediate_param_oid, 
                        final_param_oid: param_oid.clone()
                    }
                }
            }
        } else {
            dependent_relationship  
        };

        // Add the join statement
        self.insert_join(join_statement);

        // Add a constant to indicate the parameter's associated table
        self.insert_col(format!("{table_oid} AS r{param_oid}_TABLE_OID"));
        // Add a column for the OID of the parameter's associated row OID
        self.insert_col(format!("r{param_oid}.OID AS r{param_oid}_OID"));

        // Add the parameter OID to the list of table parameter OIDs, so no duplicate statements are added
        self.param_tbls.insert(param_oid, this_relationship.clone());
        return Ok(this_relationship);
    }
}


struct ReportQuery {
    base_table_oid: i64,

    param_cte: SelectParamsStatement,
    
    cmd_cols: String,
    columns: Vec<Column>,
}

impl ReportQuery {
    fn new(base_table_oid: i64) -> ReportQuery {
        ReportQuery {
            base_table_oid,
            param_cte: SelectParamsStatement::new(base_table_oid),
            cmd_cols: String::from("t_OID"),
            columns: Vec::new()
        }
    }

    /// Compiles the query into an SQL statement.
    fn compile(&self) -> String {
        format!(
            "
            WITH PARAM_QUERY AS ({})
            SELECT {} FROM PARAM_QUERY
            ",
            self.param_cte.compile(),
            self.cmd_cols
        )
    }

    /// Add a column to the query.
    fn insert_column(&mut self, col_definition: String) {
        self.cmd_cols = format!("{}, {col_definition}", self.cmd_cols);
    }

    /// Add a formula to the query as a column with an alias.
    fn insert_formula_column(&mut self, trans: &Transaction, alias: String, formula: String) -> Result<(), error::Error> {
        // Parse the formula
        let parsed_formula: Formula = Formula::parse(formula)?;

        // Validate the formula
        todo!("Formula validation has not been implemented!");

        // Turn the formula into a column expression
        // TODO
        return Ok(());
    }
}

struct ExpressionWindow {
    filter_formula: Option<Formula>,
    partition_formulae: Vec<Formula>,
    sort_formulae: Vec<Formula>
}

impl ExpressionWindow {
    /// Creates a blank window.
    fn blank() -> Self {
        Self {
            filter_formula: None,
            partition_formulae: Vec::new(),
            sort_formulae: Vec::new()
        }
    }

    fn compile(&self) -> String {
        format!(
            "{} {}",
            if let Some(filter) = self.filter_formula {
                format!(
                    "FILTER (WHERE {})",
                    filter.build_expr(query)
                )
            } else {
                String::from("")
            }
        )
    }
}

struct Expression {
    value_expr: String,
    param_expr: String,
    window: ExpressionWindow
}

impl Formula {
    /// Builds an expression.
    fn build_expr(&self, query: &mut ReportQuery) -> Result<Expression, error::Error> {
        match self {
            Self::Abs(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "ABS({})",
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Add(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} + {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::And(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} AND {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Argmax(arglist) => {
                let mut new_arglist: Vec<Expression> = Vec::new();
                for arg in arglist.iter() {
                    new_arglist.push(arg.build_expr(query)?);
                }

                return Ok(Expression {
                    value_expr: format!(
                        "MAX({})",
                        new_arglist.into_iter().map(|e| e.value_expr).reduce(|acc, expr| format!("{acc}, {expr}")).unwrap()
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Argmin(arglist) => {
                let mut new_arglist: Vec<Expression> = Vec::new();
                for arg in arglist.iter() {
                    new_arglist.push(arg.build_expr(query)?);
                }

                return Ok(Expression {
                    value_expr: format!(
                        "MIN({})",
                        new_arglist.into_iter().map(|e| e.value_expr).reduce(|acc, expr| format!("{acc}, {expr}")).unwrap()
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Average(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "AVG({})",
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Ceiling(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "CEIL({})",
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Coalesce(arglist) => {
                let mut new_arglist: Vec<Expression> = Vec::new();
                for arg in arglist.iter() {
                    new_arglist.push(arg.build_expr(query)?);
                }

                return Ok(Expression {
                    value_expr: format!(
                        "COALESCE({})",
                        new_arglist.iter().map(|e| e.value_expr.clone()).reduce(|acc, expr| format!("{acc}, {expr}")).unwrap()
                    ),
                    param_expr: format!(
                        "CASE {} ELSE {} END",
                        new_arglist.iter().take(new_arglist.len() - 1)
                            .map(|e| format!("WHEN {} IS NOT NULL THEN {}", e.value_expr, e.param_expr))
                            .reduce(|acc, expr| format!("{acc} {expr}"))
                            .unwrap(),
                        new_arglist[new_arglist.len() - 1].param_expr
                    )
                });
            }
            Self::Concat(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} || {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Conditional { condition, formula_if_true, formula_if_false } => {
                let condition_expr = condition.build_expr(query)?;
                let if_true_expr = formula_if_true.build_expr(query)?;
                let if_false_expr = formula_if_false.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "IF({}, {}, {})", 
                        condition_expr.value_expr,
                        if_true_expr.value_expr,
                        if_false_expr.value_expr
                    ),
                    param_expr: format!(
                        "IF({}, {}, {})",
                        condition_expr.value_expr,
                        if_true_expr.param_expr,
                        if_false_expr.param_expr
                    )
                });
            }
            Self::Count(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "COUNT({})",
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Divide(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} / {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Eq(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} IS {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Exponent(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "POW({}, {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Floor(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "FLOOR({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Format { format, format_params } => {
                let format_expr = format.build_expr(query)?;
                if format_params.len() > 0 {
                    let mut format_param_expr: Vec<Expression> = Vec::new();
                    for arg in format_params.iter() {
                        format_param_expr.push(arg.build_expr(query)?);
                    }

                    return Ok(Expression {
                        value_expr: format!(
                            "FORMAT({}, {})",
                            format_expr.value_expr,
                            format_param_expr.into_iter().map(|e| e.value_expr).reduce(|acc, expr| format!("{acc}, {expr}")).unwrap()
                        ),
                        param_expr: String::from("NULL")
                    });
                } else {
                    return Ok(Expression {
                        value_expr: format!(
                            "FORMAT({})",
                            format_expr.value_expr
                        ),
                        param_expr: String::from("NULL")
                    });
                }
            }
            Self::Glob { str, pattern } => {
                let arg0_expr = pattern.build_expr(query)?;
                let arg1_expr = str.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "GLOB({}, {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::In { value, collection } => {
                let arg0_expr = value.build_expr(query)?;
                let arg1_expr = collection.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} IN {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Join { collection, delimiter } => {
                let arg0_expr = collection.build_expr(query)?;
                let arg1_expr = delimiter.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "GROUP_CONCAT({}, {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Length(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "LENGTH({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::LessThan(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} < {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::LessThanOrEq(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} <= {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::LiteralArray(arglist) => {
                todo!("Literal arrays are not implemented!");
            }
            Self::LiteralBool(b) => {
                return Ok(Expression {
                    value_expr: if *b { String::from("TRUE") } else { String::from("FALSE") },
                    param_expr: String::from("NULL")
                });
            }
            Self::LiteralFloat(num) => {
                return Ok(Expression {
                    value_expr: format!("{num}"),
                    param_expr: String::from("NULL")
                });
            }
            Self::LiteralInt(num) => {
                return Ok(Expression {
                    value_expr: format!("{num}"),
                    param_expr: String::from("NULL")
                });
            }
            Self::LiteralString(text) => {
                return Ok(Expression {
                    value_expr: format!("'{}'", text.replace("'", "''")),
                    param_expr: String::from("NULL")
                });
            }
            Self::Lowercase(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "LOWER({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Max(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "MAX({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Min(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "MIN({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Modulo(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} % {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Multiply(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} * {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Not(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "(NOT {})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Null => {
                return Ok(Expression {
                    value_expr: String::from("NULL"),
                    param_expr: String::from("NULL")
                });
            }
            Self::NullIf { value, null_if_match } => {
                let arg0_expr = value.build_expr(query)?;
                let arg1_expr = null_if_match.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "NULLIF({}, {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: format!(
                        "CASE WHEN {} = {} THEN NULL ELSE {} END",
                        arg0_expr.value_expr,
                        arg1_expr.value_expr,
                        arg0_expr.param_expr
                    )
                });
            }
            Self::Or(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} OR {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Param(param_oid) => {
                query.param_cte.insert_param(param_oid.clone())?;
                return Ok(Expression {
                    value_expr: format!("PARAM{param_oid}"),
                    param_expr: format!("{param_oid}")
                });
            }
            Self::RandomInt => {
                return Ok(Expression {
                    value_expr: format!("RANDOM()"),
                    param_expr: String::from("NULL")
                });
            }
            Self::Replace { original, pattern, replacement } => {
                let arg0_expr = original.build_expr(query)?;
                let arg1_expr = pattern.build_expr(query)?;
                let arg2_expr = replacement.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "REPLACE({}, {}, {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr,
                        arg2_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Round(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "ROUND({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Sign(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "SIGN({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Slice { collection, start, length } => {
                todo!("Collection slicing is not implemented!");
            }
            Self::Substring { str, start, length } => {
                let arg0_expr = str.build_expr(query)?;
                let arg1_expr = start.build_expr(query)?;
                match length {
                    Some(length_formula) => {
                        let arg2_expr = length_formula.build_expr(query)?;
                        return Ok(Expression {
                            value_expr: format!(
                                "SUBSTR({}, {}, {})", 
                                arg0_expr.value_expr,
                                arg1_expr.value_expr,
                                arg2_expr.value_expr
                            ),
                            param_expr: String::from("NULL")
                        });
                    }
                    None => {
                        return Ok(Expression {
                            value_expr: format!(
                                "SUBSTR({}, {})", 
                                arg0_expr.value_expr,
                                arg1_expr.value_expr
                            ),
                            param_expr: String::from("NULL")
                        });
                    }
                }
            }
            Self::Subtract(arg0, arg1) => {
                let arg0_expr = arg0.build_expr(query)?;
                let arg1_expr = arg1.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({} - {})", 
                        arg0_expr.value_expr,
                        arg1_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Sum(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "SUM({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Switch { value, matches, formula_if_no_match } => {
                let value_expr = value.build_expr(query)?;
                let return_expr_if_no_match = formula_if_no_match.build_expr(query)?;

                let mut match_param_expr: Vec<Expression> = Vec::new();
                for (formula_match, formula_return) in matches.iter() {
                    let value_match_expr = formula_match.build_expr(query)?;
                    let return_expr = formula_return.build_expr(query)?;
                    match_param_expr.push(Expression { 
                        value_expr: format!(
                            "WHEN {} = {} THEN {}",
                            value_expr.value_expr.clone(),
                            value_match_expr.value_expr.clone(),
                            return_expr.value_expr
                        ), 
                        param_expr: format!(
                            "WHEN {} = {} THEN {}",
                            value_expr.value_expr.clone(),
                            value_match_expr.value_expr.clone(),
                            return_expr.param_expr
                        ) 
                    });
                }

                return Ok(Expression { 
                    value_expr: format!(
                        "CASE {} ELSE {} END",
                        match_param_expr.iter().map(|e| e.value_expr.clone()).reduce(|acc, expr| format!("{acc} {expr}")).unwrap(),
                        return_expr_if_no_match.value_expr
                    ), 
                    param_expr: format!(
                        "CASE {} ELSE {} END",
                        match_param_expr.into_iter().map(|e| e.param_expr).reduce(|acc, expr| format!("{acc} {expr}")).unwrap(),
                        return_expr_if_no_match.param_expr
                    ) 
                });
            }
            Self::Uppercase(arg0) => {
                let arg0_expr = arg0.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "UPPER({})", 
                        arg0_expr.value_expr
                    ),
                    param_expr: String::from("NULL")
                });
            }
            Self::Wrap(inner) => {
                let inner_expr = inner.build_expr(query)?;
                return Ok(Expression {
                    value_expr: format!(
                        "({})", 
                        inner_expr.value_expr
                    ),
                    param_expr: format!(
                        "({})", 
                        inner_expr.param_expr
                    ),
                });
            }
        }
    }
}



/*
/// Construct a SELECT query to get data from a table
fn construct_data_query(trans: &Transaction, rpt_oid: i64, include_row_oid_clause: bool, include_parent_row_oid_clause: bool) -> Result<ReportQuery, error::Error> {
    // Determine the table OID of the table that forms the basis for the report
    let (base_table_oid, mut subreport_base_parameter_oid) = trans.query_one(
        "SELECT BASE_TABLE_OID, SUBREPORT_BASE_PARAMETER_OID FROM (
            SELECT
                RPT_OID,
                BASE_TABLE_OID,
                NULL AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT__REPORT

            UNION

            SELECT
                s.RPT_OID,
                c.TABLE_OID AS BASE_TABLE_OID,
                s.TABLE_PARAMETER_OID AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT_COLUMN__SUBREPORT s
            INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = s.TABLE_PARAMETER_OID

            UNION

            SELECT
                s.RPT_OID,
                c.TABLE_OID AS BASE_TABLE_OID,
                s.TABLE_PARAMETER_OID AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT_COLUMN__SUBREPORT s
            INNER JOIN METADATA_TABLE_COLUMN c ON c.RPT_PARAMETER_OID = s.TABLE_PARAMETER_OID
        ) WHERE RPT_OID = ?1",
        params![rpt_oid],
        |row| {
            Ok((
                row.get::<_, i64>("BASE_TABLE_OID")?,
                row.get::<_, Option<i64>>("SUBREPORT_BASE_PARAMETER_OID")?
            ))
        }
    )?;

    let mut query: ReportQuery = ReportQuery {
        base_table_oid,
        select_cols_cmd: String::from("t.OID"),
        select_tbls_cmd: format!("FROM TABLE{base_table_oid} t"),
        columns: Vec::new(),
        param_table_oids: HashSet::new(),
    };

    db::query_iterate(trans,
        "SELECT
            c.OID,
            c.NAME,
            f.FORMULA,
            s.RPT_OID
        FROM METADATA_RPT_COLUMN c
        LEFT JOIN METADATA_RPT_COLUMN__FORMULA f ON f.RPT_COLUMN_OID = c.OID
        LEFT JOIN METADATA_RPT_COLUMN__SUBREPORT s ON s.RPT_COLUMN_OID = s.OID
        WHERE c.RPT_OID = ?1 AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;",
        params![rpt_oid],
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let formula_wrapper: Option<String> = row.get("FORMULA")?;
            let subreport_oid_wrapper: Option<i64> = row.get("RPT_OID")?;

            match formula_wrapper {
                Some(formula) => {
                    if subreport_oid_wrapper != None {
                        return Err(error::Error::AdhocError("Invalid database state detected - a report column cannot be both a formula and a subreport."));
                    }

                    // Evaluate the formula in the SQL query
                    // TODO
                },
                None => {
                    match subreport_oid_wrapper {
                        Some(subreport_oid) => {
                            // Register the subreport column details
                            columns.push_back(Column::Subreport {
                                column_oid,
                                column_name: row.get("NAME")?,
                                subreport_oid
                            });
                        },
                        None => {
                            return Err(error::Error::AdhocError("Invalid database state detected - a report must be either a formula or a subreport."));
                        }
                    }
                }
            }

            return Ok(());
        }
    )?;

    // TODO
}

     */
