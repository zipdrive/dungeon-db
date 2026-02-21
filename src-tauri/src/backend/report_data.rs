use crate::backend::{data_type, db, table, table_column};
use crate::util::error;
use rusqlite::{params, named_params, Error as RusqliteError, OptionalExtension, Row, Transaction};
use serde::Serialize;
use serde_json::{Result as SerdeJsonResult, Value};
use std::collections::{HashMap, HashSet, LinkedList};
use std::hash::Hash;
use std::ptr::null;
use crate::util::channel::Channel;
use regex::Regex;

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

enum TableParamRelationship {
    One,
    Many
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



struct ReportQuery {
    base_table_oid: i64,

    select_params_cols_cmd: String,
    param_cols_oids: HashMap<i64, ColumnParam>,
    select_params_tbls_cmd: String,
    param_tbls_oids: HashMap<i64, TableParamRelationship>,
    
    select_cols_cmd: String,
    columns: Vec<Column>,
}

impl ReportQuery {
    fn new(base_table_oid: i64) -> ReportQuery {
        ReportQuery {
            base_table_oid,
            select_params_cols_cmd: String::from("t.OID AS t_OID"),
            param_cols_oids: HashMap::new(),
            select_params_tbls_cmd: format!("FROM TABLE{base_table_oid} t"),
            param_tbls_oids: HashMap::new(),
            select_cols_cmd: String::from("t_OID"),
            columns: Vec::new()
        }
    }

    /// Compiles the query into an SQL statement.
    fn compile(&self) -> String {
        format!(
            "
            WITH PARAM_QUERY AS (
                SELECT {} {}
            )
            SELECT {} FROM PARAM_QUERY
            ",
            self.select_params_cols_cmd,
            self.select_params_tbls_cmd,
            self.select_cols_cmd
        )
    }

    /// Add a column to the query.
    fn insert_column(&mut self, col_definition: String) {
        self.select_cols_cmd = format!("{}, {col_definition}", self.select_cols_cmd);
    }

    /// Add a parameter column to the query.
    fn insert_param_col_def(&mut self, col_definition: String) {
        self.select_params_cols_cmd = format!("{}, {col_definition}", self.select_params_cols_cmd);
    }

    /// Add a joined table to the params query.
    fn insert_param_table_def(&mut self, tbl_definition: String) {
        self.select_params_tbls_cmd = format!("{} {tbl_definition}", self.select_params_tbls_cmd);
    }

    /// Parses the arguments passed to a function.
    fn parse_fixed_args<const N: usize>(&mut self, trans: &Transaction, formula: &String, str: &str) -> Result<([Formula; N], String), error::Error> {
        
    }

    /// Parses the arguments passed to a function.
    fn parse_args(&mut self, trans: &Transaction, formula: &String, str: &str, expected_num_args: Vec<i64>) -> Result<(Vec<Formula>, String), error::Error> {
        return Err(error::Error::AdhocError(""));
    }

    /// Parses an expression with a precedent.
    fn parse_post_expr(&mut self, trans: &Transaction, formula: &String, str: &str, precedent: Formula) -> Result<(Formula, String), error::Error> {
        return Err(error::Error::AdhocError(""));
    }

    /// Parses an expression with no precedent.
    fn parse_expr(&mut self, trans: &Transaction, formula: &String, str: &str) -> Result<(Formula, String), error::Error> {
        
    }

    /// Add a formula to the query as a column with an alias.
    fn insert_formula_column(&mut self, trans: &Transaction, alias: String, formula: String) -> Result<(), error::Error> {
        // Parse the formula
        

        // Turn the formula into a column expression
        // TODO
        return Ok(());
    }

    /// Add a parameter that references a column.
    fn insert_param_column(&mut self, trans: &Transaction, param_oid: i64) -> Result<(), error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.param_cols_oids.contains_key(&param_oid) {
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
            relationship = self.insert_param_table(trans, o)?;
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
                        self.insert_param_col_def(
                            format!("CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Date => {
                        self.insert_param_col_def(
                            format!("
                            DATE({source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::Timestamp => {
                        self.insert_param_col_def(
                            format!("STRFTIME('%FT%TZ', {source_alias}.COLUMN{column_oid}, 'julianday') AS PARAM{param_oid}")
                        );
                    }
                    data_type::Primitive::File => {
                        self.insert_param_col_def(
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
                        self.insert_param_col_def(
                            format!("CASE WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS PARAM{param_oid}")
                        );
                    }
                }

                self.param_cols_oids.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::SingleSelectDropdown(_) => {
                self.insert_param_col_def(format!("p{param_oid}.VALUE AS PARAM{param_oid}"));
                self.insert_param_col_def(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_param_table_def(format!("LEFT JOIN TABLE{column_type_oid} p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                self.param_cols_oids.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("_PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::MultiSelectDropdown(_) => {
                self.insert_param_col_def(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(b.VALUE) || ']' 
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS PARAM{param_oid}")
                );
                self.insert_param_col_def(
                    format!("(
                    SELECT 
                        GROUP_CONCAT(CAST(b.OID AS TEXT))
                    FROM TABLE{column_type_oid}_MULTISELECT a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                    WHERE a.ROW_OID = {source_alias}.OID 
                    GROUP BY a.ROW_OID) AS _PARAM{param_oid}")
                );
                
                self.param_cols_oids.insert(param_oid, ColumnParam {
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
                self.insert_param_col_def(format!("p{param_oid}.DISPLAY_VALUE AS PARAM{param_oid}"));
                self.insert_param_col_def(format!("CAST(p{param_oid}.OID AS TEXT) AS _PARAM{param_oid}"));
                self.insert_param_table_def(format!("LEFT JOIN TABLE{column_type_oid}_SURROGATE p{param_oid} ON p{param_oid}.OID = {source_alias}.COLUMN{column_oid}"));

                self.param_cols_oids.insert(param_oid, ColumnParam {
                    table_oid,
                    column_oid,
                    row_ord: format!("{source_alias}_OID"),
                    column_type,
                    true_ord: Some(format!("_PARAM{param_oid}")),
                    count: relationship
                });
            },
            data_type::MetadataColumnType::ChildTable(_) => {
                self.insert_param_col_def(
                    format!("(
                    SELECT 
                        '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' 
                    FROM TABLE{column_type_oid}_SURROGATE a 
                    INNER JOIN TABLE{column_type_oid} b ON b.OID = a.OID 
                    WHERE b.PARENT_OID = {source_alias}.OID 
                    GROUP BY b.PARENT_OID
                    ) AS PARAM{param_oid}")
                );

                self.param_cols_oids.insert(param_oid, ColumnParam {
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

    /// Add a parameter that references another table.
    fn insert_param_table(&mut self, trans: &Transaction, param_oid: i64) -> Result<TableParamRelationship, error::Error> {
        // First, check to make sure the parameter hasn't already been added
        if self.param_tbls_oids.contains_key(&param_oid) {
            return Ok(self.param_tbls_oids[&param_oid]);
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
            self.insert_param_table(trans, o);
            self.param_tbls_oids[&o]
        } else {
            TableParamRelationship::One
        };
        let this_relationship: TableParamRelationship = match dependent_relationship {
            TableParamRelationship::Many => TableParamRelationship::Many,
            TableParamRelationship::One => if is_many { TableParamRelationship::Many } else { TableParamRelationship::One }
        };

        // Add the join statement
        self.insert_param_table_def(join_statement);

        // Add a constant to indicate the parameter's associated table
        self.insert_column(format!("{table_oid} AS r{param_oid}_TABLE_OID"));
        // Add a column for the OID of the parameter's associated row OID
        self.insert_column(format!("r{param_oid}.OID AS r{param_oid}_OID"));

        // Add the parameter OID to the list of table parameter OIDs, so no duplicate statements are added
        self.param_tbls_oids.insert(param_oid, this_relationship);
        return Ok(this_relationship);
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
