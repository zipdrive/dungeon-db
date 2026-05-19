use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::schema;
use crate::data::surrogate;
use rusqlite::{Transaction, OptionalExtension, params};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

/// Data structure representing the table metadata
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct FullMetadata {
    pub schema: schema::FullMetadata
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state)
    }
}

impl Borrow<i64> for FullMetadata {
    fn borrow(&self) -> &i64 {
        self.schema.borrow()
    }
}

impl FullMetadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Get the schema metadata
        let schema_metadata = schema::FullMetadata::get(&conn, oid)?;

        // Return the metadata
        Ok(Self {
            schema: schema_metadata
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        
        // Create schema
        self.schema.create(&trans)?;

        // Create the table
        let create_table_cmd: String = format!(
            "
            CREATE TABLE TABLE{} (
                OID INTEGER PRIMARY KEY, 
                TRASH INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            ",
            self.schema.oid
        );
        trans.execute(&create_table_cmd, [])?;

        // To update the inheritance, now that there is a constructed table for it
        self.schema.set(&trans)?;

        // Create the table metadata
        trans.execute("INSERT INTO METADATA_TABLE (OID) VALUES (?1)", params![self.schema.oid])?;
        // Create a datasource for the table
        trans.execute("INSERT INTO METADATA_DATASOURCE (TABLE_OID) VALUES (?1)", params![self.schema.oid])?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Overwrite the schema metadata
        self.schema.set(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}



struct LabelExpression {
    /// An expression for the OID of the row.
    oid_expr: String,

    /// An expression for a label for the row, in plaintext format (if the table is not inherited from AND the table only has a single primary key) 
    /// or JSON format otherwise.
    plain_expr: String,

    /// An expression for a label for the row, in JSON format.
    json_expr: String
}

impl LabelExpression {
    /// Compiles the label expressions for a table.
    fn compile_transact(trans: &Transaction, query: &QueryBuilder, datasource: Datasource) -> Result<Self, Error> {
        let datasource_alias: String = query.insert_datasource(&datasource)?;

        let column_subqueries: Vec<(String, String)> = Vec::new();

        // Iterate over each column of the table and its master tables
        for row_result in trans.prepare("SELECT sc.COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW sc INNER JOIN METADATA_COLUMN c ON c.OID = sc.COLUMN_OID WHERE sc.SCHEMA_OID = ?1 AND sc.IS_REQUIRED ORDER BY c.ORDERING")?.query_map(params![datasource.get_schema_oid()?], |row| row.get::<_, i64>("COLUMN_OID"))? {
            let column_oid: i64 = row_result?;
            let column_metadata: column::Metadata = column::Metadata::get_transact(trans, column_oid)?;
            let sanitized_column_name: String = column_metadata.name.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "''");

            match column_metadata.column_type {
                column_type::ColumnType::Primitive(prim) => {
                    let subquery_from_clause: String = match subquery.compile_datasources()? {
                        Some((from_clause, _)) => from_clause,
                        None => String::from("")
                    };

                    match prim {
                        column_type::Primitive::Integer
                        | column_type::Primitive::Number 
                        | column_type::Primitive::JSON => {
                            column_subqueries.push((
                                format!("SELECT {datasource_alias}.COLUMN{column_oid} AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": ' || CAST({datasource_alias}.COLUMN{column_oid} AS TEXT) AS COLUMN1 {subquery_from_clause}")
                            ));
                        }
                        column_type::Primitive::Text => {
                            column_subqueries.push((
                                format!("SELECT {datasource_alias}.COLUMN{column_oid} AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": \"' || REPLACE(REPLACE({datasource_alias}.COLUMN{column_oid}, '\\', '\\\\'), '\"', '\\\"') || '\"' {subquery_from_clause}")
                            ));
                        }
                        column_type::Primitive::Checkbox => {
                            column_subqueries.push((
                                format!("SELECT IF({datasource_alias}.COLUMN{column_oid}, 'True', 'False') AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": ' || IF({datasource_alias}.COLUMN{column_oid}, 'true', 'false') AS COLUMN1 {subquery_from_clause}")
                            ));
                        }
                        column_type::Primitive::Date => {
                            column_subqueries.push((
                                format!("SELECT DATE({datasource_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": \"' || DATE({datasource_alias}.COLUMN{column_oid}, 'julianday') || '\"' AS COLUMN1 {subquery_from_clause}")
                            ));
                        }
                        column_type::Primitive::Datetime => {
                            column_subqueries.push((
                                format!("SELECT STRFTIME('%FT%TZ', {datasource_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": \"' || STRFTIME('%FT%TZ', {datasource_alias}.COLUMN{column_oid}, 'julianday') || '\"' AS COLUMN1 {subquery_from_clause}")
                            ));
                        }
                        column_type::Primitive::File 
                        | column_type::Primitive::Image => {
                            column_subqueries.push((
                                format!("SELECT (SELECT LABEL FROM METADATA_FILE_VIEW WHERE OID = {datasource_alias}.COLUMN{column_oid}) AS COLUMN1 {subquery_from_clause}"),
                                format!("SELECT '\"{sanitized_column_name}\": \"' || (SELECT LABEL FROM METADATA_FILE_VIEW WHERE OID = {datasource_alias}.COLUMN{column_oid}) || '\"' AS COLUMN1 {subquery_from_clause}")
                            ));
                        }
                    }
                }
                column_type::ColumnType::Object { table_oid, .. } 
                | column_type::ColumnType::Select { table_oid, .. } => {
                    let subquery: QueryBuilder = query.spawn();
                    let select_label: Self = Self::compile_transact(trans, &subquery, Datasource::from_alias_transact(trans, format!("{datasource_alias}_COLUMN{}", column.oid)))?;
                    
                    let subquery_from_clause: String = match subquery.compile_datasources()? {
                        Some((from_clause, _)) => from_clause,
                        None => String::from("")
                    };
                    column_subqueries.push((
                        format!("SELECT {} AS COLUMN1 {subquery_from_clause}", select_label.plain_expr), 
                        format!(
                            "SELECT '\"{sanitized_column_name}\": ' || {} AS COLUMN1 {subquery_from_clause}", 
                            select_label.json_expr
                        )
                    ));
                }
                column_type::ColumnType::Multiselect { table_oid, .. } => {
                    let subquery: QueryBuilder = query.spawn();
                    let select_label: Self = Self::compile_transact(trans, &subquery, Datasource::from_alias_transact(trans, format!("{datasource_alias}_COLUMN{}", column.oid)))?;
                    
                    let subquery_from_clause: String = match subquery.compile_datasources()? {
                        Some((from_clause, _)) => from_clause,
                        None => String::from("")
                    };
                    column_subqueries.push((
                        String::from("NULL"),
                        format!("SELECT '\"{sanitized_column_name}\": [' || COALESCE(GROUP_CONCAT({}, ', '), '') || ']' AS COLUMN1 {subquery_from_clause} GROUP BY {datasource_alias}.OID", select_label.json_expr)
                    ));
                }
            }
        }

        // Compile into a single query
        Ok(Self {
            oid_expr: format!("{datasource_alias}.OID"),
            plain_expr: if column_subqueries.len() > 1 {
                String::from("NULL")
            } else if column_subqueries.len() == 1 {
                column_subqueries[0].0
            } else {
                String::from("'— NO PRIMARY KEY —'")
            },
            json_expr: if column_subqueries.len() > 0 {
                format!(
                    "(SELECT '{{' || COALESCE(GROUP_CONCAT(({}), ', '), '') || '}}')",
                    column_subqueries.into_iter().map(|(_, key_value_pair)| key_value_pair).reduce(|acc, e| format!("{acc} UNION ALL {e}")).unwrap()
                )
            } else {
                String::from("'{{}}'")
            }
        })
    }
}

/// Regenerates the label views related to a specific table.
pub fn regenerate_label_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let mut schema_oid_seq: Vec<i64> = Vec::new();

    // Drop every label view that this one inherits from, from the top down
    trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1 ORDER BY MAX_DEPTH DESC")?
        .query_and_then(params![table_oid], |row| {
            let master_schema_oid: i64 = row.get("MASTER_SCHEMA_OID")?;
            schema_oid_seq.push(master_schema_oid);

            let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{inheritor_schema_oid}_LABEL_VIEW");
            trans.execute_batch(&drop_sql)?;
            Ok(())
        })?;

    // Drop this table's label view
    {
        schema_oid_seq.push(table_oid);

        let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{table_oid}_LABEL_VIEW");
        trans.execute_batch(&drop_sql)?;
    }

    // Drop every label view inheriting from this one, from the top down
    trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1 ORDER BY MAX_DEPTH ASC")?
        .query_and_then(params![table_oid], |row| {
            let inheritor_schema_oid: i64 = row.get("INHERITOR_SCHEMA_OID")?;
            schema_oid_seq.push(inheritor_schema_oid);

            let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{inheritor_schema_oid}_LABEL_VIEW");
            trans.execute_batch(&drop_sql)?;
            Ok(())
        })?;

    // Recreate every label view, from the bottom up
    for table_oid in schema_oid_seq.iter().rev() {
        // Get all tables that inherit directly from this one
        let inheritor_table_oids: Vec<i64> = Vec::new();
        trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?
            .query_and_then(params![table_oid], |row| { 
                let inheritor_table_oid: i64 = row.get("INHERITOR_SCHEMA_OID")?;
                inheritor_table_oids.push(inheritor_table_oid);
            })?;

        // Compile flat label expressions
        let root_datasource: Datasource = Datasource::get_default_datasource_transact(trans, table_oid)?;
        let subquery: QueryBuilder = QueryBuilder::new(vec![root_datasource]);
        let flat_label_expressions: LabelExpression = LabelExpression::compile_transact(trans, &subquery, root_datasource)?;
        let flat_label_sql: String = format!(
            "
            SELECT
                {} AS OID,
                {} AS PLAIN_LABEL,
                {} AS JSON_LABEL
            {}
            ",
            flat_label_expressions.oid_expr,
            flat_label_expressions.plain_expr,
            flat_label_expressions.json_expr,
            match subquery.compile_datasources()? {
                Some((from_clause, _)) => from_clause,
                None => String::from("")
            }
        );

        // Compile and create the view for labels
        let create_label_view_sql: String = if inheritor_table_oids.len() > 0 {
            format!(
                "
                CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS
                SELECT
                    t.OID,
                    COALESCE(u.SCHEMA_OID, ?1) AS SCHEMA_OID,
                    COALESCE(u.ROW_OID, t.OID) AS ROW_OID,
                    COALESCE(t.PLAIN_LABEL, t.JSON_LABEL) AS PLAIN_LABEL,
                    t.JSON_LABEL,
                    COALESCE(p.OBJECT_LABEL, '{{\"' || (SELECT REPLACE(REPLACE(NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA WHERE OID = ?1) || '\": ' || t.JSON_LABEL || '}}') AS OBJECT_LABEL
                FROM ({flat_label_sql}) t
                LEFT JOIN ({}) u ON u.MASTER{table_oid}_OID = t.OID
                ",
                inheritor_table_oids.into_iter()
                    .map(|inheritor_table_oid| format!("SELECT i.MASTER{table_oid}_OID, p.SCHEMA_OID, p.ROW_OID, p.OBJECT_LABEL FROM TABLE{inheritor_table_oid}_POLYMORPHISM_VIEW p INNER JOIN TABLE{inheritor_table_oid} i"))
                    .reduce(|acc, e| format!("{acc} UNION {e}"))
                    .unwrap()
            )
        } else {
            format!(
                "
                CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS
                SELECT
                    t.OID,
                    ?1 AS SCHEMA_OID,
                    t.OID AS ROW_OID,
                    COALESCE(t.PLAIN_LABEL, t.JSON_LABEL) AS PLAIN_LABEL,
                    t.JSON_LABEL,
                    '{{\"' || (SELECT REPLACE(REPLACE(NAME, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_SCHEMA WHERE OID = ?1) || '\": ' || t.JSON_LABEL || '}}') AS OBJECT_LABEL
                FROM ({flat_label_sql}) t
                "
            )
        };
        trans.execute(&create_label_view_sql, params![table_oid])?;
    }
}
