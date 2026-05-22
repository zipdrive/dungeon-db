use std::collections::{HashMap, HashSet};
use rusqlite::OptionalExtension;
use rusqlite::{Connection, Transaction, params, types::Value, vtab::array::Array};
use crate::data::query::QueryBuilder;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::data::{query, schema, table};
use crate::data::datasource::{self, Datasource};
use crate::data::column;
use crate::data::column_type;


pub struct Surrogate {
    pub oid_expr: String,
    pub label_expr: String,
    pub json_expr: String
}

impl Surrogate {
    /// Recursively build mapping from schema to default datasource by traversing up the inheritance hierarchy.
    fn build_schema_to_datasource_mapping(conn: &Connection, schema_oid: i64, schema_to_datasource: &mut HashMap<i64, datasource::Datasource>) -> Result<(), Error> {
        for master_schema_oid_result in conn.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>(0))? {
            let master_schema_oid: i64 = master_schema_oid_result?;
            if !schema_to_datasource.contains_key(&master_schema_oid) {
                let datasource: datasource::Datasource = datasource::Datasource::MasterTable { 
                    parent_datasource: Box::new(schema_to_datasource[&schema_oid].clone()), 
                    table_oid: master_schema_oid
                };
                schema_to_datasource.insert(master_schema_oid, datasource);

                Self::build_schema_to_datasource_mapping(conn, master_schema_oid, schema_to_datasource)?;
            }
        }
        Ok(())
    }



    /// Determines the surrogate expression. Does not take additional columns from subtypes into account.
    pub fn get_flat(conn: &Connection, query: &mut query::QueryBuilder, datasource: Datasource, surrogate_table_oid_chain: Vec<i64>) -> Result<Self, Error> {
        let schema_oid: i64 = datasource.get_schema_oid()?;
        let oid_expr: String = format!("{}.OID", datasource.get_alias());
        if surrogate_table_oid_chain.contains(&schema_oid) {
            // For now, just terminate the recursion
            return Ok(Self {
                oid_expr,
                label_expr: format!("'...'"),
                json_expr: format!("'{{ ... }}'")
            });
        }
        
        // Build mapping to all master tables
        let mut schema_to_datasource: HashMap<i64, datasource::Datasource> = HashMap::new();
        schema_to_datasource.insert(schema_oid, datasource);
        Self::build_schema_to_datasource_mapping(conn, schema_oid, &mut schema_to_datasource)?;

        // Determine the primary key columns of the base schema
        let mut base_columns: Vec<column::FullMetadata> = Vec::new();
        for result in conn
            .prepare(
                "
                SELECT
                    c.OID
                FROM (
                    SELECT ?1 AS OID
                    UNION
                    SELECT MASTER_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE INHERITOR_SCHEMA_OID = ?1
                ) s
                INNER JOIN METADATA_COLUMN c ON c.SCHEMA_OID = s.OID
                WHERE c.IS_PRIMARY_KEY
                    AND NOT c.TRASH
                ORDER BY c.ORDERING
                "
            )?
            .query_map(params![schema_oid], |row| row.get::<_, i64>("OID"))? {

            let column_oid: i64 = result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(conn, column_oid)?;
            base_columns.push(column_metadata);
        }

        let mut expanded_surrogate_table_oid_chain: Vec<i64> = surrogate_table_oid_chain.clone();
        expanded_surrogate_table_oid_chain.push(schema_oid);

        if base_columns.len() > 1 {
            let mut subquery = query.spawn();

            let mut json_exprs: Vec<String> = Vec::new();
            for base_column in base_columns {
                let column_name: String = base_column.name.clone();
                let col = subquery.compile_column(Some(&schema_to_datasource[&base_column.schema.oid]), base_column, expanded_surrogate_table_oid_chain.clone())?;
                json_exprs.push(format!(
                    r#"SELECT '"{}": ' || {} AS COLUMN1"#,
                    column_name.replace("\\", "\\\\").replace("\"", "\\\""),
                    col.get_json_expr(String::from("NULL"))
                ));
            }

            let Some((subquery_from_clause, _)) = subquery.compile_datasources(false)? else {
                return Err(Error::AdhocError("Subquery for surrogate key of table failed to compile."));
            };

            let json_expr: String = format!(
                "(SELECT '{{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1) FROM ({})), '') || '}}' {subquery_from_clause})",
                json_exprs.into_iter().reduce(|acc, e| format!("{acc} UNION {e}")).unwrap_or(String::from(""))
            );

            Ok(Self {
                oid_expr,
                label_expr: json_expr.clone(),
                json_expr
            })
        } else if base_columns.len() == 1 {
            let col = query.compile_column(Some(&schema_to_datasource[&base_columns[0].schema.oid]), base_columns[0].clone(), expanded_surrogate_table_oid_chain)?;
            Ok(Self {
                oid_expr,
                label_expr: col.get_label_expr(),
                json_expr: col.get_json_expr(String::from("'null'"))
            })
        } else {
            Ok(Self {
                oid_expr,
                label_expr: String::from("'— NO PRIMARY KEY —'"),
                json_expr: String::from("'null'")
            })
        }
    }


    /// Determines the surrogate expression. DOES take additional columns from subtypes into account.
    pub fn get_object(conn: &Connection, query: &mut query::QueryBuilder, datasource: Datasource, surrogate_table_oid_chain: Vec<i64>) -> Result<Self, Error> {
        let mut subquery = query.spawn();

        let schema_oid: i64 = datasource.get_schema_oid()?;
        let datasource_alias: String = datasource.get_alias();
        let oid_expr: String = format!("{datasource_alias}.OID");
        if surrogate_table_oid_chain.contains(&schema_oid) {
            // For now, just terminate the recursion
            return Ok(Self {
                oid_expr,
                label_expr: format!("'...'"),
                json_expr: format!("'{{ ... }}'")
            });
        }
        
        // Build mapping to all master tables
        let mut schema_to_datasource: HashMap<i64, datasource::Datasource> = HashMap::new();
        schema_to_datasource.insert(schema_oid, datasource);
        Self::build_schema_to_datasource_mapping(conn, schema_oid, &mut schema_to_datasource)?;

        // Determine the primary key columns of the base schema
        let mut base_columns: Vec<column::FullMetadata> = Vec::new();
        for result in conn
            .prepare(
                "
                SELECT
                    c.OID
                FROM (
                    SELECT ?1 AS OID
                    UNION
                    SELECT MASTER_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE INHERITOR_SCHEMA_OID = ?1
                    UNION
                    SELECT INHERITOR_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1
                ) s
                INNER JOIN METADATA_COLUMN c ON c.SCHEMA_OID = s.OID
                WHERE c.IS_PRIMARY_KEY
                    AND NOT c.TRASH
                ORDER BY c.ORDERING
                "
            )?
            .query_map(params![schema_oid], |row| row.get::<_, i64>("OID"))? {

            let column_oid: i64 = result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(conn, column_oid)?;
            base_columns.push(column_metadata);
        }

        // Determine the possible types for the object
        let mut inheritor_datasources: Vec<(String, String)> = Vec::new();
        for result in conn
            .prepare(
                "
                SELECT 
                    OID,
                    NAME,
                    ?2 AS DATASOURCE_PATH,
                    0 AS MAX_DEPTH
                FROM METADATA_SCHEMA
                WHERE OID = ?1

                UNION
                
                SELECT 
                    s.OID,
                    s.NAME,
                    ?2 || inh.INHERITOR_DATASOURCE_PATH AS DATASOURCE_PATH,
                    inh.MAX_DEPTH
                FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW inh
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID
                WHERE inh.MASTER_SCHEMA_OID = ?1

                ORDER BY MAX_DEPTH DESC
                "
            )?
            .query_map(params![schema_oid, datasource_alias], |row| Ok((row.get::<_, i64>("OID")?, row.get::<_, String>("NAME")?, row.get::<_, String>("DATASOURCE_PATH")?)))? {

            let (inheritor_schema_oid, inheritor_schema_name, inheritor_schema_datasource_path) = result?;
            let inheritor_datasource: Datasource = Datasource::from_alias_transact(conn, format!("{datasource_alias}{inheritor_schema_datasource_path}"))?;
            schema_to_datasource.insert(inheritor_schema_oid, inheritor_datasource);
            inheritor_datasources.push((subquery.insert_datasource(&schema_to_datasource[&inheritor_schema_oid])?, inheritor_schema_name));
        }

        // Compile expression to get object type name
        let obj_type_expr: String = match inheritor_datasources.into_iter().map(|(inheritor_datasource_alias, inheritor_schema_name)|
            format!(
                "WHEN {inheritor_datasource_alias}.OID IS NOT NULL THEN '\"{}\"'", 
                inheritor_schema_name.replace("'", "''").replace("\\", "\\\\").replace("\"", "\\\"")
            )
        ).reduce(|acc, e| format!("{acc} {e}")) {
            Some(when_clauses) => format!("CASE {when_clauses} ELSE NULL END"),
            None => String::from("NULL")
        };

        if base_columns.len() > 0 {
            let mut json_exprs: Vec<String> = Vec::new();
            for base_column in base_columns {
                let mut expanded_surrogate_table_oid_chain: Vec<i64> = surrogate_table_oid_chain.clone();
                expanded_surrogate_table_oid_chain.push(schema_oid);
                expanded_surrogate_table_oid_chain.push(base_column.schema.oid);
                
                let column_name: String = base_column.name.clone();
                let col = subquery.compile_column(Some(&schema_to_datasource[&base_column.schema.oid]), base_column, expanded_surrogate_table_oid_chain.clone())?;
                json_exprs.push(format!(
                    r#"SELECT '"{}": ' || {} AS COLUMN1"#,
                    column_name.replace("'", "''").replace("\\", "\\\\").replace("\"", "\\\""),
                    col.get_json_expr(String::from("NULL"))
                ));
            }

            let Some((subquery_from_clause, _)) = subquery.compile_datasources(false)? else {
                return Err(Error::AdhocError("Subquery for surrogate key of table failed to compile."));
            };

            let json_expr: String = format!(
                "(SELECT '{{' || {obj_type_expr} || '{{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1) FROM ({})), '') || '}}}}' {subquery_from_clause})",
                json_exprs.into_iter().reduce(|acc, e| format!("{acc} UNION {e}")).unwrap_or(String::from(""))
            );

            Ok(Self {
                oid_expr,
                label_expr: json_expr.clone(),
                json_expr
            })
        } else {
            let Some((subquery_from_clause, _)) = subquery.compile_datasources(false)? else {
                return Err(Error::AdhocError("Subquery for surrogate key of table failed to compile."));
            };

            let json_expr: String = format!("(SELECT '{{' || {obj_type_expr} || ': {{}}}}' {subquery_from_clause})");
            Ok(Self {
                oid_expr,
                label_expr: json_expr.clone(),
                json_expr
            })
        }
    }
}
