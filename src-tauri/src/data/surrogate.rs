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
        for master_schema_oid_result in conn.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>(0))? {
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
                    SELECT MASTER_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1
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
            let mut json_exprs: Vec<String> = Vec::new();
            for base_column in base_columns {
                let column_name: String = base_column.name.clone();
                let col = query.compile_column(Some(&schema_to_datasource[&base_column.schema.oid]), base_column, expanded_surrogate_table_oid_chain.clone())?;
                json_exprs.push(format!(
                    r#"SELECT '"{}": ' || {} AS COLUMN1"#,
                    column_name.replace("\\", "\\\\").replace("\"", "\\\""),
                    col.get_json_expr(String::from("NULL"))
                ));
            }
            let json_expr: String = format!(
                "'{{' || (SELECT GROUP_CONCAT(COLUMN1) FROM ({})) || '}}'",
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
}
