use crate::data::query::QueryBuilder;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::schema;
use crate::data::surrogate;
use crate::data::datasource::Datasource;
use crate::data::column_type;
use crate::data::column;
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

        // Regenerate the label view
        regenerate_table_views(&trans, self.schema.oid)?;

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

        // Regenerate the label view
        regenerate_table_views(&trans, self.schema.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}



/// Compiles a CTE to determine the lowest-level inheritor table that is associated with a particular row in the master table.
fn compile_polymorphism_cte(trans: &Transaction, table_oid: i64, compiled_cte: &mut HashMap<String, String>) -> Result<(), Error> {
    let cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(()); // Prevent infinite recursion, just in case
    }
    
    // The components of the query will be combined with UNION
    let mut polymorphism_cte_components: Vec<String> = Vec::new();

    // Get polymorphism of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        compile_polymorphism_cte(trans, inheritor_table_oid, compiled_cte)?;

        // Get the polymorphism from the inheritor CTE
        polymorphism_cte_components.push(format!(
            "
            SELECT
                i.MASTER{table_oid}_OID AS OID,
                p.TABLE_OID,
                p.ROW_OID
            FROM TABLE{inheritor_table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = p.OID
            "
        ));
    }

    // Compile the final CTE
    compiled_cte.insert(cte_name, 
        if polymorphism_cte_components.len() > 0 {
            format!(
                "
                SELECT
                    t.OID,
                    COALESCE(u.TABLE_OID, {table_oid}) AS TABLE_OID,
                    COALESCE(u.ROW_OID, t.OID) AS ROW_OID
                FROM TABLE{table_oid} t
                LEFT JOIN ({}) u ON u.OID = t.OID
                ",
                polymorphism_cte_components.into_iter().reduce(|acc, e| format!("{acc} UNION {e}")).unwrap()
            )
        } else {
            format!(
                "
                SELECT
                    t.OID,
                    {table_oid} AS TABLE_OID,
                    t.OID AS ROW_OID
                FROM TABLE{table_oid} t
                "
            )
        }
    );
    Ok(())
}

/// Creates a view to determine the lowest-level inheritor table that is associated with a particular row in the master table.
fn create_polymorphism_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let final_cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    let view_name: String = format!("TABLE{table_oid}_POLYMORPHISM_VIEW");

    // Compile all necessary CTEs
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    compile_polymorphism_cte(trans, table_oid.clone(), &mut compiled_cte)?;

    // Compile and create the final view
    if let Some(final_cte) = compiled_cte.remove(&final_cte_name) {
        let create_sql: String = format!(
            "
            CREATE VIEW {view_name} AS 
            {}
            {final_cte}
            ",
            if compiled_cte.len() > 0 {
                format!("WITH {}",
                    compiled_cte.into_iter()
                        .map(|(cte_name, cte_sql)| format!("{cte_name} AS ({cte_sql})"))
                        .reduce(|acc, e| format!("{acc}, {e}"))
                        .unwrap()
                )
            } else {
                String::from("")
            }
        );
        trans.execute(&create_sql, [])?;
    }
    Ok(())
}



/// Compiles a CTE to get the primary key columns for a particular row in a table.
fn compile_keycolumn_cte(trans: &Transaction, table_oid: i64, compiled_cte: &mut HashMap<String, String>) -> Result<bool, Error> {
    // Prevent duplication
    let cte_name: String = format!("TABLE{table_oid}_KEYCOLUMNS_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] != "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // The components of the query will be combined with UNION
    let mut column_cte_components: Vec<String> = Vec::new();

    // Get primary keys of master tables
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        // Ensure that master CTE is compiled
        let master_table_oid: i64 = row_result?;
        if compile_keycolumn_cte(trans, master_table_oid, compiled_cte)? {
            // Get the columns from the master CTE
            column_cte_components.push(format!(
                "
                SELECT
                    t.OID,
                    m.PLAIN_LABEL,
                    m.JSON_LABEL
                FROM TABLE{master_table_oid}_KEYCOLUMNS_CTE m
                INNER JOIN TABLE{table_oid} t ON t.MASTER{master_table_oid}_OID = m.OID
                "
            ));
        } // Otherwise, ignore the primary keys of the master table
    }

    // Get each primary key column from this table
    for row_result in trans.prepare("SELECT OID FROM METADATA_COLUMN WHERE SCHEMA_OID = ?1 AND IS_PRIMARY_KEY AND NOT TRASH")?.query_map(params![table_oid], |row| row.get::<_, i64>("OID"))? {
        let column_oid: i64 = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;
        let sanitized_column_name: String = column_metadata.name.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "''");

        // Determine expression for this specific column
        match column_metadata.column_type {
            column_type::ColumnType::Primitive(prim) => {
                match prim {
                    column_type::Primitive::Integer
                    | column_type::Primitive::Number 
                    | column_type::Primitive::JSON => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE(CAST(t.COLUMN{column_oid} AS TEXT), 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Text => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                t.COLUMN{column_oid} AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(t.COLUMN{column_oid}, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Checkbox => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                IF(t.COLUMN{column_oid}, 'True', 'False') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || CASE WHEN t.COLUMN{column_oid} IS NULL THEN 'null' WHEN t.COLUMN{column_oid} THEN 'true' ELSE 'false' END AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Date => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                DATE(t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || DATE(t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::Datetime => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'julianday') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            "
                        ));
                    }
                    column_type::Primitive::File 
                    | column_type::Primitive::Image => {
                        column_cte_components.push(format!(
                            "
                            SELECT
                                t.OID,
                                f.LABEL AS PLAIN_LABEL,
                                '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', 'null') AS JSON_LABEL
                            FROM TABLE{table_oid} t
                            LEFT JOIN METADATA_FILE_VIEW f ON f.OID = t.COLUMN{column_oid}
                            "
                        ));
                    }
                }
            }
            column_type::ColumnType::Object { table_oid: object_table_oid, .. } => {
                if compile_full_label_cte(trans, object_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            o.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE('\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{' || s.JSON_LABEL || '}}', 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{object_table_oid}_LABEL_CTE o ON o.OID = t.COLUMN{column_oid}
                        LEFT JOIN METADATA_SCHEMA s ON s.OID = o.SCHEMA_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Select { table_oid: select_table_oid, .. } => {
                if compile_full_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            s.PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ' || COALESCE(s.JSON_LABEL, 'null') AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = t.COLUMN{column_oid}
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '...' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": ...' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            column_type::ColumnType::Multiselect { table_oid: select_table_oid, .. } => {
                if compile_full_label_cte(trans, select_table_oid, compiled_cte)? {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            m.TABLE{table_oid}_OID AS OID,
                            '[' || GROUP_CONCAT('\"' || REPLACE(REPLACE(s.PLAIN_LABEL, '\\', '\\\\'), '\"', '\\\"') || '\"', ', ') || ']' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [' || COALESCE(GROUP_CONCAT(s.JSON_LABEL, ', '), '') || ']' AS JSON_LABEL
                        FROM MULTISELECT{column_oid} m
                        LEFT JOIN TABLE{select_table_oid}_LABEL_CTE s ON s.OID = m.TABLE{select_table_oid}_OID
                        GROUP BY m.TABLE{table_oid}_OID
                        "
                    ));
                } else {
                    column_cte_components.push(format!(
                        "
                        SELECT
                            t.OID,
                            '[...]' AS PLAIN_LABEL,
                            '\"{sanitized_column_name}\": [...]' AS JSON_LABEL
                        FROM TABLE{table_oid} t
                        "
                    ));
                }
            }
            _ => {
                // Skip any other type of column
            }
        }
    }

    // Compile the final CTE
    match column_cte_components.into_iter().reduce(|acc, e| format!("{acc} UNION {e}")) {
        Some(compiled_column_cte) => {
            compiled_cte.insert(cte_name, compiled_column_cte);
            Ok(true)
        }
        None => {
            Ok(false)
        }
    } 
}

/// Compile a CTE that combines the polymorphism CTE and the key columns CTE into a single CTE.
fn compile_full_label_cte(trans: &Transaction, table_oid: i64, compiled_cte: &mut HashMap<String, String>) -> Result<bool, Error> {
    let cte_name: String = format!("TABLE{table_oid}_LABEL_CTE");
    if compiled_cte.contains_key(&cte_name) {
        return Ok(compiled_cte[&cte_name] == "...");
    }
    compiled_cte.insert(cte_name.clone(), String::from("..."));

    // Ensure that the polymorphism CTE has been compiled
    compile_polymorphism_cte(trans, table_oid, compiled_cte)?;

    // Ensure that the keycolumns CTE has been compiled
    if !compile_keycolumn_cte(trans, table_oid, compiled_cte)? {
        // Again, something is completely fucked, recursion-wise
        return Ok(false);
    }

    let mut label_cte_components: Vec<String> = Vec::new();

    // Get labels of inheritor tables
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![table_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        // Ensure that inheritor CTE is compiled
        let inheritor_table_oid: i64 = row_result?;
        if compile_full_label_cte(trans, inheritor_table_oid, compiled_cte)? {
            label_cte_components.push(format!(
                "
                SELECT
                    i.MASTER{table_oid}_OID AS OID,
                    lbl.SCHEMA_OID,
                    lbl.ROW_OID,
                    lbl.PLAIN_LABEL,
                    lbl.JSON_LABEL
                FROM TABLE{inheritor_table_oid}_LABEL_CTE lbl
                INNER JOIN TABLE{inheritor_table_oid} i ON i.OID = lbl.OID
                "
            ));
        } // Otherwise, ignore the inheritor labels
    }

    if label_cte_components.len() > 0 {
        compiled_cte.insert(cte_name, format!(
            "
            SELECT
                p.OID,
                p.SCHEMA_OID,
                p.ROW_OID,
                COALESCE(u.PLAIN_LABEL,
                    (
                        SELECT
                            CASE
                                WHEN COUNT(k.*) = 0 THEN '— NO PRIMARY KEY —'
                                WHEN COUNT(k.*) = 1 THEN MIN(k.PLAIN_LABEL)
                                ELSE NULL
                            END
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS PLAIN_LABEL,
                COALESCE(u.JSON_LABEL, 
                    (
                        SELECT 
                            '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' 
                        FROM TABLE{table_oid}_KEYCOLUMNS_CTE k 
                        WHERE k.OID = p.OID
                        GROUP BY p.OID
                    )
                ) AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            LEFT JOIN ({}) u ON u.SCHEMA_OID = p.SCHEMA_OID AND u.ROW_OID = p.ROW_OID
            ",
            label_cte_components.into_iter().reduce(|acc, e| format!("{acc} UNION {e}")).unwrap()
        ));
    } else {
        compiled_cte.insert(cte_name, format!(
            "
            SELECT
                p.OID,
                p.SCHEMA_OID,
                p.ROW_OID,
                CASE
                    WHEN COUNT(k.*) = 0 THEN '— NO PRIMARY KEY —'
                    WHEN COUNT(k.*) = 1 THEN MIN(k.PLAIN_LABEL)
                    ELSE NULL
                END AS PLAIN_LABEL,
                '{{' || COALESCE(GROUP_CONCAT(k.JSON_LABEL, ', '), '') || '}}' AS JSON_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_CTE p
            INNER JOIN TABLE{table_oid}_KEYCOLUMNS_CTE k ON k.OID = p.OID
            GROUP BY
                p.OID,
                p.SCHEMA_OID,
                p.ROW_OID
            "
        ));
    }
    Ok(true)
}

/// Creates the label view for a table.
fn create_label_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    if compile_full_label_cte(trans, table_oid, &mut compiled_cte)? {
        let create_sql: String = format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS
            WITH {} 
            SELECT
                lbl.OID,
                lbl.SCHEMA_OID,
                lbl.ROW_OID,
                COALESCE(lbl.PLAIN_LABEL, lbl.JSON_LABEL) AS SELECT_LABEL,
                lbl.JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": ' || lbl.JSON_LABEL || '}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_LABEL_CTE lbl
            INNER JOIN METADATA_SCHEMA s ON s.OID = lbl.SCHEMA_OID
            ",
            compiled_cte.into_iter().map(|(cte_name, cte_definition)| format!("{cte_name} AS ({cte_definition})")).reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(format!("TABLE{table_oid}_LABEL_CTE AS (SELECT OID, {table_oid} AS SCHEMA_OID, OID AS ROW_OID, )"))
        );
        trans.execute(&create_sql, [])?;
    }
    Ok(())
}

/// Regenerates the views related to a specific table.
pub fn regenerate_table_views(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    // TODO logic for when inheritance is dropped?

    // Drop and recreate the label views for the tables that this table inherits from
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE INHERITOR_SCHEMA_OID = ?1 ORDER BY MAX_DEPTH DESC")?.query_map(params![table_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let cte_table_oid: i64 = row_result?;
        
        let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{cte_table_oid}_LABEL_VIEW; DROP VIEW IF EXISTS TABLE{cte_table_oid}_POLYMORPHISM_VIEW;");
        trans.execute_batch(&drop_sql)?;

        create_polymorphism_view(trans, cte_table_oid)?;
        create_label_view(trans, cte_table_oid)?;
    }

    // Drop and recreate this table's label view
    {
        let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{table_oid}_LABEL_VIEW; DROP VIEW IF EXISTS TABLE{table_oid}_POLYMORPHISM_VIEW;");
        trans.execute_batch(&drop_sql)?;
        
        create_polymorphism_view(trans, table_oid)?;
        create_label_view(trans, table_oid)?;
    }

    // Drop and recreate every label view inheriting from this one
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW WHERE MASTER_SCHEMA_OID = ?1 ORDER BY MAX_DEPTH ASC")?.query_map(params![table_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let cte_table_oid: i64 = row_result?;
        
        let drop_sql: String = format!("DROP VIEW IF EXISTS TABLE{cte_table_oid}_LABEL_VIEW; DROP VIEW IF EXISTS TABLE{cte_table_oid}_POLYMORPHISM_VIEW;");
        trans.execute_batch(&drop_sql)?;

        create_polymorphism_view(trans, cte_table_oid)?;
        create_label_view(trans, cte_table_oid)?;
    }
    Ok(())
}
