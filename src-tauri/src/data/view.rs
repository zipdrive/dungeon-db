use std::collections::{HashMap, HashSet};
use bitflags::bitflags;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use crate::{data::{column, column_type, datasource::Datasource, schema, table}, util::{error::Error, formula::Formula}};

struct ViewsToCreate {
    /// The OID of the schema to create views for.
    schema_oid: i64,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool,

    /// True if the polymorphism view needs to be created. False otherwise.
    create_polymorphism_view: bool
}


/// Drop the views associated with a schema.
fn drop_all_views(trans: &Transaction, schema_oid: i64, create_schema_oid_seq: &mut Vec<ViewsToCreate>) -> Result<(), Error> {
    create_schema_oid_seq.push(ViewsToCreate { 
        schema_oid, 
        create_label_view: true, 
        create_polymorphism_view: true
    });

    // Drop the views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("MASTER_SCHEMA_OID"))? {
        let master_schema_oid: i64 = row_result?;
        drop_all_views(trans, master_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
        let inheritor_schema_oid: i64 = row_result?;
        drop_all_views(trans, inheritor_schema_oid, create_schema_oid_seq)?;
    }

    // Drop the views that use the label
    for row_result in trans.prepare("
        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__OBJECT o ON o.OID = c.TYPE_OID
        WHERE o.TABLE_OID = ?1

        UNION ALL 

        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__SELECT s ON s.OID = c.TYPE_OID
        WHERE s.TABLE_OID = ?1

        UNION ALL 

        SELECT 
            c.SCHEMA_OID, 
            c.IS_PRIMARY_KEY 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT s ON s.OID = c.TYPE_OID
        WHERE s.TABLE_OID = ?1
        ")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("SCHEMA_OID")?, row.get::<_, bool>("IS_PRIMARY_KEY")?)))? {
        
        let (referencing_schema_oid, referenced_in_label) = row_result?;
        drop_views_associated_with_label(trans, referencing_schema_oid, referenced_in_label, create_schema_oid_seq)?;
    }

    // Drop the associated views
    let drop_sql: String = format!("
        DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW;
        DROP VIEW IF EXISTS TABLE{schema_oid}_POLYMORPHISM_VIEW;
    ");
    trans.execute_batch(&drop_sql)?;
    Ok(())
}

/// Drop the views that reference the label of another schema.
fn drop_views_associated_with_label(trans: &Transaction, schema_oid: i64, drop_label_view: bool, create_schema_oid_seq: &mut Vec<ViewsToCreate>) -> Result<(), Error> {
    create_schema_oid_seq.push(ViewsToCreate { 
        schema_oid, 
        create_label_view: drop_label_view.clone(), 
        create_polymorphism_view: false 
    });

    // Drop the primary schema view
    let drop_main_view_sql: String = format!("DROP VIEW IF EXISTS SCHEMA{schema_oid}_VIEW");
    trans.execute(&drop_main_view_sql, [])?;

    if drop_label_view {
        // Drop the views associated with any inheritor schema
        // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
        for row_result in trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?.query_map(params![schema_oid], |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"))? {
            let inheritor_schema_oid: i64 = row_result?;
            drop_views_associated_with_label(trans, inheritor_schema_oid, true, create_schema_oid_seq)?;
        }

        // Drop all views that require that label view
        for row_result in trans.prepare("
            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__OBJECT o ON o.OID = c.TYPE_OID
            WHERE o.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__SELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1

            UNION ALL 

            SELECT 
                c.SCHEMA_OID, 
                c.IS_PRIMARY_KEY 
            FROM METADATA_COLUMN c 
            INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT s ON s.OID = c.TYPE_OID
            WHERE s.TABLE_OID = ?1
            ")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("SCHEMA_OID")?, row.get::<_, bool>("IS_PRIMARY_KEY")?)))? {
            
            let (referencing_schema_oid, referenced_in_label) = row_result?;
            drop_views_associated_with_label(trans, referencing_schema_oid, referenced_in_label, create_schema_oid_seq)?;
        }

        // Drop the label view
        let drop_label_view_sql: String = format!("DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW");
        trans.execute(&drop_label_view_sql, [])?;
    }
    Ok(())
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
                WHERE NOT t.TRASH
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
                WHERE NOT t.TRASH
                "
            )
        }
    );
    Ok(())
}

/// Create a view describing the lowest-level table that has a row inheriting from a particular row in the table.
fn create_table_polymorphism_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let final_cte_name: String = format!("TABLE{table_oid}_POLYMORPHISM_CTE");
    let view_name: String = format!("TABLE{table_oid}_POLYMORPHISM_VIEW");

    // Compile all necessary CTEs
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    compile_polymorphism_cte(trans, table_oid.clone(), &mut compiled_cte)?;

    // Compile and create the final view
    if let Some(final_cte) = compiled_cte.remove(&final_cte_name) {
        let create_sql: String = format!(
            "
            CREATE VIEW IF NOT EXISTS {view_name} AS 
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
                if compile_label_cte(trans, object_table_oid, compiled_cte)? {
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
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
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
                if compile_label_cte(trans, select_table_oid, compiled_cte)? {
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

/// Compiles a CTE to get the label for a particular row in a table.
fn compile_label_cte(trans: &Transaction, table_oid: i64, compiled_cte: &mut HashMap<String, String>) -> Result<bool, Error> {
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
        if compile_label_cte(trans, inheritor_table_oid, compiled_cte)? {
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
                                WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                                WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
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
                    WHEN COUNT(k.JSON_LABEL) = 0 THEN '— NO PRIMARY KEY —'
                    WHEN COUNT(k.JSON_LABEL) = 1 THEN MIN(k.PLAIN_LABEL)
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

/// Create a view for the label of each row in the table.
fn create_table_label_view(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let mut compiled_cte: HashMap<String, String> = HashMap::new();
    let create_sql: String = if compile_label_cte(trans, table_oid, &mut compiled_cte)? {
        format!(
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
            compiled_cte.into_iter().map(|(cte_name, cte_definition)| format!("{cte_name} AS ({cte_definition})")).reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(format!("
                TABLE{table_oid}_LABEL_CTE AS (
                    SELECT
                        OID,
                        SCHEMA_OID,
                        ROW_OID,
                        '...' AS SELECT_LABEL,
                        '{{ ... }}' AS JSON_LABEL
                    FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
                )
            "))
        )
    } else {
        // Create a label view with dummy labels
        format!(
            "
            CREATE VIEW IF NOT EXISTS TABLE{table_oid}_LABEL_VIEW AS 
            SELECT
                OID,
                SCHEMA_OID,
                ROW_OID,
                '...' AS SELECT_LABEL,
                '{{ ... }}' AS JSON_LABEL,
                '{{\"' || REPLACE(REPLACE(s.NAME, '\\', '\\\\'), '\"', '\\\"') || '\": {{ ... }}}}' AS OBJECT_LABEL
            FROM TABLE{table_oid}_POLYMORPHISM_VIEW p 
            INNER JOIN METADATA_SCHEMA s ON s.OID = p.SCHEMA_OID
            "
        )
    };
    trans.execute(&create_sql, [])?;
    Ok(())
}



enum SchemaViewColumn {
    TableData {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String
    },
    Formula {
        label_expr: String,
        label_ord: String,
        value_expr: String,
        value_ord: String,
        param_expr: String,
        param_ord: String
    }
}

impl SchemaViewColumn {
    /// Compiles the column.
    pub fn compile(&self) -> Result<String, Error> {
        Ok(match self {
            Self::TableData { label_expr, label_ord, value_expr, value_ord } =>
                format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}"),
            Self::Formula { label_expr, label_ord, value_expr, value_ord, param_expr, param_ord } => {
                format!("{label_expr} AS {label_ord}, {value_expr} AS {value_ord}, {param_expr} AS {param_ord}")
            }
        })
    }
}

#[derive(PartialEq, Eq, Clone)]
struct PrimitiveScalarType(u32);
bitflags! {
    impl PrimitiveScalarType: u32 {
        const Null          = 0b00000000;
        const AnyPrimitive  = 0b11111111;
        const Boolean       = 0b00000001;
        const Integer       = 0b00000010;
        const Number        = 0b00000110;
        const Date          = 0b00001000;
        const Datetime      = 0b00011000;
        const Text          = 0b00100000;
        const JSON          = 0b01000000;

        /// File data is represented as an OID in the METADATA_FILE table.
        const File          = 0b10000000;
    }
}

impl PrimitiveScalarType {
    /// Converts from a scalar type to a string.
    fn to_string(&self) -> String {
        let mut flags: Vec<Self> = self.iter().collect();
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
            Self::AnyPrimitive => String::from("primitive"),
            Self::Boolean => String::from("boolean"),
            Self::Integer => String::from("integer"),
            Self::Number => String::from("number"),
            Self::Date => String::from("date"),
            Self::Datetime => String::from("timestamp"),
            Self::Text => String::from("text"),
            Self::JSON => String::from("JSON"),
            Self::File => String::from("file"),
            _ => String::from("unknown") // This case shouldn't ever happen; if it does, something has gone wrong
        }).reduce(|acc, e| format!("{acc} | {e}")).unwrap_or(String::from("null"))
    }
}

#[derive(PartialEq, Eq, Clone)]
enum ExpressionReturnType {
    // Any possible value or reference.
    Any,

    // A more specific type.
    Selected {
        // The type can take on a primitive value.
        primitive: PrimitiveScalarType,

        // The type can take on a reference to a record in one of the indicated tables.
        record_in_table_oid: HashSet<i64>
    }
}

impl ExpressionReturnType {
    /// Construct a new type representing a primitive value.
    pub fn new_primitive(primitive: PrimitiveScalarType) -> Self { 
        Self::Selected { primitive, record_in_table_oid: HashSet::new() }
    }

    /// Construct a new type representing a reference to a record in the table with the provided OID.
    pub fn new_record(table_oid: i64) -> Self {
        let mut record_in_table_oid: HashSet<i64> = HashSet::new();
        record_in_table_oid.insert(table_oid);
        Self::Selected {
            primitive: PrimitiveScalarType::Null,
            record_in_table_oid
        }
    }

    /// Returns true if a parameter of this type can accept an argument of the given type.
    /// In other words, returns true if this type is equivalent to or a supertype of the given type.
    pub fn accepts_arg(&self, other: &ExpressionReturnType) -> bool {
        match self {
            Self::Any => true,
            Self::Selected { primitive: self_primitive, record_in_table_oid: self_table_oid } => match other {
                Self::Any => false,
                Self::Selected { primitive: other_primitive, record_in_table_oid: other_table_oid } => {
                    self_primitive.contains(other_primitive.clone()) && self_table_oid.is_superset(other_table_oid)
                }
            }
        }
    }

    /// Returns a type that encompasses both this type and the given type.
    pub fn generalize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => Self::Any,
            Self::Selected { primitive: self_primitive, record_in_table_oid: self_table_oid } => match other {
                Self::Any => Self::Any,
                Self::Selected { primitive: other_primitive, record_in_table_oid: other_table_oid } => Self::Selected {
                    primitive: self_primitive.clone() | other_primitive.clone(),
                    record_in_table_oid: self_table_oid.union(other_table_oid).map(|ir| ir.clone()).collect()
                }
            }
        }
    }

    /// Returns a type that is encompassed by both this type and the given type.
    pub fn specialize(&self, other: &ExpressionReturnType) -> Self {
        match self {
            Self::Any => other.clone(),
            Self::Selected { primitive: self_primitive, record_in_table_oid: self_table_oid } => match other {
                Self::Any => Self::Selected {
                    primitive: self_primitive.clone(),
                    record_in_table_oid: self_table_oid.clone()
                },
                Self::Selected { primitive: other_primitive, record_in_table_oid: other_table_oid } => Self::Selected {
                    primitive: self_primitive.clone() & other_primitive.clone(),
                    record_in_table_oid: self_table_oid.intersection(other_table_oid).map(|ir| ir.clone()).collect()
                }
            }
        }
    }


    /// Converts the expression return type to a string.
    pub fn to_string(&self, conn: &Connection) -> String {
        match self {
            Self::Any => format!("any"),
            Self::Selected { primitive, record_in_table_oid } => {
                let mut record_types: Vec<String> = Vec::new();
                if record_in_table_oid.len() > 0 {
                    for table_oid in record_in_table_oid {
                        if let Ok(schema_metadata) = schema::FullMetadata::get(conn, table_oid.clone()) {
                            record_types.push(format!("record [{}]", schema_metadata.name));
                        } else {
                            record_types.push(String::from("record [-ERROR-]"));
                        }
                    }

                    if primitive == &PrimitiveScalarType::Null {
                        record_types.into_iter().reduce(|acc, e| format!("{acc} | {e}")).unwrap_or(String::from("null"))
                    } else {
                        record_types.into_iter().fold(primitive.to_string(), |acc, e| format!("{acc} | {e}"))
                    }
                } else {
                    primitive.to_string()
                }
            }
        }
    } 
}



#[derive(Clone)]
struct ParamCTEColumnCell {
    /// The OID of the table. May be different from the schema OID indicated by the column, if the cell has a reversed relationship.
    table_oid: i64,

    /// The OID of the column.
    column_oid: i64,

    /// The ordinal of the row OID.
    row_ord: String
}

#[derive(Clone)]
struct ParamCTEColumn {
    /// The expression for the label.
    /// Always returns a string that is 1-to-1 with its datasource.
    label_expr: String,

    /// The ordinal of the label.
    label_ord: String,

    /// The expression for the value.
    /// Always returns a value that is 1-to-1 with its datasource.
    value_expr: String,

    /// The ordinal of the value.
    value_ord: String,

    /// The expression for the parameter as an argument to a formula.
    /// May return a value that is 1-to-* with its datasource, in the case of reversed Object columns, reversed Select columns, or normal/reversed Multiselect columns.
    arg_expr: String,

    /// The type of the parameter as an argument to a formula.
    arg_type: ExpressionReturnType,

    /// The identifier for the column and row.
    cell: Option<ParamCTEColumnCell>
}

struct ParamCTE {
    datasource: Datasource,
    child_datasources: HashSet<Datasource>,
    columns: HashMap<String, ParamCTEColumn>
}

impl ParamCTE {
    /// Compiles the CTE.
    pub fn compile(self) -> Result<String, Error> {
        let datasource_alias: String = self.datasource.get_alias();
        let datasource_schema_oid: i64 = self.datasource.get_schema_oid()?;

        // If datasource is an Object or Select column with a reversed relationship, make sure that column is included in the CTE
        // If datasource is an inheritor table, make sure the OID of the master table is included in the CTE
        let key: String = match &self.datasource {
            Datasource::InheritorTable { parent_datasource, .. } => format!(", d.MASTER{}_OID AS {datasource_alias}_KEY", parent_datasource.get_schema_oid()?),
            Datasource::Column { column, .. } => {
                match column.column_type {
                    column_type::ColumnType::Object { table_oid, .. }
                    | column_type::ColumnType::Select { table_oid, .. } => {
                        if column.schema.oid == datasource_schema_oid {
                            // Relationship is reversed
                            format!(", d.COLUMN{} AS {datasource_alias}_KEY", column.oid)
                        } else {
                            String::from("")
                        }
                    }
                    _ => String::from("")
                }
            }
            _ => String::from("")
        };

        // Select the columns for OIDs/parameters from child datasources and the FROM/JOIN tables/CTEs
        let (oid_columns, datasources) = self.child_datasources.into_iter().map(|child_datasource| {
            let child_datasource_alias: String = child_datasource.get_alias();
            (
                format!("{child_datasource_alias}.*"),
                match &child_datasource {
                    Datasource::Table { .. } => {
                        // This case shouldn't happen
                        format!("INNER JOIN {child_datasource_alias}")
                    }
                    Datasource::MasterTable { table_oid, .. } => {
                        format!("INNER JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.MASTER{table_oid}_OID")
                    }
                    Datasource::InheritorTable { .. } => {
                        format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                    }
                    Datasource::Column { column, .. } => {
                        match column.column_type {
                            column_type::ColumnType::Object { table_oid, .. }
                            | column_type::ColumnType::Select { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID = d.COLUMN{}", column.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_KEY = d.OID")
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            column_type::ColumnType::Multiselect { table_oid, .. } => {
                                if column.schema.oid == datasource_schema_oid {
                                    // Is normal
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{table_oid}_OID FROM MULTISELECT{} m WHERE m.TABLE{}_OID = d.OID)", column.oid, column.schema.oid)
                                } else if table_oid == datasource_schema_oid {
                                    // Is reversed
                                    format!("LEFT JOIN {child_datasource_alias} ON {child_datasource_alias}.{child_datasource_alias}_OID IN (SELECT m.TABLE{}_OID FROM MULTISELECT{} m WHERE m.TABLE{table_oid}_OID = d.OID)", column.schema.oid, column.oid)
                                } else {
                                    // Invalid relationship
                                    format!("LEFT JOIN {child_datasource_alias} ON FALSE")
                                }
                            }
                            _ => {
                                todo!("Error message here?")
                            }
                        }
                    }
                }
            )
        }).fold(
            (
                format!("d.OID AS {datasource_alias}_OID{key}"), 
                format!("FROM TABLE{datasource_schema_oid} d")
            ), 
            |(acc1, acc2), (e1, e2)| (format!("{acc1}, {e1}"), format!("{acc2} {e2}"))
        );

        // Compile all columns
        let all_columns: String = self.columns.into_iter().map(|(_, column)| format!("{} AS {}, {} AS {}", column.label_expr, column.label_ord, column.value_expr, column.value_ord)).fold(oid_columns, |acc, e| format!("{acc}, {e}"));

        Ok(format!(
            "
            {datasource_alias} AS (
                SELECT 
                    {all_columns} 
                {datasources}
                WHERE NOT d.TRASH
            )
            "
        ))
    }
}

/// Adds a CTE for params from a datasource.
fn add_datasource_cte(param_cte: &mut HashMap<Datasource, ParamCTE>, datasource: &Datasource) {
    if !param_cte.contains_key(&datasource) {
        // Add the parent datasource
        if let Some(parent_datasource) = datasource.get_parent() {
            add_datasource_cte(param_cte, &parent_datasource);

            // Link the datasource to its parent
            if let Some(parent_datasource_cte) = param_cte.get_mut(&parent_datasource) {
                parent_datasource_cte.child_datasources.insert(datasource.clone());
            }
        }

        // Add a CTE for the datasource
        param_cte.insert(datasource.clone(), ParamCTE {
            datasource: datasource.clone(),
            child_datasources: HashSet::new(),
            columns: HashMap::new()
        });
    }
}

/// Adds a parameter to a datasource.
fn add_param<'a>(param_cte: &'a mut HashMap<Datasource, ParamCTE>, datasource: Datasource, column: column::FullMetadata) -> Result<(Datasource, ParamCTEColumn), Error> {
    // Ensure the CTE for the datasource exists
    add_datasource_cte(param_cte, &datasource);

    // Add the column to that CTE
    let column_path: String = format!("{}_COLUMN{}", datasource.get_alias(), column.oid);
    let param = if let Some(datasource_cte) = param_cte.get_mut(&datasource) {
        if !datasource_cte.columns.contains_key(&column_path) {
            datasource_cte.columns.insert(column_path.clone(), match column.column_type {
                column_type::ColumnType::Primitive(prim) => {
                    match prim {
                        column_type::Primitive::Checkbox => ParamCTEColumn { 
                            label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()),  
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::Integer => ParamCTEColumn { 
                            label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()),  
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::Number => ParamCTEColumn { 
                            label_expr: format!("CAST(d.COLUMN{} AS TEXT)", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()),  
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::Text => ParamCTEColumn { 
                            label_expr: format!("d.COLUMN{}", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()),  
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::JSON => ParamCTEColumn { 
                            label_expr: format!("d.COLUMN{}", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::JSON),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::Date => ParamCTEColumn { 
                            label_expr: format!("DATE(d.COLUMN{}, 'julianday')", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Date),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::Datetime => ParamCTEColumn { 
                            label_expr: format!("STRFTIME('%FT%TZ', d.COLUMN{}, 'julianday')", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Datetime),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        },
                        column_type::Primitive::File | column_type::Primitive::Image => ParamCTEColumn { 
                            label_expr: format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = d.COLUMN{})", column.oid), 
                            label_ord: format!("{column_path}_LABEL"), 
                            value_expr: format!("d.COLUMN{}", column.oid), 
                            value_ord: format!("{column_path}_VALUE"), 
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::File),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    }
                }
                column_type::ColumnType::Object { table_oid, .. } => {
                    if column.schema.oid == datasource.get_schema_oid()? {
                        // Is normal
                        ParamCTEColumn {
                            label_expr: format!("(SELECT l.OBJECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("d.COLUMN{}", column.oid),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_record(table_oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    } else {
                        // Is reversed
                        ParamCTEColumn {
                            label_expr: format!("
                                (
                                    SELECT 
                                        '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                    FROM TABLE{table_oid}_LABEL_VIEW l 
                                    WHERE l.COLUMN{} = d.OID
                                )
                                ", 
                                column.oid
                            ),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("
                                (
                                    SELECT 
                                        GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                    FROM TABLE{} s
                                    WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                )
                                ", 
                                column.schema.oid,
                                column.oid
                            ),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("
                                (
                                    SELECT
                                        s.OID
                                    FROM TABLE{} s
                                    WHERE s.COLUMN{} = {}.{}_OID AND NOT s.TRASH
                                )
                                ", 
                                column.schema.oid, 
                                column.oid,
                                datasource.seek_root().get_alias(),
                                datasource.get_alias()
                            ), 
                            arg_type: ExpressionReturnType::new_record(column.schema.oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    }
                }
                column_type::ColumnType::Select { table_oid, .. } => {
                    if column.schema.oid == datasource.get_schema_oid()? {
                        // Is normal
                        ParamCTEColumn {
                            label_expr: format!("(SELECT l.SELECT_LABEL FROM TABLE{table_oid}_LABEL_VIEW l WHERE l.OID = d.COLUMN{})", column.oid),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("d.COLUMN{}", column.oid),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("{}.{column_path}_VALUE", datasource.seek_root().get_alias()), 
                            arg_type: ExpressionReturnType::new_record(table_oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    } else {
                        // Is reversed
                        ParamCTEColumn {
                            label_expr: format!("
                                (
                                    SELECT 
                                        '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                    FROM TABLE{table_oid}_LABEL_VIEW l 
                                    WHERE l.COLUMN{} = d.OID
                                )
                                ", 
                                column.oid
                            ),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("
                                (
                                    SELECT 
                                        GROUP_CONCAT(CAST(s.OID AS TEXT), ',') 
                                    FROM TABLE{} s
                                    WHERE s.COLUMN{} = d.OID AND NOT s.TRASH
                                )
                                ", 
                                column.schema.oid,
                                column.oid
                            ),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("
                                (
                                    SELECT
                                        s.OID
                                    FROM TABLE{} s
                                    WHERE s.COLUMN{} = {}.{}_OID AND NOT s.TRASH
                                )
                                ", 
                                column.schema.oid, 
                                column.oid,
                                datasource.seek_root().get_alias(),
                                datasource.get_alias()
                            ), 
                            arg_type: ExpressionReturnType::new_record(column.schema.oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    }
                }
                column_type::ColumnType::Multiselect { table_oid, .. } => {
                    if column.schema.oid == datasource.get_schema_oid()? {
                        // Is normal
                        ParamCTEColumn {
                            label_expr: format!("
                                (
                                    SELECT 
                                        '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                    FROM MULTISELECT{} m 
                                    INNER JOIN TABLE{table_oid}_LABEL_VIEW l ON l.OID = m.TABLE{table_oid}_OID 
                                    WHERE m.TABLE{}_OID = d.OID
                                )", 
                                column.oid,
                                column.schema.oid
                            ),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("
                                (
                                    SELECT 
                                        GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                    FROM MULTISELECT{} m 
                                    INNER JOIN TABLE{table_oid} t ON t.OID = m.TABLE{table_oid}_OID 
                                    WHERE m.TABLE{}_OID = d.OID AND NOT t.TRASH
                                )", 
                                column.oid,
                                column.schema.oid
                            ),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("
                                (
                                    SELECT 
                                        TABLE{table_oid}_OID
                                    FROM MULTISELECT{}
                                    WHERE TABLE{}_OID = {}.{}_OID AND TABLE{table_oid}_OID IN (SELECT OID FROM TABLE{table_oid} WHERE NOT TRASH)
                                )", 
                                column.oid,
                                column.schema.oid,
                                datasource.seek_root().get_alias(),
                                datasource.get_alias()
                            ),
                            arg_type: ExpressionReturnType::new_record(table_oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid: column.schema.oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    } else {
                        // Is reversed
                        ParamCTEColumn {
                            label_expr: format!("
                                (
                                    SELECT 
                                        '[' || COALESCE(GROUP_CONCAT(l.JSON_LABEL, ', '), '') || ']'
                                    FROM MULTISELECT{} m 
                                    INNER JOIN TABLE{}_LABEL_VIEW l ON l.OID = m.TABLE{}_OID 
                                    WHERE m.TABLE{table_oid}_OID = d.OID
                                )", 
                                column.oid,
                                column.schema.oid,
                                column.schema.oid
                            ),
                            label_ord: format!("{column_path}_LABEL"),
                            value_expr: format!("
                                (
                                    SELECT 
                                        GROUP_CONCAT(CAST(t.OID AS TEXT), ',')
                                    FROM MULTISELECT{} m 
                                    INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                    WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                )", 
                                column.oid,
                                column.schema.oid,
                                column.schema.oid 
                            ),
                            value_ord: format!("{column_path}_VALUE"),
                            arg_expr: format!("
                                (
                                    SELECT 
                                        t.OID
                                    FROM MULTISELECT{} m 
                                    INNER JOIN TABLE{} t ON t.OID = m.TABLE{}_OID 
                                    WHERE m.TABLE{table_oid}_OID = d.OID AND NOT t.TRASH
                                )", 
                                column.oid,
                                column.schema.oid,
                                column.schema.oid 
                            ),
                            arg_type: ExpressionReturnType::new_record(column.schema.oid),
                            cell: Some(ParamCTEColumnCell {
                                table_oid,
                                column_oid: column.oid,
                                row_ord: format!("{}.{}_OID", datasource.seek_root().get_alias(), datasource.get_alias())
                            })
                        }
                    }
                }
                _ => {
                    // Not added as a parameter
                    todo!("Need error here")
                }
            });
        }
        
        datasource_cte.columns[&column_path].clone()
    } else {
        todo!("Need error here")
    };
    Ok((datasource.seek_root(), param))
}




/// Represents an expression returning a scalar value.
#[derive(PartialEq, Eq, Clone)]
struct ScalarExpression {
    /// The SQL expression resulting in a scalar value that can be used as an argument to an operator or function.
    arg_expr: String,

    /// The scalar type returned by the arg_expr SQL expression.
    arg_type: ExpressionReturnType,

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

/// Compile the formula into SQL.
fn compile_formula<'a>(trans: &Transaction, param_cte: &'a mut HashMap<Datasource, ParamCTE>, formula: Box<Formula>) -> Result<ScalarExpression, Error> {
    Ok(match *formula {
        Formula::Null => ScalarExpression {
            arg_expr: String::from("NULL"),
            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Null),
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
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Boolean),
                value_expr,
                label_expr,
                param_expr: String::from("NULL"),
                deterministic: true
            }
        }
        Formula::LiteralInt(num) => ScalarExpression {
            arg_expr: format!("{num}"),
            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
            value_expr: format!("{num}"),
            label_expr: format!("'{num}'"),
            param_expr: String::from("NULL"),
            deterministic: true
        },
        Formula::LiteralFloat(num) => ScalarExpression {
            arg_expr: format!("{num}"),
            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number),
            value_expr: format!("{num}"),
            label_expr: format!("'{num}'"),
            param_expr: String::from("NULL"),
            deterministic: true
        },
        Formula::LiteralString(str) => {
            let safe_str: String = format!("'{}'", str.replace("'", "''"));
            ScalarExpression {
                arg_expr: safe_str.clone(),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Text),
                value_expr: safe_str.clone(),
                label_expr: safe_str.clone(),
                param_expr: String::from("NULL"),
                deterministic: true
            }
        }
        Formula::RandomInt => ScalarExpression {
            arg_expr: format!("RANDOM()"),
            arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
            value_expr: format!("RANDOM()"),
            label_expr: format!("CAST(RANDOM() AS TEXT)"),
            param_expr: String::from("NULL"),
            deterministic: false
        },
        Formula::Param { datasource_alias, column_oid } => {
            let column_datasource: Datasource = Datasource::from_alias(datasource_alias.clone())?;
            let column_metadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
            match &column_metadata.column_type {
                column_type::ColumnType::Primitive(_)
                | column_type::ColumnType::Object { .. }
                | column_type::ColumnType::Select { .. }
                | column_type::ColumnType::Multiselect { .. } => {
                    let (_, param) = add_param(param_cte, column_datasource, column_metadata)?;
                    
                    // Parameter expressions return a string in the form "{TABLE_OID}:{COLUMN_OID}:{ROW_OID}"
                    let param_expr: String = if let Some(param_cell) = param.cell {
                        format!("('{}:{}:' || CAST({} AS TEXT))", param_cell.table_oid, param_cell.column_oid, param_cell.row_ord)
                    } else {
                        String::from("NULL")
                    };

                    // 
                    ScalarExpression {
                        arg_expr: param.arg_expr,
                        arg_type: param.arg_type,
                        value_expr: param.value_expr,
                        label_expr: param.label_expr,
                        param_expr,
                        deterministic: true
                    }
                }
                column_type::ColumnType::Formula { formula, .. } => {
                    // Parse the formula
                    let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                    // Compile the formula into a scalar expression
                    compile_formula(trans, param_cte, parsed_formula)?
                }
                _ => {
                    // Column type is not allowed to be used as a parameter in a formula
                    todo!("Error message here")
                }
            }
        }
        Formula::Coalesce(items) => {
            let mut items_compiled: Vec<ScalarExpression> = Vec::new();
            for item in items {
                let item_compiled: ScalarExpression = compile_formula(trans, param_cte, Box::new(item))?;
                items_compiled.push(item_compiled);
            }

            let deterministic: bool = items_compiled.iter().all(|item_compiled| item_compiled.deterministic);
            let arg_type: ExpressionReturnType = items_compiled.iter().fold(ExpressionReturnType::new_primitive(PrimitiveScalarType::Null), |acc, item_compiled| acc.generalize(&item_compiled.arg_type));
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
                arg_type,
                value_expr,
                label_expr,
                param_expr,
                deterministic
            }
        }
        Formula::Abs(inner) => {
            let inner_name: String = inner.to_string();
            let inner_compiled: ScalarExpression = compile_formula(trans, param_cte, inner)?;
            if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).accepts_arg(&inner_compiled.arg_type) {
                return Err(Error::FormulaTypeValidationError { 
                    outer_name: "abs", 
                    inner_name, 
                    expected_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).to_string(trans), 
                    received_type: inner_compiled.arg_type.to_string(trans) 
                });
            }

            let value_expr: String = format!("ABS({})", inner_compiled.arg_expr);
            let label_expr: String = format!("CAST({value_expr} AS TEXT)");

            ScalarExpression {
                arg_expr: value_expr.clone(),
                arg_type: inner_compiled.arg_type,
                label_expr,
                value_expr,
                param_expr: String::from("NULL"),
                deterministic: inner_compiled.deterministic
            }
        }
        Formula::Sign(inner) => {
            let inner_name: String = inner.to_string();
            let inner_compiled: ScalarExpression = compile_formula(trans, param_cte, inner)?;
            if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).accepts_arg(&inner_compiled.arg_type) {
                return Err(Error::FormulaTypeValidationError { 
                    outer_name: "sign", 
                    inner_name, 
                    expected_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).to_string(trans), 
                    received_type: inner_compiled.arg_type.to_string(trans) 
                });
            }

            let value_expr: String = format!("SIGN({})", inner_compiled.arg_expr);
            let label_expr: String = format!("CAST({value_expr} AS TEXT)");

            ScalarExpression {
                arg_expr: value_expr.clone(),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                label_expr,
                value_expr,
                param_expr: String::from("NULL"),
                deterministic: inner_compiled.deterministic
            }
        }
        Formula::Floor(inner) => {
            let inner_name: String = inner.to_string();
            let inner_compiled: ScalarExpression = compile_formula(trans, param_cte, inner)?;
            if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).accepts_arg(&inner_compiled.arg_type) {
                return Err(Error::FormulaTypeValidationError { 
                    outer_name: "floor", 
                    inner_name, 
                    expected_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).to_string(trans), 
                    received_type: inner_compiled.arg_type.to_string(trans) 
                });
            }

            let value_expr: String = format!("FLOOR({})", inner_compiled.arg_expr);
            let label_expr: String = format!("CAST({value_expr} AS TEXT)");

            ScalarExpression {
                arg_expr: value_expr.clone(),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                label_expr,
                value_expr,
                param_expr: String::from("NULL"),
                deterministic: inner_compiled.deterministic
            }
        }
        Formula::Ceiling(inner) => {
            let inner_name: String = inner.to_string();
            let inner_compiled: ScalarExpression = compile_formula(trans, param_cte, inner)?;
            if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).accepts_arg(&inner_compiled.arg_type) {
                return Err(Error::FormulaTypeValidationError { 
                    outer_name: "ceil", 
                    inner_name, 
                    expected_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).to_string(trans), 
                    received_type: inner_compiled.arg_type.to_string(trans) 
                });
            }

            let value_expr: String = format!("CEILING({})", inner_compiled.arg_expr);
            let label_expr: String = format!("CAST({value_expr} AS TEXT)");

            ScalarExpression {
                arg_expr: value_expr.clone(),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
                label_expr,
                value_expr,
                param_expr: String::from("NULL"),
                deterministic: inner_compiled.deterministic
            }
        }
        Formula::Round(inner) => {
            let inner_name: String = inner.to_string();
            let inner_compiled: ScalarExpression = compile_formula(trans, param_cte, inner)?;
            if !ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).accepts_arg(&inner_compiled.arg_type) {
                return Err(Error::FormulaTypeValidationError { 
                    outer_name: "round", 
                    inner_name, 
                    expected_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Number).to_string(trans), 
                    received_type: inner_compiled.arg_type.to_string(trans) 
                });
            }

            let value_expr: String = format!("ROUND({})", inner_compiled.arg_expr);
            let label_expr: String = format!("CAST({value_expr} AS TEXT)");

            ScalarExpression {
                arg_expr: value_expr.clone(),
                arg_type: ExpressionReturnType::new_primitive(PrimitiveScalarType::Integer),
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



/// Create a view for the table.
fn create_schema_view(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Get the root table datasource for the view
    let mut param_cte: HashMap<Datasource, ParamCTE> = HashMap::new();
    let root_datasource: Option<Datasource> = if let Some(root_datasource_oid) = trans.query_one("SELECT OID FROM METADATA_DATASOURCE WHERE TABLE_OID = ?1 LIMIT 1", params![schema_oid], |row| row.get("OID")).optional()? {
        Some(Datasource::get_transact(trans, root_datasource_oid)?)
    } else {
        None 
    };

    // Add each column that belongs to the schema
    let mut view_columns: HashMap<i64, SchemaViewColumn> = HashMap::new();
    for row_result in trans.prepare("SELECT DATASOURCE_PATH, COLUMN_OID FROM METADATA_SCHEMA_COLUMN_VIEW WHERE SCHEMA_OID = ?1 AND IS_REQUIRED")?.query_map(params![schema_oid], |row| Ok((row.get::<_, String>("DATASOURCE_PATH")?, row.get::<_, i64>("COLUMN_OID")?)))? {
        let (datasource_path, column_oid) = row_result?;
        let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid.clone())?;
        match &column_metadata.column_type {
            column_type::ColumnType::Primitive(_) 
            | column_type::ColumnType::Object { .. } 
            | column_type::ColumnType::Select { .. } 
            | column_type::ColumnType::Multiselect { .. } => {
                if let Some(root_datasource) = &root_datasource {
                    // Add the primitive column as a param
                    let column_datasource: Datasource = root_datasource.append_path(datasource_path)?;
                    let (access_datasource, access_param) = add_param(&mut param_cte, column_datasource, column_metadata)?;

                    // Register the column to the query
                    view_columns.insert(column_oid.clone(), SchemaViewColumn::TableData { 
                        label_expr: format!("{}.{}", access_datasource.get_alias(), access_param.label_ord), 
                        label_ord: format!("COLUMN{}_LABEL", column_oid), 
                        value_expr: format!("{}.{}", access_datasource.get_alias(), access_param.value_ord), 
                        value_ord: format!("COLUMN{}_VALUE", column_oid)
                    });
                } else {
                    return Err(Error::OrphanedDataColumn { 
                        column_oid: column_metadata.oid, 
                        column_name: column_metadata.name 
                    });
                };
            }
            column_type::ColumnType::Formula { formula, .. } => {
                // Parse the formula
                let parsed_formula: Box<Formula> = Box::new(Formula::parse(formula.clone())?);

                // Compile the formula into SQL
                let scalar_expression: ScalarExpression = compile_formula(trans, &mut param_cte, parsed_formula)?;

                // Turn into a column
                view_columns.insert(column_oid, SchemaViewColumn::Formula { 
                    label_expr: scalar_expression.label_expr, 
                    label_ord: format!("COLUMN{}_LABEL", column_metadata.oid), 
                    value_expr: scalar_expression.value_expr, 
                    value_ord: format!("COLUMN{}_VALUE", column_metadata.oid), 
                    param_expr: scalar_expression.param_expr, 
                    param_ord: format!("COLUMN{}_PARAM", column_metadata.oid) 
                });
            }
            _ => {
                // Ignore other virtual column types
            }
        }
    }

    // Compile the CTEs and select only from root datasources
    let (with_expr, oid_expr, from_expr) = if param_cte.len() > 0 {
        let mut with_expr: String = String::from("WITH");
        let mut oid_expr: Vec<String> = Vec::new();
        let mut filter_expr: String = String::from("");
        let mut from_expr: String = String::from("FROM");

        let mut basis_datasource: HashSet<Datasource> = HashSet::new();
        for (cte_datasource, cte) in param_cte.into_iter() {
            let cte_root_datasource: Datasource = cte_datasource.seek_root();
            basis_datasource.insert(cte_datasource.seek_basis()?);

            let cte_datasource_alias: String = cte_datasource.get_alias();
            let cte_root_datasource_alias: String = cte_root_datasource.get_alias();
            
            // Compile the CTE
            with_expr = format!("{with_expr}{} {}", if with_expr == "WITH" { "" } else { "," }, cte.compile()?);

            // Select OID from the datasource
            oid_expr.push(format!("{cte_root_datasource_alias}.{cte_datasource_alias}_OID"));
            filter_expr = format!("{filter_expr}{}{}", if filter_expr == "" { "'" } else { " || '&" }, format!("{cte_datasource_alias}=' || CAST({cte_root_datasource_alias}.{cte_datasource_alias}_OID AS TEXT)"));

            // If the datasource is a root, select from it
            if let Datasource::Table { .. } = cte_datasource {
                from_expr = format!("{from_expr}{} {}", if from_expr == "FROM" { "" } else { " INNER JOIN" }, cte_datasource.get_alias());
            }
        }
        (
            with_expr, 
            oid_expr.into_iter().fold(
                format!(
                    "{} AS QUERY_FILTER{}", 
                    if filter_expr == "" { String::from("''") } else { filter_expr },
                    if basis_datasource.len() == 1 {
                        let basis_datasource: Datasource = basis_datasource.into_iter().next().unwrap();
                        format!(
                            ", {} AS TABLE_OID, {}.{}_OID AS ROW_OID", 
                            basis_datasource.get_schema_oid()?, 
                            basis_datasource.seek_root().get_alias(), 
                            basis_datasource.get_alias()
                        )
                    } else {
                        String::from(", NULL AS TABLE_OID, NULL AS ROW_OID")
                    }
                ),
                |acc, e| format!("{acc}, {e}")
            ), 
            from_expr
        )
    } else {
        (
            String::from(""), 
            String::from("'' AS QUERY_FILTER, NULL AS TABLE_OID, NULL AS ROW_OID"), 
            String::from("")
        )
    };

    // Compile ordering columns
    let mut index_ordering_expr: String = String::from("");
    for row_result in trans.prepare("SELECT COLUMN_OID, SORT_ASCENDING FROM METADATA_SCHEMA_ORDERBY_VIEW WHERE SCHEMA_OID = ?1 ORDER BY ORDERING")?.query_map(params![schema_oid], |row| Ok((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)))? {
        let (column_oid, sort_ascending) = row_result?;

        if let Some(c) = view_columns.get(&column_oid) {
            if index_ordering_expr == "" {
                index_ordering_expr = format!("{} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
            } else {
                index_ordering_expr = format!("{index_ordering_expr}, {} {}", c.compile()?, if sort_ascending { "ASC" } else { "DESC" });
            }
        }
    }

    // Compile each column
    let column_expr: String = {
        let mut column_expr: Vec<String> = Vec::new();
        for (_, c) in view_columns.into_iter() {
            column_expr.push(c.compile()?);
        }
        column_expr.into_iter().fold(oid_expr, |acc, e| format!("{acc}, {e}"))
    };

    // Compile the view
    let create_sql: String = format!(
        "
        CREATE VIEW IF NOT EXISTS SCHEMA{schema_oid}_VIEW AS 
            {with_expr}
            SELECT 
                ROW_NUMBER() OVER ({index_ordering_expr}) AS ROW_INDEX, {column_expr}
            {from_expr}
        "
    );
    trans.execute(&create_sql, [])?;
    Ok(())
}


/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    let mut view_creation_ordering: Vec<ViewsToCreate> = Vec::new();
    drop_all_views(trans, schema_oid, &mut view_creation_ordering)?;

    // Create all of the polymorphism views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_polymorphism_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_polymorphism_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the label views
    for v in view_creation_ordering.iter() {
        let table_name: String = format!("TABLE{}", v.schema_oid);
        if v.create_label_view && trans.table_exists(Some("main"), &table_name)? {
            create_table_label_view(trans, v.schema_oid.clone())?;
        }
    }

    // Create all of the schema views
    for v in view_creation_ordering.iter() {
        create_schema_view(trans, v.schema_oid)?;
    }
    Ok(())
}