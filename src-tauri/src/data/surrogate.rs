use std::collections::{HashMap, HashSet};
use rusqlite::{Transaction, params, types::Value, vtab::array::Array};
use crate::util::error::Error;

/// Regenerates the surrogate view for the given table and all other tables that may reference one of its columns in their surrogate view.
pub fn regenerate_surrogate(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    // Drop every surrogate view that is dependent on this table
    let mut dependencies: HashMap<i64, HashSet<i64>> = HashMap::new();
    drop_surrogate(trans, table_oid, &mut dependencies, Vec::new())?;

    // Re-create all of the surrogate views
    while dependencies.len() > 0 {
        let next_table_oid: i64 = {
            // Find the next table with no remaining dependencies
            let mut iter = dependencies.iter();
            loop { 
                if let Some((k_oid, k_dependencies)) = iter.next() {
                    // Create an arbitrary surrogate view that has no remaining dependent surrogate views that need to be created
                    if !k_dependencies.iter().any(|i| dependencies.contains_key(i)) {
                        break k_oid.clone();
                    }
                } else {
                    return Err(Error::AdhocError("The primary key is recursively self-referential!"));
                }
            }
        };
        dependencies.remove(&next_table_oid);
        create_surrogate(trans, next_table_oid)?;
    }
    Ok(())
}

/// Drops the surrogate views for all tables related to the given table or dependent on the surrogate view of the given table.
fn drop_surrogate(trans: &Transaction, table_oid: i64, dependencies: &mut HashMap<i64, HashSet<i64>>, dependent_on: Vec<HashSet<i64>>) -> Result<(), Error> {
    // Get all related table OIDs
    let related_table_oid_results: Vec<_> = trans.prepare(
            "
            SELECT ?1 AS OID
            UNION 
            SELECT MASTER_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE INHERITOR_SCHEMA_OID = ?1
            UNION
            SELECT INHERITOR_SCHEMA_OID AS OID FROM METADATA_SCHEMA_INHERITANCE_VIEW WHERE MASTER_SCHEMA_OID = ?1
            "
        )?
        .query_map(params![table_oid], |row| row.get::<_, i64>("OID"))?
        .collect();
    let mut related_table_oids: HashSet<i64> = HashSet::new();
    for related_table_oid_result in related_table_oid_results {
        related_table_oids.insert(related_table_oid_result?);
    }

    // Ensure there is not a recursive loop of primary keys referencing each other
    if dependent_on.iter().any(|dependent_on_table_oid| related_table_oids.intersection(dependent_on_table_oid).any(|_| true)) {
        if dependent_on.len() == 1 {
            return Err(Error::AdhocError("The primary key may be recursively self-referential!"));
        } else {
            return Err(Error::AdhocError("There is a recursive loop of primary keys on different tables that cause this primary key to be self-referential!"));
        }
    }

    // Drop every surrogate view that is dependent on this table or any related table
    if !dependencies.contains_key(&table_oid) {
        for related_table_oid in related_table_oids.iter() {
            dependencies.insert(related_table_oid.clone(), HashSet::new());
        }

        // Drop the surrogate views that are dependent on this table's surrogate view, because one of their primary keys is a reference to this table
        let dependent_table_oid_results: Vec<_> = trans.prepare(
                "
                SELECT DISTINCT
                    c.SCHEMA_OID
                FROM (
                    SELECT OID, TABLE_OID FROM MEATDATA_COLUMN_TYPE__OBJECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__SELECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__MULTISELECT
                ) ct
                INNER JOIN METADATA_COLUMN c ON c.TYPE_OID = ct.OID
                WHERE ct.TABLE_OID IN ?1
                    and c.IS_PRIMARY_KEY
                    AND NOT c.TRASH
                "
            )?
            .query_map(
                params![Array::new(related_table_oids.iter().map(|i| Value::Integer(i.clone())).collect())],
                |row| row.get::<_, i64>("SCHEMA_OID")
            )?
            .collect();
        let mut next_dependent_on: Vec<HashSet<i64>> = dependent_on.clone();
        next_dependent_on.push(related_table_oids.clone());
        for dependent_table_oid_result in dependent_table_oid_results {
            drop_surrogate(trans, dependent_table_oid_result?, dependencies, next_dependent_on.clone())?;
        }
    }
    
    // Add new dependencies to the dict
    if let Some(immediately_dependent_on_table_oids) = dependent_on.iter().last() {
        for related_table_oid in related_table_oids.iter() {
            if let Some(prior_dependencies) = dependencies.get_mut(related_table_oid) {
                prior_dependencies.extend(immediately_dependent_on_table_oids);
            }
        }
    }

    // Drop the surrogate views from every related table
    for related_table_oid in related_table_oids.iter() {
        let sql_drop: String = format!("DROP VIEW IF EXISTS TABLE{related_table_oid}_SURROGATE");
        trans.execute(&sql_drop, [])?;
    }
    Ok(())
}

/// Creates the surrogate view for the given table.
fn create_surrogate(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    let query: QueryBuilder = QueryBuilder::new(Vec::new());

    // Query for datasources + primary key columns
    let columns = {
        let column_results: Vec<_> = trans.prepare(
            "
            WITH RECURSIVE ROOT_DATASOURCE (DATASOURCE_ALIAS) AS (
                SELECT 
                    'ROOT' || FORMAT('%d', MIN(OID)) AS DATASOURCE_ALIAS
                FROM METADATA_DATASOURCE
                WHERE TABLE_OID = ?1
            ), NON_OPTIONAL_DATASOURCES (DATASOURCE_ALIAS, DATASOURCE_TABLE_OID) AS (
                SELECT
                    DATASOURCE_ALIAS,
                    ?1 AS DATASOURCE_TABLE_OID
                FROM ROOT_DATASOURCE

                UNION

                SELECT
                    d.DATASOURCE_ALIAS || '_MASTER' || FORMAT('%d', inh.MASTER_SCHEMA_OID) AS DATASOURCE_ALIAS
                    inh.MASTER_SCHEMA_OID AS DATASOURCE_TABLE_OID
                FROM NON_OPTIONAL_DATASOURCES d
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.INHERITOR_SCHEMA_OID = d.DATASOURCE_TABLE_OID
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.MASTER_SCHEMA_OID
                WHERE EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = s.OID) AND NOT inh.TRASH AND NOT s.TRASH
            ), OPTIONAL_DATASOURCES (DATASOURCE_ALIAS, DATASOURCE_TABLE_OID) AS (
                SELECT
                    r.DATASOURCE_ALIAS || '_INHERITOR' || FORMAT('%d', inh.INHERITOR_SCHEMA_OID) AS DATASOURCE_ALIAS,
                    s.OID AS DATASOURCE_TABLE_OID
                FROM ROOT_DATASOURCE r
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = ?1
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID
                WHERE EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = s.OID) AND NOT inh.TRASH AND NOT s.TRASH

                UNION

                SELECT
                    d.DATASOURCE_ALIAS || '_INHERITOR' || FORMAT('%d', inh.INHERITOR_SCHEMA_OID) AS DATASOURCE_ALIAS,
                    s.OID AS DATASOURCE_TABLE_OID
                FROM NON_OPTIONAL_DATASOURCES d
                INNER JOIN METADATA_SCHEMA_INHERITANCE inh ON inh.MASTER_SCHEMA_OID = ?1
                INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID
                WHERE EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = s.OID) AND NOT inh.TRASH AND NOT s.TRASH
            )
            
            SELECT
                d.DATASOURCE_ALIAS,
                c.OID AS COLUMN_OID,
                d.IS_OPTIONAL
            FROM (
                SELECT
                    DATASOURCE_ALIAS,
                    DATASOURCE_TABLE_OID,
                    FALSE AS IS_OPTIONAL
                FROM NON_OPTIONAL_DATASOURCES

                UNION

                SELECT
                    DATASOURCE_ALIAS,
                    DATASOURCE_TABLE_OID,
                    TRUE AS IS_OPTIONAL
                FROM OPTIONAL_DATASOURCES
            ) d
            INNER JOIN METADATA_COLUMN c ON c.SCHEMA_OID = d.DATASOURCE_TABLE_OID
            WHERE c.IS_PRIMARY_KEY AND NOT c.TRASH
            ORDER BY c.ORDERING
            "
        )?
        .query_map(
            params![table_oid],
            |row| Ok::<(String, i64, bool), rusqlite::Error>((row.get("DATASOURCE_ALIAS")?, row.get("COLUMN_OID")?, row.get("IS_OPTIONAL")?))
        )?
        .collect();

        let mut columns: Vec<(datasource::Datasource, column::FullMetadata, bool)> = Vec::new();
        for column_result in column_results {
            let (datasource_alias, column_oid, column_is_optional) = column_result?;

            // Construct the datasource
            let datasource_path: Vec<String> = datasource_alias.split('_').map(|s| String::from(s)).collect();
            let datasource: datasource::Datasource = datasource::Datasource::from_path(datasource_path)?;
            
            // Construct the column metadata
            let column: column::FullMetadata = column::FullMetadata::get(column_oid)?;

            // Add the datasource + column to the list
            columns.push((datasource, column, column_is_optional));
        }
        columns
    };

    if columns.len() == 0 {
        // Table has no primary key
        let sql_view: String = format!("
            CREATE VIEW TABLE{table_oid}_SURROGATE AS 
            SELECT 
                OID, 
                '— NO PRIMARY KEY —' AS LABEL,
                'null' AS JSON_LABEL 
            FROM TABLE{table_oid}
            ");
        trans.execute(&sql_view, [])?;
    } else if columns.len() == 1 {
        // Table has a single column that serves as its primary key
        let (datasource, column, column_is_optional) = columns[0];
        let root_oid_expr: String = format!("{}.OID", datasource.get_alias().split('_').next().unwrap());
        let sql_column: String = match query.compile_column(datasource, column)? {
            query::QueryBuilderColumn::Primitive { primitive_type, label_expr, value_expr, .. } => {
                let label_expr: String = format!("COALESCE({label_expr}, '— NULL PRIMARY KEY —')");
                format!(
                    "
                    SELECT
                        {root_oid_expr},
                        {label_expr} AS LABEL,
                        {} AS JSON_LABEL 
                    ", 
                    match primitive_type {
                        column_type::Primitive::Text => format!(r#"COALESCE('"' || REPLACE(REPLACE({value_expr}, '\', '\\'), '"', '\"') || '"', 'null')"#),
                        column_type::Primitive::Checkbox => format!(r#"CASE WHEN {value_expr} IS NULL THEN 'null' WHEN {value_expr} THEN 'true' ELSE 'false' END"#),
                        column_type::Primitive::Date => format!(r#"COALESCE(DATE({value_expr}, 'julianday'), 'null')"#),
                        column_type::Primitive::Datetime => format!(r#"COALESCE(STRFTIME('%FT%TZ', {value_expr}, 'julianday'), 'null')"#),
                        column_type::Primitive::JSON => format!(r#"COALESCE({value_expr}, 'null')"#),
                        _ => format!(r#"COALESCE(FORMAT('%d', {value_expr}), 'null')"#)
                    }
                )
            }
            query::QueryBuilderColumn::Object { label_expr, json_expr, .. }
            | query::QueryBuilderColumn::Select { label_expr, json_expr, .. } => {
                let label_expr: String = format!("COALESCE({label_expr}, '— NULL PRIMARY KEY —')");
                let json_expr: String = format!("COALESCE({json_expr}, 'null')");
                format!(
                    "
                    SELECT
                        {root_oid_expr},
                        {label_expr} AS LABEL,
                        {json_expr} AS JSON_LABEL 
                    "
                )
            }
            query::QueryBuilderColumn::Multiselect { label_expr, .. } => {
                let label_expr: String = format!("COALESCE({label_expr}, '[]')");
                format!(
                    "
                    SELECT
                        {root_oid_expr},
                        {label_expr} AS LABEL,
                        {label_expr} AS JSON_LABEL 
                    "
                )
            }
            _ => {
                return Err(Error::AdhocError("Table has primary key with a column type disallowed to be a primary key."));
            }
        };
        if let Some(sql_datasources) = query.compile_datasources()? {
            let sql_view: String = format!(
                "
                CREATE VIEW TABLE{table_oid}_SURROGATE AS
                {sql_column}
                {sql_datasources}
                "
            );
            trans.execute(&sql_view, [])?;
        } else {
            return Err(Error::AdhocError("Surrogate view has no datasources.")); // This case shouldn't occur
        }
    } else {
        // Table's primary key is the combination of multiple columns
        let mut json_exprs: Vec<String> = Vec::new();
        for (idx, (datasource, column, column_is_optional)) in columns.iter().enumerate() {
            let column_name_expr: String = format!(
                r#"'{}"' || {} || '": ' || "#, 
                column.name.replace(r#"\"#, r#"\\"#).replace(r#"""#, r#"\""#)
            );
            let value_expr: String = match query.compile_column(datasource, column)? {
                query::QueryBuilderColumn::Primitive { primitive_type, label_expr, value_expr, .. } => {
                    match primitive_type {
                        column_type::Primitive::Text => format!(r#"'"' || REPLACE(REPLACE({value_expr}, '\', '\\'), '"', '\"') || '"'"#),
                        column_type::Primitive::Checkbox => format!(r#"CASE WHEN {value_expr} IS NULL THEN NULL WHEN {value_expr} THEN 'true' ELSE 'false' END"#),
                        column_type::Primitive::Date => format!(r#"DATE({value_expr}, 'julianday')"#),
                        column_type::Primitive::Datetime => format!(r#"STRFTIME('%FT%TZ', {value_expr}, 'julianday')"#),
                        column_type::Primitive::JSON => format!(r#"{value_expr}"#),
                        _ => format!(r#"CAST({value_expr} AS TEXT)"#)
                    }
                }
                query::QueryBuilderColumn::Object { label_expr, json_expr, .. }
                | query::QueryBuilderColumn::Select { label_expr, json_expr, .. } => {
                    format!("{json_expr}")
                }
                query::QueryBuilderColumn::Multiselect { label_expr, .. } => {
                    format!("{label_expr}")
                }
                _ => {
                    return Err(Error::AdhocError("Table has primary key with a column type disallowed to be a primary key."));
                }
            };
            json_exprs.push(if column_is_optional {
                format!("{column_name_expr}{value_expr}")
            } else {
                format!("{column_name_expr}COALESCE({value_expr}, 'null')")
            });
        }
        let compiled_values_expr: String = format!("VALUES ({})", json_exprs.into_iter().reduce(|acc, e| format!("{acc}), ({e}")).unwrap());
        if let Some(sql_datasources) = query.compile_datasources()? {
            let sql_view: String = format!(
                "
                CREATE VIEW TABLE{table_oid}_SURROGATE AS
                SELECT
                    {root_oid_expr},
                    '{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1, ',') OVER () FROM ({compiled_values_expr})), '') || '}' AS LABEL,
                    '{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1, ',') OVER () FROM ({compiled_values_expr})), '') || '}' AS JSON_LABEL 
                {sql_datasources}
                "
            );
            trans.execute(&sql_view, [])?;
        } else {
            return Err(Error::AdhocError("Surrogate view has no datasources.")); // This case shouldn't occur
        }
    }
    Ok(())
}