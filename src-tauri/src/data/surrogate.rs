use std::collections::{HashMap, HashSet};
use rusqlite::OptionalExtension;
use rusqlite::{Connection, Transaction, params, types::Value, vtab::array::Array};
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

    /// Determines the surrogate expression.
    pub fn get_flat(conn: &Connection, query: &mut query::QueryBuilder, datasource: Datasource, surrogate_table_oid_chain: Vec<i64>) -> Result<Self, Error> {
        let schema_oid: i64 = datasource.get_schema_oid()?;
        if surrogate_table_oid_chain.contains(&schema_oid) {
            return Err(Error::AdhocError(if surrogate_table_oid_chain.len() == 1 {
                "A table has a self-referential key!"
            } else {
                "There is a loop of recursively self-referential keys."
            }))
        }
        let oid_expr: String = format!("{}_OID", datasource.get_alias());
        
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
                let col = query.compile_column(Some(&schema_to_datasource[&base_column.schema.oid]), base_column, expanded_surrogate_table_oid_chain)?;
                json_exprs.push(format!(
                    r#"'"{}": ' || {}"#,
                    column_name.replace("\\", "\\\\").replace("\"", "\\\""),
                    col.get_json_expr(String::from("NULL"))
                ));
            }
            let json_expr: String = format!(
                "'{{' || (SELECT GROUP_CONCAT(COLUMN1) FROM (VALUES ({}))) || '}}'",
                json_exprs.into_iter().reduce(|acc, e| format!("{acc}, {e}")).unwrap_or(String::from(""))
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
            return Err(Error::AdhocError("The primary key is recursively self-referential!"));
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
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__OBJECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__SELECT
                    UNION
                    SELECT OID, TABLE_OID FROM METADATA_COLUMN_TYPE__MULTISELECT
                ) ct
                INNER JOIN METADATA_COLUMN c ON c.TYPE_OID = ct.OID
                WHERE ct.TABLE_OID IN rarray(?1)
                    AND c.IS_PRIMARY_KEY
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

    /// Builds a basic query to get all columns associated with the given schema.
    /// Also sends the column information through the provided Sender object.
    fn build_query(mut column_sender: Sender<column::FullMetadata>, schema_oid: i64, initial_datasources: Vec<datasource::Datasource>, filters: Vec<(String, i64)>) -> Result<query::QueryBuilder, Error> {
        // Construct mapping from schema to default datasource
        let mut schema_to_datasource: HashMap<i64, datasource::Datasource> = HashMap::new();
        {
            let mut conn = db::open()?;
            let trans = conn.transaction()?;

            for datasource in initial_datasources.iter() {
                schema_to_datasource.insert(datasource.get_schema_oid()?, datasource.clone());

                // Make sure all master tables of a root table are also included as a datasource
                if let datasource::Datasource::Table { table_oid, .. } = datasource {
                    let table: table::FullMetadata = table::FullMetadata::get(table_oid.clone())?;
                    Self::build_schema_to_datasource_mapping(&trans, &mut schema_to_datasource, table)?;
                }
            }

            trans.commit()?;
        }
        
        // Build query to get data for each column in the schema
        let mut query: query::QueryBuilder = query::QueryBuilder::new(initial_datasources);
        column::FullMetadata::query_by_schema(
            Sender::Callback(Box::new(|col: column::FullMetadata| -> Result<(), Error> {
                // Add column to query
                query.insert_column(schema_to_datasource.get(&col.schema.oid), col.clone())?;

                // Send column metadata over the provided Sender object
                column_sender.send(col)?;
                Ok(())
            })), 
            schema_oid
        )?;

        let conn: Connection = db::open()?;

        // Filter rows in the query based on the METADATA_REPORT.FILTER_FORMULA formula
        println!("Now applying filter formula...");
        if let Some(Some(filter_formula)) = conn.query_one("SELECT FILTER_FORMULA FROM METADATA_REPORT WHERE OID = ?1", params![schema_oid], |row| row.get::<_, Option<String>>("FILTER_FORMULA")).optional()? {
            // Insert WHERE clause
            query.insert_filter(filter_formula)?;
        }

        // Additionally filter rows in the query based on the provided filters
        println!("Now applying row filters {:?}", filters);
        for (filter_datasource_alias, filter_datasource_row_oid) in filters {
            query.insert_row_filter(filter_datasource_alias, filter_datasource_row_oid);
        }

        // Group rows in the query based on the METADATA_REPORT_GROUPBY table
        println!("Now applying GROUP BY...");
        let mut stmt_groupby = conn.prepare(
            "
            SELECT 
                COLUMN_OID 
            FROM METADATA_REPORT_GROUPBY_VIEW
            WHERE REPORT_OID = ?1 
            "
        )?;
        for row_result in stmt_groupby.query_and_then(params![schema_oid], |row| row.get::<_, i64>("COLUMN_OID"))? {
            let column_oid = row_result?;
            // Insert GROUP BY clause
            query.insert_grouping(column_oid)?;
        }

        // Order the query based on the METADATA_SCHEMA_ORDERBY table
        println!("Now applying ORDER BY...");
        let mut stmt_orderby = conn.prepare(
            "
            SELECT 
                COLUMN_OID, 
                SORT_ASCENDING 
            FROM METADATA_SCHEMA_ORDERBY_VIEW
            WHERE SCHEMA_OID = ?1 
            "
        )?;
        for row_result in stmt_orderby.query_and_then(params![schema_oid], |row| { Ok::<(i64, bool), rusqlite::Error>((row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?)) })? {
            let (column_oid, sort_ascending) = row_result?;
            // Insert ORDER BY clause
            query.insert_ordering(column_oid, sort_ascending)?;
        }

        Ok(query)
    }

/// Creates the surrogate view for the given table.
fn create_surrogate(trans: &Transaction, table_oid: i64) -> Result<(), Error> {
    println!("Now constructing TABLE{table_oid}_SURROGATE.");

    // Construct mapping from schema to default datasource
    let mut schema_to_datasource: HashMap<i64, datasource::Datasource> = HashMap::new();
    {
        let root_datasource: Datasource = Datasource::get_default_datasource_transact(trans, table_oid)?;
        schema_to_datasource.insert(root_datasource.get_schema_oid()?, root_datasource.clone());

        // Make sure all master tables of a root table are also included as a datasource
        if let datasource::Datasource::Table { table_oid, .. } = root_datasource {
            let table: table::FullMetadata = table::FullMetadata::get(table_oid.clone())?;
            build_schema_to_datasource_mapping(&trans, &mut schema_to_datasource, table)?;
        }
    }

    let mut query: query::QueryBuilder = query::QueryBuilder::new(schema_to_datasource.iter().map(|(_, schema_datasource)| schema_datasource.clone()).collect());

    // Apply ordering to the query
    let mut stmt_orderby = trans.prepare(
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
                d.DATASOURCE_ALIAS || '_MASTER' || FORMAT('%d', inh.MASTER_SCHEMA_OID) AS DATASOURCE_ALIAS,
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
            c.COLUMN_OID, 
            c.SORT_ASCENDING,
            d.IS_OPTIONAL
        FROM METADATA_SCHEMA_ORDERBY_VIEW c
        INNER JOIN (
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
        ) d ON c.SCHEMA_OID = d.DATASOURCE_TABLE_OID
        "
    )?;
    for row_result in stmt_orderby.query_map(params![table_oid], |row| { Ok::<(String, i64, bool, bool), rusqlite::Error>((row.get("DATASOURCE_ALIAS")?, row.get::<_, i64>("COLUMN_OID")?, row.get::<_, bool>("SORT_ASCENDING")?, row.get::<_, bool>("IS_OPTIONAL")?)) })? {
        let (datasource_alias, column_oid, sort_ascending, is_optional) = row_result?;

        // Construct the datasource
        let datasource_path: Vec<String> = datasource_alias.split('_').map(|s| String::from(s)).collect();
        let datasource: datasource::Datasource = datasource::Datasource::from_path_transact(trans, datasource_path)?;
        
        // Construct the column metadata
        let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

        // Compile and insert column
        query.insert_column(Some(&datasource), column)?;

        // Insert ORDER BY clause
        query.insert_ordering(column_oid, sort_ascending)?;
    }

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
                    d.DATASOURCE_ALIAS || '_MASTER' || FORMAT('%d', inh.MASTER_SCHEMA_OID) AS DATASOURCE_ALIAS,
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
            let datasource: datasource::Datasource = datasource::Datasource::from_path_transact(trans, datasource_path)?;
            
            // Construct the column metadata
            let column: column::FullMetadata = column::FullMetadata::get_transact(trans, column_oid)?;

            // Add the datasource + column to the list
            columns.push((datasource, column, column_is_optional));
        }
        columns
    };

    let sql_surrogate_label: String = if columns.len() == 0 {
        format!(
            "
                '— NO PRIMARY KEY —' AS LABEL,
                'null' AS JSON_LABEL 
            "
        )
    } else if columns.len() == 1 {
        // Table has a single column that serves as its primary key
        let (datasource, column, _) = columns.into_iter().next().unwrap();
        match query.compile_column(Some(&datasource), column)? {
            query::QueryBuilderColumn::Primitive { primitive_type, label_expr, value_expr, .. } => {
                let label_expr: String = format!("COALESCE({label_expr}, '— NO PRIMARY KEY —')");
                format!(
                    "
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
                        {label_expr} AS LABEL,
                        {json_expr} AS JSON_LABEL 
                    "
                )
            }
            query::QueryBuilderColumn::Multiselect { label_expr, .. } => {
                let label_expr: String = format!("COALESCE({label_expr}, '[]')");
                format!(
                    "
                        {label_expr} AS LABEL,
                        {label_expr} AS JSON_LABEL 
                    "
                )
            }
            _ => {
                return Err(Error::AdhocError("Table has primary key with a column type disallowed to be a primary key."));
            }
        }
    } else {
        // Table's primary key is the combination of multiple columns

        // Compile JSON expression for each primary key
        let mut json_exprs: Vec<String> = Vec::new();
        for (idx, (datasource, column, column_is_optional)) in columns.into_iter().enumerate() {
            let column_name_expr: String = format!(
                r#"'"{}": ' || "#, 
                column.name.replace(r#"\"#, r#"\\"#).replace(r#"""#, r#"\""#).replace(r#"'"#, r#"''"#)
            );
            let value_expr: String = match query.compile_column(Some(&datasource), column)? {
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

        // Construct label of table as JSON object comprised of the non-null values
        let compiled_values_expr: String = format!("VALUES ({})", json_exprs.into_iter().reduce(|acc, e| format!("{acc}), ({e}")).unwrap());
        format!(
            "
                '{{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1, ',') OVER () FROM ({compiled_values_expr})), '') || '}}' AS LABEL,
                '{{' || COALESCE((SELECT GROUP_CONCAT(COLUMN1, ',') OVER () FROM ({compiled_values_expr})), '') || '}}' AS JSON_LABEL 
            "
        )
    };

    if let Some((sql_surrogate_from, datasource_aliases)) = query.compile_datasources()? {
        // Construct the expression to retrieve the table's OID
        let root_oid_expr: String = format!("{}.OID", datasource_aliases.into_iter().next().unwrap());
        
        // Construct the surrogate view
        let sql_view: String = format!("CREATE VIEW TABLE{table_oid}_SURROGATE AS SELECT {root_oid_expr} AS OID, {sql_surrogate_label} {sql_surrogate_from}");
        println!("{sql_view}");
        trans.execute(&sql_view, [])?;
    } else {
        // Construct the surrogate view
        let sql_view: String = format!("CREATE VIEW TABLE{table_oid}_SURROGATE AS SELECT OID, '— NO PRIMARY KEY —' AS LABEL, 'null' AS JSON_LABEL FROM TABLE{table_oid}");
        println!("{sql_view}");
        trans.execute(&sql_view, [])?;
    }
    Ok(())
}