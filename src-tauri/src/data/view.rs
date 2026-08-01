use std::collections::{HashMap, HashSet};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use crate::util::error::Error;
use crate::util::db::{sql_execute, sql_map_then_iter};
use crate::data::column;
use crate::data::column_type;
use crate::data::datasource::Datasource;


mod formula;
mod datasource_cte;
mod wrapper_cte;

mod label;
mod main;


struct ViewsToCreate {
    /// True if the main view needs to be created. False otherwise.
    create_main_view: bool,

    /// True if the label view needs to be created. False otherwise.
    create_label_view: bool
}

/// Analyze which views are associated with a schema and will need to be recreated.
fn analyze_views(
    trans: &Transaction,
    schema_oid: i64,
    drop_main: bool,
    drop_label: bool,
    views_to_create: &mut HashMap<i64, ViewsToCreate>,
) -> Result<(), Error> {
    if let Some(view_to_create) = views_to_create.get_mut(&schema_oid) {
        println!("  Analyzing views for schema OID: {schema_oid}");
        println!("    Main = {} || {drop_main}", view_to_create.create_main_view);
        println!("    Label = {} || {drop_label}", view_to_create.create_label_view);
        if (!view_to_create.create_main_view && drop_main) || (!view_to_create.create_label_view && drop_label) {
            // Some new information is being added
            view_to_create.create_main_view = drop_main.clone() || view_to_create.create_main_view;
            view_to_create.create_label_view = drop_label.clone() || view_to_create.create_label_view;
        } else {
            // No new information is being added
            println!("    Skipping...");
            return Ok(());
        }
    } else {
        println!("  Analyzing views for schema OID: {schema_oid}");
        println!("    Main = {drop_main}");
        println!("    Label = {drop_label}");
        views_to_create.insert(schema_oid, ViewsToCreate { 
            create_main_view: drop_main.clone(), 
            create_label_view: drop_label.clone() 
        });
    }

    // Drop the views associated with any inheritor schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    sql_map_then_iter(
        trans,
        "
        SELECT 
            INHERITOR_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE 
        WHERE MASTER_SCHEMA_OID = ?1
        ",
        params![schema_oid],
        |row| row.get::<_, i64>("INHERITOR_SCHEMA_OID"),
        |inheritor_schema_oid| {
            analyze_views(trans, inheritor_schema_oid, true, true, views_to_create)?;
            Ok(None::<()>)
        }
    )?;

    // Drop only the label views associated with any master schema
    // It doesn't matter whether the inheritance relationship has been trashed or not, nor whether the schema itself has been trashed or not
    sql_map_then_iter(
        trans,
        "
        SELECT 
            MASTER_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE 
        WHERE INHERITOR_SCHEMA_OID = ?1
        ",
        params![schema_oid],
        |row| row.get::<_, i64>("MASTER_SCHEMA_OID"),
        |master_schema_oid| {
            analyze_views(trans, master_schema_oid, true, true, views_to_create)?;
            Ok(None::<()>)
        }
    )?;

    if drop_label {
        // Drop the main views that use the label view, or label views that are dependent on the schema where the label view is being dropped
        sql_map_then_iter(
            trans,
            "
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
            ",
            params![schema_oid],
            |row| {
                Ok((
                    row.get::<_, i64>("SCHEMA_OID")?,
                    row.get::<_, bool>("IS_PRIMARY_KEY")?,
                ))
            },
            |(referencing_schema_oid, referenced_in_label)| {
                analyze_views(
                    trans,
                    referencing_schema_oid,
                    true,
                    referenced_in_label,
                    views_to_create
                )?;
                Ok(None::<()>)
            }
        )?;
    }
    Ok(())
}

/// Create the views associated with a schema.
pub fn regenerate_schema_views(trans: &Transaction, schema_oid: i64) -> Result<(), Error> {
    // Drop existing views that are dependent on these
    println!("Analyzing which views need to be recreated...");
    let mut views_to_create: HashMap<i64, ViewsToCreate> = HashMap::new();
    analyze_views(trans, schema_oid, true, true, &mut views_to_create)?;

    // Drop all of the main views
    for (view_schema_oid, view_to_drop) in views_to_create.iter() {
        if view_to_drop.create_main_view {
            println!("Now dropping SCHEMA{view_schema_oid}_VIEW...");
            let sql_drop: String = format!("DROP VIEW IF EXISTS SCHEMA{view_schema_oid}_VIEW");
            sql_execute(trans, &sql_drop, [])?;
        }
    }

    // Drop and recreate all of the label views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_label_view {
            println!("Now dropping SCHEMA{view_schema_oid}_LABEL_VIEW...");
            let sql_drop: String = format!("DROP VIEW IF EXISTS SCHEMA{view_schema_oid}_LABEL_VIEW");
            sql_execute(trans, &sql_drop, [])?;

            println!("Now generating SQL for SCHEMA{view_schema_oid}_LABEL_VIEW...");
            label::construct_label_view(trans, *view_schema_oid)?;
        }
    }

    // Create all of the main views
    for (view_schema_oid, view_to_create) in views_to_create.iter() {
        if view_to_create.create_main_view {
            println!("Now generating SQL for SCHEMA{view_schema_oid}_VIEW...");
            main::construct_main_view(trans, *view_schema_oid)?;
        }
    }
    Ok(())
}
