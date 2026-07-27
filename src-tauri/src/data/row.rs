use crate::data::cell::DataCellEntry;
use crate::data::column;
use crate::data::column_type;
use crate::util::db;
use crate::util::error::Error;
use crate::util::db::{sql_iter, sql_map_then_iter, sql_one, sql_zero_or_one, sql_execute};
use rusqlite::Connection;
use rusqlite::{params, OptionalExtension, Transaction};
use std::collections::{HashMap, HashSet};

/// Constructs a mapping of all associated rows in master tables.
fn map_all_master_tables(
    conn: &Connection,
    table_oid: i64,
    row_oid: i64,
    mapped_table_oid: &mut HashMap<i64, Option<i64>>,
) -> Result<(), Error> {
    if !mapped_table_oid.contains_key(&table_oid) {
        mapped_table_oid.insert(table_oid, Some(row_oid));

        sql_map_then_iter(
            conn, 
            "
            SELECT 
                inh.MASTER_SCHEMA_OID 
            FROM METADATA_SCHEMA_INHERITANCE_VIEW inh 
            INNER JOIN METADATA_SCHEMA s ON s.OID = inh.MASTER_SCHEMA_OID 
            WHERE inh.INHERITOR_SCHEMA_OID = ?1
            ", 
            params![table_oid], 
            |row| row.get::<_, i64>(0), 
            |master_table_oid| {
                let master_row_oid: i64 = sql_one(
                    conn,
                    format!(
                        "
                        SELECT 
                            MASTER{master_table_oid}_OID 
                        FROM 
                        TABLE{table_oid} 
                        WHERE OID = ?1
                        "
                    ), 
                    params![row_oid], 
                    |row| row.get(0)
                )?;

                // Map all master tables of the master table
                map_all_master_tables(conn, master_table_oid, master_row_oid, mapped_table_oid)?;
                Ok(None::<()>)
            }
        )?;
    }
    Ok(())
}

/// Constructs a mapping of all associated rows in inheritor tables.
fn map_all_inheritor_tables(
    conn: &Connection,
    table_oid: i64,
    row_oid: Option<i64>,
    mapped_table_oid: &mut HashMap<i64, Option<i64>>,
) -> Result<(usize, Option<i64>), Error> {
    if !mapped_table_oid.contains_key(&table_oid) {
        mapped_table_oid.insert(table_oid, row_oid);

        let mut deepest_level: usize = 0;
        let mut deepest_table_oid: Option<i64> = None;

        sql_map_then_iter(
            conn,
            "
            SELECT 
                inh.INHERITOR_SCHEMA_OID 
            FROM METADATA_SCHEMA_INHERITANCE_VIEW inh 
            INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID 
            WHERE inh.MASTER_SCHEMA_OID = ?1
            ",
            params![table_oid],
            |row| row.get::<_, i64>(0),
            |inheritor_table_oid| {
                if let Some(row_oid) = row_oid {
                    match sql_zero_or_one(
                        conn,
                        format!(
                            "
                            SELECT 
                                OID, 
                                TRASH 
                            FROM TABLE{inheritor_table_oid} 
                            WHERE MASTER{table_oid}_OID = ?1
                            "
                        ), 
                        params![row_oid], 
                        |row| Ok((row.get::<_, i64>("OID")?, row.get::<_, bool>("TRASH")?))
                    )? {
                        Some((inheritor_row_oid, inheritor_row_is_trashed)) => {
                            // Map all inheritor tables of the inheritor table
                            let (deepest_mapped_level, deepest_mapped_table_oid) = map_all_inheritor_tables(conn, inheritor_table_oid, Some(inheritor_row_oid), mapped_table_oid)?;
                            if !inheritor_row_is_trashed && deepest_mapped_level > deepest_level {
                                deepest_level = deepest_mapped_level;
                                deepest_table_oid = deepest_mapped_table_oid;
                            }
                        }
                        None => {
                            map_all_inheritor_tables(conn, inheritor_table_oid, None, mapped_table_oid)?;        
                        }
                    }
                } else {
                    map_all_inheritor_tables(conn, inheritor_table_oid, None, mapped_table_oid)?;
                }
                Ok(None::<()>)
            }
        )?;
        return Ok((deepest_level + 1, deepest_table_oid));
    }
    Ok((0, None))
}

/// Inserts a row into the table.
/// Optionally, a specific OID for the row can be provided.
pub fn insert_transact(
    trans: &Transaction,
    table_oid: i64,
    row_oid: Option<i64>,
    master_rows: &mut HashMap<i64, i64>,
) -> Result<i64, Error> {
    if let Some(row_oid) = master_rows.get(&table_oid) {
        return Ok(row_oid.clone());
    }

    // Add a related row to every master table
    let mut cols: Vec<(String, String)> = Vec::new();
    sql_iter(
        trans,
        "
        SELECT 
            MASTER_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE_VIEW 
        WHERE INHERITOR_SCHEMA_OID = ?1
        ",
        params![table_oid],
        |row| row.get(0),
        |master_schema_oid| {
            let master_table_name: String = format!("TABLE{master_schema_oid}");
            if trans.table_exists(Some("main"), &master_table_name)? {
                let master_schema_row_oid: i64 = insert_transact(
                    trans, 
                    master_schema_oid, 
                    None, 
                    master_rows
                )?;

                cols.push((
                    format!("MASTER{master_schema_oid}_OID"),
                    format!("{}", master_schema_row_oid),
                ));
            }
            Ok(None::<()>)
        }
    )?;

    // Add a related row for every non-nullable Object column
    sql_map_then_iter(
        trans,
        "
        SELECT c.OID, typ.TABLE_OID 
        FROM METADATA_COLUMN c
        INNER JOIN METADATA_COLUMN_TYPE__OBJECT typ ON typ.OID = c.TYPE_OID
        WHERE c.SCHEMA_OID = ?1 
            AND NOT c.IS_NULLABLE
        ",
        params![table_oid],
        |row| {
            let column_oid: i64 = row.get("OID")?;
            let object_schema_oid: i64 = row.get("TABLE_OID")?;
            Ok::<(String, i64), rusqlite::Error>((format!("COLUMN{column_oid}"), object_schema_oid))
        },
        |(column_name, object_schema_oid)| {
            let mut object_master_rows: HashMap<i64, i64> = HashMap::new();
            let object_row_oid: i64 = insert_transact(trans, object_schema_oid, None, &mut object_master_rows)?;

            cols.push((column_name, format!("{object_row_oid}")));
            Ok(None::<()>)
        }
    )?;

    // Query for any default values that need to be populated
    sql_iter(
        trans,
        "
        SELECT c.OID, c.DEFAULT_VALUE 
        FROM METADATA_COLUMN c
        INNER JOIN METADATA_COLUMN_TYPE__PRIMITIVE typ ON typ.OID = c.TYPE_OID
        WHERE c.SCHEMA_OID = ?1 
            AND c.DEFAULT_VALUE IS NOT NULL 
            AND typ.MODE NOT IN ('file', 'image')
        ",
        params![table_oid],
        |row| Ok((
            row.get::<_, i64>("OID")?,
            row.get::<_, String>("DEFAULT_VALUE")?
        )),
        |(column_oid, default_value)| {
            cols.push((format!("COLUMN{column_oid}"), default_value));
            Ok(None::<()>)
        }
    )?;

    // Handle insertion at a specific location in the table
    if let Some(o) = row_oid {
        // Make space for the new row at the designated OID
        sql_execute(
            trans, 
            format!(
                "
                UPDATE TABLE{table_oid} SET 
                    OID = -OID 
                WHERE OID >= ?1
                "
            ), 
            params![o]
        )?;
        sql_execute(
            trans, 
            format!(
                "
                UPDATE TABLE{table_oid} SET 
                    OID = 1 - OID 
                WHERE OID < 0
                "
            ), 
            []
        )?;

        // Add initial value for the OID
        cols.push((String::from("OID"), format!("{o}")));
    }

    // Compile the INSERT statement and execute
    let sql_insert_row_params: Vec<String> = cols
        .iter()
        .map(|(_, column_value)| column_value.clone())
        .collect();
    sql_execute(
        trans,
        format!(
            "INSERT INTO TABLE{} {}",
            table_oid,
            if cols.len() == 0 {
                String::from("DEFAULT VALUES")
            } else {
                let (column_names, column_params) = cols.into_iter().enumerate().fold(
                    (String::from(""), String::from("")),
                    |(acc_column_names, acc_column_params), (e_idx, (e_column_name, _))| {
                        (
                            if acc_column_names == "" {
                                e_column_name
                            } else {
                                format!("{acc_column_names}, {e_column_name}")
                            },
                            if acc_column_params == "" {
                                format!("?{}", e_idx + 1)
                            } else {
                                format!("{acc_column_params}, ?{}", e_idx + 1)
                            },
                        )
                    },
                );
                format!("({column_names}) VALUES ({column_params})")
            }
        ),
        rusqlite::params_from_iter(sql_insert_row_params.into_iter()),
    )?;

    // Get the OID and add to the HashMap of master tables
    let row_oid: i64 = trans.last_insert_rowid();
    master_rows.insert(table_oid, row_oid);
    Ok(row_oid)
}

/// Inserts a row into the table.
/// Optionally, a specific OID for the new row can be provided.
/// Returns the OID of the new row.
pub fn insert(
    table_oid: i64,
    row_oid: Option<i64>,
    fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>,
) -> Result<i64, Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    // Insert the row into the table, + related rows for each master table
    let mut master_rows: HashMap<i64, i64> = HashMap::new();
    let row_oid: i64 = insert_transact(&trans, table_oid, row_oid, &mut master_rows)?;

    // Ensure that rows fixed by filters remain fixed
    // e.g. A row connected to a parent table via a Multiselect column on a parent row will be auto-selected by the parent row.
    if let Some((
        fixed_parent_datasource_table_oid,
        fixed_parent_datasource_row_oid,
        fixed_parent_datasource_relationship_column,
    )) = fixed_parent_datasource
    {
        match &fixed_parent_datasource_relationship_column.column_type {
            column_type::ColumnType::Select { .. } => {
                if fixed_parent_datasource_relationship_column.schema.oid
                    == fixed_parent_datasource_table_oid
                {
                    // Select columns on the parent datasource's schema have a *-to-1 relationship with their child datasource, so throw an error
                    return Err(Error::adhoc("The new row has a fixed parent datasource joined to it by a Select column on the parent datasource, so creating a new row is not allowed."));
                } else {
                    // Automatically set the Select column of the created row to match the fixed parent datasource row
                    sql_execute(
                        &trans,
                        format!(
                            "
                            UPDATE TABLE{} SET 
                                COLUMN{} = ?1 
                            WHERE OID = ?2
                            ",
                            table_oid, fixed_parent_datasource_relationship_column.oid
                        ),
                        params![fixed_parent_datasource_row_oid, row_oid],
                    )?;
                }
            }
            column_type::ColumnType::Multiselect { .. } => {
                // Automatically add a Multiselect choice to link the parent datasource row with the newly-created row
                sql_execute(
                    &trans,
                    format!(
                        "
                        INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) 
                        VALUES (?1, ?2)
                        ",
                        fixed_parent_datasource_relationship_column.oid,
                        fixed_parent_datasource_table_oid,
                        table_oid
                    ),
                    params![fixed_parent_datasource_row_oid, row_oid],
                )?;
            }
            column_type::ColumnType::Object { .. } => {
                // Object columns have a 1-to-1 relationship between the parent and child datasources, so throw an error
                return Err(Error::adhoc("The new row has a fixed parent datasource joined to it by an Object column, so creating a new row is not allowed."));
            }
            _ => {
                // No other case should ever occur, so throw an error
                return Err(Error::adhoc("The new row has a fixed parent datasource supposedly joined to it by a column without a relationship to that parent datasource."));
            }
        }
    }

    // Commit the transaction
    trans.commit()?;
    Ok(row_oid)
}

/// Inserts a new row and copies data.
pub fn copy_transact(
    trans: &Transaction,
    table_oid: i64,
    cells: Vec<DataCellEntry>,
    master_rows: &mut HashMap<i64, i64>,
) -> Result<i64, Error> {
    // Insert a new row
    insert_transact(trans, table_oid, None, master_rows)?;

    // For each cell with data, set the content of that cell
    for mut cell_entry in cells {
        cell_entry.row_oid = master_rows[&cell_entry.table_oid].clone();
        cell_entry.set_transact(trans)?;
    }

    // Return the row OID of the new row
    Ok(master_rows[&table_oid])
}

/// Sets the TRASH flag for the row + all master rows + all inheritor rows.
/// Returns the table OID and row OID of the deepest schema level where a row was trashed.
pub fn trash_transact(
    trans: &Transaction,
    table_oid: i64,
    row_oid: i64,
    completed_table_oid: &mut HashSet<i64>,
) -> Result<Option<(i64, i64)>, Error> {
    // Check if the row is already trashed
    if sql_one(
        trans,
        format!(
            "
            SELECT 
                TRASH 
            FROM TABLE{table_oid} 
            WHERE OID = ?1
            "
        ), 
        params![row_oid], 
        |row| row.get::<_, bool>("TRASH")
    )? {
        return Ok(None); // If it is already trashed, then all of its children should be trash, and its master rows can be handled elsewhere in the recursion tree
    }
    // Trash the row
    sql_execute(
        trans, 
        format!(
            "
            UPDATE TABLE{table_oid} SET 
                TRASH = TRUE 
            WHERE OID = ?1
            "
        ), 
        params![row_oid]
    )?;

    // Trash upwards in the inheritance tree
    sql_map_then_iter(
        trans,
        "
        SELECT 
            MASTER_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE_VIEW 
        WHERE INHERITOR_SCHEMA_OID = ?1
        ",
        params![table_oid],
        |row| row.get(0),
        |master_schema_oid| {
            let master_table_name: String = format!("TABLE{master_schema_oid}");
            if !completed_table_oid.contains(&master_schema_oid) && trans.table_exists(Some("main"), &master_table_name)?
            {
                completed_table_oid.insert(master_schema_oid);
                let master_schema_row_oid: i64 = sql_one(
                    trans,
                    format!(
                        "
                        SELECT 
                            MASTER{master_schema_oid}_OID 
                        FROM TABLE{table_oid} 
                        WHERE OID = ?1
                        "
                    ), 
                    params![row_oid], 
                    |row| row.get(0)
                )?;
                trash_transact(
                    trans,
                    master_schema_oid,
                    master_schema_row_oid,
                    completed_table_oid,
                )?;
            }
            Ok(None::<()>)
        }
    )?;

    // Trash deeper in the inheritance tree
    if let Some(deepest_trashed_inheritor) = sql_map_then_iter(
        trans,
        "
        SELECT 
            INHERITOR_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE_VIEW 
        WHERE MASTER_SCHEMA_OID = ?1
        ",
        params![table_oid],
        |row| row.get(0),
        |inheritor_schema_oid| {
            let inheritor_table_name: String = format!("TABLE{inheritor_schema_oid}");
            if !completed_table_oid.contains(&inheritor_schema_oid)
                && trans.table_exists(Some("main"), &inheritor_table_name)?
            {
                completed_table_oid.insert(inheritor_schema_oid);
                if let Some(inheritor_schema_row_oid) = sql_zero_or_one(
                    trans,
                    format!(
                        "
                        SELECT 
                            OID 
                        FROM TABLE{inheritor_schema_oid} 
                        WHERE MASTER{table_oid}_OID = ?1
                        "
                    ), 
                    params![row_oid], 
                    |row| row.get(0)
                )? {
                    // Stop iteration at the first inheritor schema found to have been previously untrashed
                    if let Some(deepest_level_trashed_table_and_row) = trash_transact(
                        trans,
                        inheritor_schema_oid,
                        inheritor_schema_row_oid,
                        completed_table_oid,
                    )? {
                        return Ok(Some(deepest_level_trashed_table_and_row));
                    }
                }
            }
            Ok(None)
        }
    )? {
        Ok(Some(deepest_trashed_inheritor))
    } else {
        // If no inheritor schema was trashed, this is the deepest level that was trashed, so return (table_oid, row_oid)
        Ok(Some((table_oid, row_oid)))
    }
}

/// Sets the flag labelling a row for garbage collection.
pub fn trash(table_oid: i64, row_oid: i64) -> Result<Option<(i64, i64)>, Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    // Trash the row + all related rows up and down the inheritance tree
    let mut completed_table_oid: HashSet<i64> = HashSet::new();
    let deepest_level_trashed_table_and_row: Option<(i64, i64)> =
        trash_transact(&trans, table_oid, row_oid, &mut completed_table_oid)?;

    // Commit the transaction
    trans.commit()?;
    Ok(deepest_level_trashed_table_and_row)
}

/// Unsets the TRASH flag for a row + all master rows.
pub fn untrash_transact(
    trans: &Transaction,
    table_oid: i64,
    row_oid: i64,
    completed_table_oid: &mut HashSet<i64>,
) -> Result<(), Error> {
    // Untrash the row
    sql_execute(
        trans, 
        format!(
            "
            UPDATE TABLE{table_oid} SET 
                TRASH = FALSE 
            WHERE OID = ?1
            "
        ), 
        params![row_oid]
    )?;

    // Untrash upwards in the inheritance tree
    sql_iter(
        trans,
        "
        SELECT 
            MASTER_SCHEMA_OID 
        FROM METADATA_SCHEMA_INHERITANCE_VIEW 
        WHERE INHERITOR_SCHEMA_OID = ?1
        ",
        params![table_oid],
        |row| row.get(0),
        |master_schema_oid| {
            let master_table_name: String = format!("TABLE{master_schema_oid}");
            if !completed_table_oid.contains(&master_schema_oid)
                && trans.table_exists(Some("main"), &master_table_name)?
            {
                completed_table_oid.insert(master_schema_oid);
                let master_schema_row_oid: i64 = sql_one(
                    trans,
                    format!(
                        "
                        SELECT 
                            MASTER{master_schema_oid}_OID 
                        FROM TABLE{table_oid} 
                        WHERE OID = ?1
                        "
                    ), 
                    params![row_oid], 
                    |row| row.get(0)
                )?;
                untrash_transact(
                    trans,
                    master_schema_oid,
                    master_schema_row_oid,
                    completed_table_oid,
                )?;
            }
            Ok(None::<()>)
        }
    )?;
    Ok(())
}

/// Unsets the flag labelling a row for garbage collection.
pub fn untrash(table_oid: i64, row_oid: i64) -> Result<(), Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    // Unset the TRASH flag for the row + every master row
    let mut completed_table_oid: HashSet<i64> = HashSet::new();
    untrash_transact(&trans, table_oid, row_oid, &mut completed_table_oid)?;

    // Commit the transaction
    trans.commit()?;
    Ok(())
}

/// Change the object type of a row in a table.
pub fn change_object_type(
    table_oid: i64,
    row_oid: i64,
    inheritor_table_oid: i64,
) -> Result<i64, Error> {
    // Start a transaction
    let mut conn = db::open()?;

    // Map all existing related rows, up and down the inheritance tree
    let mut mapped_table_oid: HashMap<i64, Option<i64>> = HashMap::new();
    map_all_master_tables(&conn, table_oid, row_oid, &mut mapped_table_oid)?;
    mapped_table_oid.remove(&table_oid);
    let (_, deepest_untrashed_table_oid) =
        map_all_inheritor_tables(&conn, table_oid, Some(row_oid), &mut mapped_table_oid)?;
    println!("  Changing object type. {:?}", mapped_table_oid);

    // Trash the row + all related rows up and down the inheritance tree
    let trans: Transaction = conn.transaction()?;
    for (related_table_oid, related_row_oid) in mapped_table_oid.iter() {
        if let Some(related_row_oid) = related_row_oid {
            sql_execute(
                &trans, 
                format!(
                    "
                    UPDATE TABLE{related_table_oid} SET 
                        TRASH = TRUE 
                    WHERE OID = ?1
                    "
                ), 
                params![related_row_oid]
            )?;
        }
    }

    // Check whether a row already exists in the table for the new type
    if let Some(Some(inheritor_row_oid)) = mapped_table_oid.get(&inheritor_table_oid) {
        // If a row does already exist, untrash it
        let mut completed_untrash_table_oid: HashSet<i64> = HashSet::new();
        untrash_transact(
            &trans,
            inheritor_table_oid,
            inheritor_row_oid.clone(),
            &mut completed_untrash_table_oid,
        )?;
    } else {
        // If a row does not already exist, create a new row associated with the known rows
        let mut master_rows: HashMap<i64, i64> = mapped_table_oid
            .into_iter()
            .filter_map(|(table_oid, row_oid)| {
                if let Some(row_oid) = row_oid {
                    Some((table_oid, row_oid))
                } else {
                    None
                }
            })
            .collect();
        insert_transact(&trans, inheritor_table_oid, None, &mut master_rows)?;
    }

    // Commit the transaction
    trans.commit()?;
    Ok(deepest_untrashed_table_oid.unwrap_or(table_oid))
}

/// Reorders a row in a table.
pub fn reorder(table_oid: i64, row_oid: i64, new_row_oid: Option<i64>) -> Result<i64, Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    let new_row_oid: i64 = match new_row_oid {
        Some(new_row_oid) => {
            // Make room for the row OID
            sql_execute(
                &trans, 
                format!(
                    "
                    UPDATE TABLE{table_oid} SET 
                        OID = -OID 
                    WHERE OID >= ?1 
                        AND OID != ?2
                    "
                ), 
                params![new_row_oid, row_oid]
            )?;

            // Change the row OID
            sql_execute(
                &trans, 
                format!(
                    "
                    UPDATE TABLE{table_oid} SET 
                        OID = ?1 
                    WHERE OID = ?2
                    "
                ), 
                params![new_row_oid, row_oid]
            )?;

            // Move back the other row OIDs
            sql_execute(
                &trans, 
                format!(
                    "
                    UPDATE TABLE{table_oid} SET 
                        OID = 1 - OID 
                    WHERE OID < 0
                    "), 
                    []
            )?;

            new_row_oid
        }
        None => {
            // Query for the next OID
            let new_row_oid: i64 = sql_zero_or_one(
                &trans,
                format!(
                    "
                    SELECT 
                        MAX(OID) + 1 
                    FROM TABLE{table_oid}
                    "
                ), 
                [], 
                |row| row.get::<_, Option<i64>>(0)
            )?.unwrap_or(Some(1)).unwrap_or(1);

            // Change the row OID
            sql_execute(
                &trans, 
                format!(
                    "
                    UPDATE TABLE{table_oid} SET 
                        OID = ?1 
                    WHERE OID = ?2
                    "
                ), 
                params![new_row_oid, row_oid]
            )?;

            new_row_oid
        }
    };

    // Commit the transaction
    trans.commit()?;
    Ok(new_row_oid)
}
