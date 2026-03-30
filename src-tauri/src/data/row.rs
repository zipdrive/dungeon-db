use std::collections::{HashMap, HashSet};
use rusqlite::Connection;
use rusqlite::{OptionalExtension, Transaction, params};
use crate::data::column;
use crate::data::column_type;
use crate::util::db;
use crate::util::error::Error;



/// Constructs a mapping of all associated rows in master tables.
fn map_all_master_tables(conn: &Connection, table_oid: i64, row_oid: i64, mapped_table_oid: &mut HashMap<i64, Option<i64>>) -> Result<(), Error> {
    if !mapped_table_oid.contains_key(&table_oid) {
        mapped_table_oid.insert(table_oid, Some(row_oid));

        for master_table_oid_result in conn.prepare("SELECT inh.MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE inh INNER JOIN METADATA_SCHEMA s ON s.OID = inh.MASTER_SCHEMA_OID WHERE inh.INHERITOR_SCHEMA_OID = ?1 AND NOT inh.TRASH AND NOT s.TRASH")?.query_map(params![table_oid], |row| row.get::<_, i64>(0))? {
            // Query for the associated row in the master table
            let master_table_oid: i64 = master_table_oid_result?;
            let sql_select: String = format!("SELECT MASTER{master_table_oid}_OID FROM TABLE{table_oid} WHERE OID = ?1");
            let master_row_oid: i64 = conn.query_one(&sql_select, params![row_oid], |row| row.get(0))?;

            // Map all master tables of the master table
            map_all_master_tables(conn, master_table_oid, master_row_oid, mapped_table_oid)?;
        }
    }
    Ok(())
}

/// Constructs a mapping of all associated rows in inheritor tables.
fn map_all_inheritor_tables(conn: &Connection, table_oid: i64, row_oid: Option<i64>, mapped_table_oid: &mut HashMap<i64, Option<i64>>) -> Result<(usize, Option<i64>), Error> {
    if !mapped_table_oid.contains_key(&table_oid) {
        mapped_table_oid.insert(table_oid, row_oid);

        let mut deepest_level: usize = 0;
        let mut deepest_table_oid: Option<i64> = None;

        for inheritor_table_oid_result in conn.prepare("SELECT inh.INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE inh INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID WHERE inh.MASTER_SCHEMA_OID = ?1 AND NOT inh.TRASH AND NOT s.TRASH")?.query_map(params![table_oid], |row| row.get::<_, i64>(0))? {
            // Query for the associated row in the inheritor table
            let inheritor_table_oid: i64 = inheritor_table_oid_result?;
            let sql_select: String = format!("SELECT OID, TRASH FROM TABLE{inheritor_table_oid} WHERE MASTER{table_oid}_OID = ?1");
            if let Some(row_oid) = row_oid {
                match conn.query_one(&sql_select, params![row_oid], |row| Ok((row.get::<_, i64>("OID")?, row.get::<_, bool>("TRASH")?))).optional()? {
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
            };
        }
        return Ok((deepest_level + 1, deepest_table_oid));
    }
    Ok((0, None))
}



/// Inserts a row into the table.
/// Optionally, a specific OID for the row can be provided.
pub fn insert_transact(trans: &Transaction, table_oid: i64, row_oid: Option<i64>, master_rows: &mut HashMap<i64, i64>) -> Result<i64, Error> {
    if let Some(row_oid) = master_rows.get(&table_oid) {
        return Ok(row_oid.clone());
    }

    // Add a related row to every master table
    let mut cols: Vec<(String, String)> = Vec::new();
    let mut query_master_cmd = trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?;
    for master_schema_oid_result in query_master_cmd.query_and_then(params![table_oid], |row| row.get(0))? {
        let master_schema_oid: i64 = master_schema_oid_result?;
        let master_table_name: String = format!("TABLE{master_schema_oid}");
        if trans.table_exists(Some("main"), &master_table_name)? {
            let master_schema_row_oid: i64 = insert_transact(trans, master_schema_oid, None, master_rows)?;
            cols.push((format!("MASTER{master_schema_oid}_OID"), format!("{}", master_schema_row_oid)));
        }
    }

    // Add a related row for every non-nullable Object column
    {
        let mut col_query_stmt = trans.prepare(
            "
            SELECT c.OID, typ.TABLE_OID 
            FROM METADATA_COLUMN c
            INNER JOIN METADATA_COLUMN_TYPE__OBJECT typ ON typ.OID = c.TYPE_OID
            WHERE c.SCHEMA_OID = ?1 
                AND NOT c.IS_NULLABLE
            "
        )?;
        let col_query_rows = col_query_stmt.query_map(params![table_oid], |row| {
            let column_oid: i64 = row.get("OID")?;
            let object_schema_oid: i64 = row.get("TABLE_OID")?;
            Ok::<(String, i64), rusqlite::Error>((format!("COLUMN{column_oid}"), object_schema_oid))
        })?;
        for col_query_row_result in col_query_rows {
            let (column_name, object_schema_oid) = col_query_row_result?;

            let mut object_master_rows: HashMap<i64, i64> = HashMap::new();
            let object_row_oid: i64 = insert_transact(trans, object_schema_oid, None, &mut object_master_rows)?;

            cols.push((column_name, format!("{object_row_oid}")));
        }
    }

    // Query for any default values that need to be populated
    {
        let mut col_query_stmt = trans.prepare(
            "
            SELECT c.OID, c.DEFAULT_VALUE 
            FROM METADATA_COLUMN c
            INNER JOIN METADATA_COLUMN_TYPE__PRIMITIVE typ ON typ.OID = c.TYPE_OID
            WHERE c.SCHEMA_OID = ?1 
                AND c.DEFAULT_VALUE IS NOT NULL 
                AND typ.MODE NOT IN ('file', 'image')
            "
        )?;
        let _ = col_query_stmt.query_and_then(params![table_oid], |row| {
            let column_oid: i64 = row.get("OID")?;
            let default_value: String = row.get("DEFAULT_VALUE")?;
            cols.push((format!("COLUMN{column_oid}"), default_value));
            Ok::<(), rusqlite::Error>(())
        })?;
    }

    // Handle insertion at a specific location in the table
    if let Some(o) = row_oid {
        // Make space for the new row at the designated OID
        let sql_invert_oids: String = format!("UPDATE TABLE{table_oid} SET OID = -OID WHERE OID > ?1");
        trans.execute(&sql_invert_oids, params![o])?;
        let sql_revert_oids: String = format!("UPDATE TABLE{table_oid} SET OID = 1 - OID WHERE OID < 0");
        trans.execute(&sql_revert_oids, [])?;

        // Add initial value for the OID
        cols.push((String::from("OID"), format!("{o}")));
    }

    // Compile the INSERT statement and execute
    let sql_insert_row_params: Vec<String> = cols.iter().map(|(_, column_value)| column_value.clone()).collect();
    let sql_insert_row: String = format!("INSERT INTO TABLE{} {}",
        table_oid,
        if cols.len() == 0 {
            String::from("DEFAULT VALUES")
        } else {
            let (column_names, column_params) = cols.into_iter().enumerate().fold(
                (String::from(""), String::from("")), 
                |(acc_column_names, acc_column_params), (e_idx, (e_column_name, _))| (
                    if acc_column_names == "" { e_column_name } else { format!("{acc_column_names}, {e_column_name}") }, 
                    if acc_column_params == "" { format!("?{}", e_idx + 1) } else { format!("{acc_column_params}, ?{}", e_idx + 1) }
                )
            );
            format!("({column_names}) VALUES ({column_params})")
        }
    );
    trans.execute(&sql_insert_row, rusqlite::params_from_iter(sql_insert_row_params.into_iter()))?;

    // Get the OID and add to the HashMap of master tables
    let row_oid: i64 = trans.last_insert_rowid();
    master_rows.insert(table_oid, row_oid);
    Ok(row_oid)
}

/// Inserts a row into the table.
/// Optionally, a specific OID for the new row can be provided.
/// Returns the OID of the new row.
pub fn insert(table_oid: i64, row_oid: Option<i64>, fixed_parent_datasource: Option<(i64, i64, column::FullMetadata)>) -> Result<i64, Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    // Insert the row into the table, + related rows for each master table
    let mut master_rows: HashMap<i64, i64> = HashMap::new();
    let row_oid: i64 = insert_transact(&trans, table_oid, row_oid, &mut master_rows)?;
    
    // Ensure that rows fixed by filters remain fixed
    // e.g. A row connected to a parent table via a Multiselect column on a parent row will be auto-selected by the parent row.
    if let Some((fixed_parent_datasource_table_oid, fixed_parent_datasource_row_oid, fixed_parent_datasource_relationship_column)) = fixed_parent_datasource {
        match &fixed_parent_datasource_relationship_column.column_type {
            column_type::ColumnType::Select { .. } => {
                if fixed_parent_datasource_relationship_column.schema.oid == fixed_parent_datasource_table_oid {
                    // Select columns on the parent datasource's schema have a *-to-1 relationship with their child datasource, so throw an error
                    return Err(Error::AdhocError("The new row has a fixed parent datasource joined to it by a Select column on the parent datasource, so creating a new row is not allowed."));
                } else {
                    // Automatically set the Select column of the created row to match the fixed parent datasource row
                    let sql_fix_parent: String = format!(
                        "UPDATE TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2",
                        table_oid,
                        fixed_parent_datasource_relationship_column.oid
                    );
                    trans.execute(&sql_fix_parent, params![fixed_parent_datasource_row_oid, row_oid])?;
                }
            }
            column_type::ColumnType::Multiselect { .. } => {
                // Automatically add a Multiselect choice to link the parent datasource row with the newly-created row
                let sql_fix_parent: String = format!(
                    "INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) VALUES (?1, ?2)",
                    fixed_parent_datasource_relationship_column.oid,
                    fixed_parent_datasource_table_oid,
                    table_oid
                );
                trans.execute(&sql_fix_parent, params![fixed_parent_datasource_row_oid, row_oid])?;
            }
            column_type::ColumnType::Object { .. } => {
                // Object columns have a 1-to-1 relationship between the parent and child datasources, so throw an error
                return Err(Error::AdhocError("The new row has a fixed parent datasource joined to it by an Object column, so creating a new row is not allowed."));
            }
            _ => {
                // No other case should ever occur, so throw an error
                return Err(Error::AdhocError("The new row has a fixed parent datasource supposedly joined to it by a column without a relationship to that parent datasource."));
            }
        }
    }

    // Commit the transaction
    trans.commit()?;
    Ok(row_oid)
}

/// Sets the TRASH flag for the row + all master rows + all inheritor rows.
/// Returns the table OID and row OID of the deepest schema level where a row was trashed.
pub fn trash_transact(trans: &Transaction, table_oid: i64, row_oid: i64, completed_table_oid: &mut HashSet<i64>) -> Result<Option<(i64, i64)>, Error> {
    // Check if the row is already trashed
    let sql_is_trashed: String = format!("SELECT TRASH FROM TABLE{table_oid} WHERE OID = ?1");
    if trans.query_one(&sql_is_trashed, params![row_oid], |row| row.get::<_, bool>("TRASH"))? {
        return Ok(None); // If it is already trashed, then all of its children should be trash, and its master rows can be handled elsewhere in the recursion tree
    }
    // Trash the row
    let sql_trash: String = format!("UPDATE TABLE{table_oid} SET TRASH = TRUE WHERE OID = ?1");
    trans.execute(&sql_trash, params![row_oid])?;

    // Trash upwards in the inheritance tree
    let mut query_master_cmd = trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?;
    for master_schema_oid_result in query_master_cmd.query_map(params![table_oid], |row| row.get(0))? {
        let master_schema_oid: i64 = master_schema_oid_result?;
        let master_table_name: String = format!("TABLE{master_schema_oid}");
        if !completed_table_oid.contains(&master_schema_oid) && trans.table_exists(Some("main"), &master_table_name)? {
            completed_table_oid.insert(master_schema_oid);
            let sql_master_schema_row_oid: String = format!("SELECT MASTER{master_schema_oid}_OID FROM TABLE{table_oid} WHERE OID = ?1");
            let master_schema_row_oid: i64 = trans.query_one(&sql_master_schema_row_oid, params![row_oid], |row| row.get(0))?;
            trash_transact(trans, master_schema_oid, master_schema_row_oid, completed_table_oid)?;
        }
    }

    // Trash deeper in the inheritance tree
    let mut query_inheritor_cmd = trans.prepare("SELECT INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE MASTER_SCHEMA_OID = ?1")?;
    for inheritor_schema_oid_result in query_inheritor_cmd.query_map(params![table_oid], |row| row.get(0))? {
        let inheritor_schema_oid: i64 = inheritor_schema_oid_result?;
        let inheritor_table_name: String = format!("TABLE{inheritor_schema_oid}");
        if !completed_table_oid.contains(&inheritor_schema_oid) && trans.table_exists(Some("main"), &inheritor_table_name)? {
            completed_table_oid.insert(inheritor_schema_oid);
            let sql_inheritor_schema_row_oid: String = format!("SELECT OID FROM TABLE{inheritor_schema_oid} WHERE MASTER{table_oid}_OID = ?1");
            if let Some(inheritor_schema_row_oid) = trans.query_one(&sql_inheritor_schema_row_oid, params![row_oid], |row| row.get(0)).optional()? {
                // Stop iteration at the first inheritor schema found to have been previously untrashed
                if let Some(deepest_level_trashed_table_and_row) = trash_transact(trans, inheritor_schema_oid, inheritor_schema_row_oid, completed_table_oid)? {
                    return Ok(Some(deepest_level_trashed_table_and_row));
                }
            }
        }
    }

    // If no inheritor schema was trashed, this is the deepest level that was trashed, so return (table_oid, row_oid)
    Ok(Some((table_oid, row_oid)))
}

/// Sets the flag labelling a row for garbage collection.
pub fn trash(table_oid: i64, row_oid: i64) -> Result<Option<(i64, i64)>, Error> {
    // Start a transaction
    let mut conn = db::open()?;
    let trans: Transaction = conn.transaction()?;

    // Trash the row + all related rows up and down the inheritance tree
    let mut completed_table_oid: HashSet<i64> = HashSet::new();
    let deepest_level_trashed_table_and_row: Option<(i64, i64)> = trash_transact(&trans, table_oid, row_oid, &mut completed_table_oid)?;

    // Commit the transaction
    trans.commit()?;
    Ok(deepest_level_trashed_table_and_row)
}

/// Unsets the TRASH flag for a row + all master rows.
pub fn untrash_transact(trans: &Transaction, table_oid: i64, row_oid: i64, completed_table_oid: &mut HashSet<i64>) -> Result<(), Error> {
    // Untrash the row
    let sql_trash: String = format!("UPDATE TABLE{table_oid} SET TRASH = FALSE WHERE OID = ?1");
    trans.execute(&sql_trash, params![row_oid])?;

    // Untrash upwards in the inheritance tree
    let mut query_master_cmd = trans.prepare("SELECT MASTER_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE WHERE INHERITOR_SCHEMA_OID = ?1")?;
    for master_schema_oid_result in query_master_cmd.query_map(params![table_oid], |row| row.get(0))? {
        let master_schema_oid: i64 = master_schema_oid_result?;
        let master_table_name: String = format!("TABLE{master_schema_oid}");
        if !completed_table_oid.contains(&master_schema_oid) && trans.table_exists(Some("main"), &master_table_name)? {
            completed_table_oid.insert(master_schema_oid);
            let sql_master_schema_row_oid: String = format!("SELECT MASTER{master_schema_oid}_OID FROM TABLE{table_oid} WHERE OID = ?1");
            let master_schema_row_oid: i64 = trans.query_one(&sql_master_schema_row_oid, params![row_oid], |row| row.get(0))?;
            untrash_transact(trans, master_schema_oid, master_schema_row_oid, completed_table_oid)?;
        }
    }
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
pub fn change_object_type(table_oid: i64, row_oid: i64, inheritor_table_oid: i64) -> Result<i64, Error> {
    // Start a transaction
    let mut conn = db::open()?;

    // Map all existing related rows, up and down the inheritance tree
    let mut mapped_table_oid: HashMap<i64, Option<i64>> = HashMap::new();
    map_all_master_tables(&conn, table_oid, row_oid, &mut mapped_table_oid)?;
    mapped_table_oid.remove(&table_oid);
    let (_, deepest_untrashed_table_oid) = map_all_inheritor_tables(&conn, table_oid, Some(row_oid), &mut mapped_table_oid)?;
    println!("  Changing object type. {:?}", mapped_table_oid);

    // Trash the row + all related rows up and down the inheritance tree
    let trans: Transaction = conn.transaction()?;
    for (related_table_oid, related_row_oid) in mapped_table_oid.iter() {
        if let Some(related_row_oid) = related_row_oid {
            let sql_update: String = format!("UPDATE TABLE{related_table_oid} SET TRASH = TRUE WHERE OID = ?1");
            trans.execute(&sql_update, params![related_row_oid])?;
        }
    }

    // Check whether a row already exists in the table for the new type
    if let Some(Some(inheritor_row_oid)) = mapped_table_oid.get(&inheritor_table_oid) {
        // If a row does already exist, untrash it
        let mut completed_untrash_table_oid: HashSet<i64> = HashSet::new();
        untrash_transact(&trans, inheritor_table_oid, inheritor_row_oid.clone(), &mut completed_untrash_table_oid)?;
    } else {
        // If a row does not already exist, create a new row associated with the known rows
        let mut master_rows: HashMap<i64, i64> = mapped_table_oid.into_iter().filter_map(|(table_oid, row_oid)| if let Some(row_oid) = row_oid { Some((table_oid, row_oid)) } else { None }).collect();
        insert_transact(&trans, inheritor_table_oid, None, &mut master_rows)?;
    }

    // Commit the transaction
    trans.commit()?;
    Ok(deepest_untrashed_table_oid.unwrap_or(table_oid))
}