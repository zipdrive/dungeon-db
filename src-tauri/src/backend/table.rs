use crate::backend::{data_type, db, table};
use crate::util::error;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Error as RusqliteError, Row, Transaction};
use serde::Serialize;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::i32::MAX;
use std::ops::Index;
use tauri::ipc::Channel;

/// Creates a new table.
pub fn create(name: String, master_table_oid_list: &Vec<i64>) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Add metadata for the table
    trans.execute("INSERT INTO METADATA_TYPE (MODE) VALUES (3);", [])?;
    let table_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_TABLE (TYPE_OID, NAME) VALUES (?1, ?2);",
        params![table_oid, &name],
    )?;

    // Create the table
    let create_table_cmd: String = format!(
        "
    CREATE TABLE TABLE{table_oid} (
        OID INTEGER PRIMARY KEY, 
        TRASH INTEGER NOT NULL DEFAULT 0
    ) STRICT;"
    );
    trans.execute(&create_table_cmd, [])?;

    // Add inheritance from each master table
    for master_table_oid in master_table_oid_list.iter() {
        // Insert metadata indicating that this table inherits from the master table
        trans.execute(
            "INSERT INTO METADATA_TABLE_INHERITANCE (INHERITOR_TABLE_OID, MASTER_TABLE_OID) VALUES (?1, ?2);",
            params![table_oid, master_table_oid]
        )?;

        // Add a column to the table that references a row in the master list
        let alter_table_cmd: String = format!("ALTER TABLE TABLE{table_oid} ADD COLUMN MASTER{master_table_oid}_OID INTEGER NOT NULL REFERENCES TABLE{master_table_oid} (OID) ON UPDATE CASCADE ON DELETE CASCADE;");
        trans.execute(&alter_table_cmd, [])?;
    }

    // Update the surrogate view
    update_surrogate_view(&trans, table_oid.clone())?;

    // Commit the transaction
    trans.commit()?;
    return Ok(table_oid);
}

#[derive(PartialEq, Eq)]
struct TableDependency {
    dependency_depth: i32,
    table_oid: i64,
}

impl PartialOrd for TableDependency {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dependency_depth.partial_cmp(&other.dependency_depth)
    }
}

impl Ord for TableDependency {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.dependency_depth.cmp(&other.dependency_depth)
    }
}

/// Update the surrogate view for the table.
pub fn update_surrogate_view(trans: &Transaction, table_oid: i64) -> Result<(), error::Error> {
    // Drop the surrogate view and build up a directed graph of dependencies between the primary keys
    let empty_chain: Vec<i64> = Vec::new();
    let dependencies = drop_surrogate_view(trans, table_oid, &empty_chain)?;

    // Create a priority queue of dependencies
    let mut heap: BinaryHeap<TableDependency> = BinaryHeap::new();
    for (dependent_table_oid, dependent_table_depth) in dependencies {
        heap.push(TableDependency {
            dependency_depth: dependent_table_depth,
            table_oid: dependent_table_oid,
        });
    }

    // Recreate the surrogate view, then recreate the surrogate views for each dependent table
    loop {
        match heap.pop() {
            Some(dep) => {
                create_surrogate_view(trans, dep.table_oid)?;
            }
            None => {
                break;
            }
        }
    }
    return Ok(());
}

/// Drops the surrogate view for the specified table, as well as the surrogate views for any table referencing it in its primary key.
fn drop_surrogate_view(
    trans: &Transaction,
    table_oid: i64,
    above_table_oid: &Vec<i64>,
) -> Result<HashMap<i64, i32>, error::Error> {
    println!("Dropping surrogate view for table TABLE{table_oid}");

    let mut found_dependencies: HashMap<i64, i32> = HashMap::new();
    found_dependencies.insert(table_oid, 0);
    let mut above_table_oid = above_table_oid.clone();
    above_table_oid.push(table_oid);

    // Query to find all tables dependent on the one being dropped
    for dependent_table_oid_result in trans.prepare(
        "SELECT TABLE_OID FROM METADATA_TABLE_COLUMN WHERE TYPE_OID = ?1 AND IS_PRIMARY_KEY = 1"
        )?.query_and_then(
            params![table_oid], 
            |row| {
                row.get::<_, i64>("TABLE_OID")
            }
        )? {

        // Drop all the dependent surrogate views
        let dependent_table_oid: i64 = dependent_table_oid_result?;
        if dependent_table_oid != table_oid { // Prevent infinite recursion in case of self-referencing tables
            // Check to make sure no infinite loop of primary keys referencing each other
            match above_table_oid.iter().position(|elem| *elem == dependent_table_oid) {
                Some(_) => {
                    // Terminate recursion, notate that there is a loop
                    return Err(error::Error::AdhocError("There is an infinite loop of primary keys that reference each other!"));
                },
                None => {
                    // Recurse deeper
                    for (found_dependent_table_oid, found_dependent_table_depth) in drop_surrogate_view(&trans, dependent_table_oid, &above_table_oid)? {
                        match found_dependencies.get_mut(&found_dependent_table_oid) {
                            Some(previously_found_dependent_table_maxdepth) => {
                                *previously_found_dependent_table_maxdepth = std::cmp::max(*previously_found_dependent_table_maxdepth, found_dependent_table_depth + 1);
                            },
                            None => {
                                found_dependencies.insert(found_dependent_table_oid, found_dependent_table_depth + 1);
                            }
                        }
                    }
                }
            }
        }
    }

    // Drop the requested surrogate view
    let drop_view_cmd: String = format!("DROP VIEW IF EXISTS TABLE{table_oid}_SURROGATE");
    trans.execute(&drop_view_cmd, [])?;

    // Return an ordered
    return Ok(found_dependencies);
}

fn create_surrogate_view(trans: &Transaction, table_oid: i64) -> Result<(), error::Error> {
    println!("Creating surrogate view for table TABLE{table_oid}");

    let mut select_tbls_cmd: String = format!("FROM TABLE{table_oid} t");
    struct PrimaryKey {
        single_expr: String,
        json_expr: String,
    }
    let mut select_display_value: Vec<PrimaryKey> = Vec::new(); // The primary key (column name, value, needs to be enclosed in quotes?) tuple
    let mut tbl_count: i64 = 1;

    // Iterate over all columns of the table, building up the table's view
    db::query_iterate(
        trans,
        "SELECT
            c.OID,
            c.NAME,
            c.TYPE_OID,
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID = ?1 AND c.TRASH = 0 AND c.IS_PRIMARY_KEY = 1
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid],
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let column_name: String = row.get("NAME")?;
            let json_column_name: String = match serde_json::to_string(&column_name) {
                Ok(s) => s,
                Err(_) => {
                    return Err(error::Error::AdhocError(
                        "Couldn't serialize a String, for some reason.",
                    ));
                }
            };
            let column_type: data_type::MetadataColumnType =
                data_type::MetadataColumnType::from_database(
                    row.get("TYPE_OID")?,
                    row.get("MODE")?,
                );

            match column_type {
                data_type::MetadataColumnType::Primitive(prim) => match prim {
                    data_type::Primitive::Boolean => {
                        select_display_value.push(PrimaryKey {
                                single_expr: format!("CASE WHEN t.COLUMN{column_oid} = 1 THEN 'True' ELSE 'False' END"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} = 1 THEN 'true' ELSE 'false' END")
                            });
                    }
                    data_type::Primitive::Text => {
                        select_display_value.push(PrimaryKey {
                                single_expr: format!("t.COLUMN{column_oid}"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || t.COLUMN{column_oid} || '\"' ELSE 'null' END")
                            });
                    }
                    data_type::Primitive::Any
                    | data_type::Primitive::Integer
                    | data_type::Primitive::Number
                    | data_type::Primitive::JSON => {
                        select_display_value.push(PrimaryKey { 
                                single_expr: format!("CAST(t.COLUMN{column_oid} AS TEXT)"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN CAST(t.COLUMN{column_oid} AS TEXT) ELSE 'null' END")
                            });
                    }
                    data_type::Primitive::Date => {
                        select_display_value.push(PrimaryKey { 
                                single_expr: format!("DATE(t.COLUMN{column_oid}, 'unixepoch')"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || DATE(t.COLUMN{column_oid}, 'unixepoch') || '\"' ELSE 'null' END") 
                            });
                    }
                    data_type::Primitive::Timestamp => {
                        select_display_value.push(PrimaryKey { 
                                single_expr: format!("STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'unixepoch')"), 
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || STRFTIME('%FT%TZ', t.COLUMN{column_oid}, 'unixepoch') || '\"' ELSE 'null' END") 
                            });
                    }
                    data_type::Primitive::File | data_type::Primitive::Image => {
                        select_display_value.push(PrimaryKey {
                                single_expr: format!("CASE WHEN t.COLUMN{column_oid} IS NULL THEN NULL ELSE '{{}}' END"),
                                json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '{{}}' ELSE 'null' END")
                            });
                    }
                },
                data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("t{tbl_count}.VALUE"),
                        json_expr: format!("'{json_column_name}: ' || CASE WHEN t.COLUMN{column_oid} IS NOT NULL THEN '\"' || t{tbl_count}.VALUE || '\"' ELSE 'null' END")
                    });
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                }
                data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("(SELECT '[' || GROUP_CONCAT(b.VALUE) || ']' FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID)"),
                        json_expr: format!("'{json_column_name}: ' || COALESCE('[' || (SELECT GROUP_CONCAT(b.VALUE) FROM TABLE{column_type_oid}_MULTISELECT a INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID WHERE a.ROW_OID = t.OID GROUP BY a.ROW_OID) || ']', 'null')")
                    });
                }
                data_type::MetadataColumnType::Reference(referenced_table_oid)
                | data_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("t{tbl_count}.DISPLAY_VALUE"),
                        json_expr: format!(
                            "'{json_column_name}: ' || t{tbl_count}.JSON_DISPLAY_VALUE"
                        ),
                    });
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = t.COLUMN{column_oid}");
                    tbl_count += 1;
                }
                data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_display_value.push(PrimaryKey {
                        single_expr: format!("'[' || (SELECT GROUP_CONCAT(a.DISPLAY_VALUE) FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID) || ']'"),
                        json_expr: format!("'{json_column_name}: [' || (SELECT GROUP_CONCAT(a.JSON_DISPLAY_VALUE) FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = t.OID GROUP BY a.PARENT_OID) || ']'")
                    });
                }
            }
            return Ok(());
        },
    )?;

    let json_display_value: String = if select_display_value.len() > 0 {
        format!(
            "'{{ ' || {} || ' }}'",
            select_display_value
                .iter()
                .map(|primary_key| primary_key.json_expr.clone())
                .collect::<Vec<String>>()
                .join(" || ', ' || ")
        )
    } else {
        String::from("'{}'")
    };
    let standard_display_value: String = if select_display_value.len() > 1 {
        json_display_value.clone()
    } else if select_display_value.len() == 1 {
        select_display_value[0].single_expr.clone()
    } else {
        String::from("'— NO PRIMARY KEY —'")
    };

    // Create the new surrogate view
    let create_view_cmd: String = format!(
        "
        CREATE VIEW TABLE{table_oid}_SURROGATE 
        AS 
        SELECT
            t.OID,
            CASE
                WHEN t.TRASH = 0 THEN {standard_display_value}
                ELSE '— DELETED —'
            END AS DISPLAY_VALUE,
            CASE
                WHEN t.TRASH = 0 THEN {json_display_value}
                ELSE 'null'
            END AS JSON_DISPLAY_VALUE
        {select_tbls_cmd}"
    );
    println!("{}", create_view_cmd);
    trans.execute(&create_view_cmd, params![])?;
    return Ok(());
}

/// Flags a table as trash.
pub fn move_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute(
        "UPDATE METADATA_TABLE SET TRASH = 1 WHERE TYPE_OID = ?1;",
        params![table_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a table as trash.
pub fn unmove_trash(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute(
        "UPDATE METADATA_TABLE SET TRASH = 0 WHERE TYPE_OID = ?1;",
        params![table_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Deletes the table with the given OID and all associated local columns.
/// Generally, this function should only be called after the table has been flagged as trash for reasonably long enough that the user could undo it if they wanted to.
pub fn delete(table_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Drop data from the table
    let drop_cmd: String = format!("DROP TABLE IF EXISTS TABLE{table_oid};");
    trans.execute(&drop_cmd, [])?;

    // Drop any of the table's child tables
    for child_table_oid_result in trans.prepare("SELECT t.OID FROM METADATA_TABLE_COLUMN c INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID WHERE c.TABLE_OID = ?1 AND t.MODE = 5")?
        .query_and_then(
            params![table_oid], |row| row.get::<_, i64>("OID")
        )? {
        
        // Extract the OID of the child table
        let child_table_oid = child_table_oid_result?;

        // Drop the child table's data
        let drop_child_cmd = format!("DROP TABLE IF EXISTS TABLE{child_table_oid};");
        trans.execute(&drop_child_cmd, [])?;

        // Drop the child table from metadata
        trans.execute(
            "DELETE FROM METADATA_TYPE WHERE OID = ?1;",
            params![child_table_oid]
        )?;
    }

    // Drop any of the table's single-select dropdown value tables
    // TODO

    // Drop any of the table's multi-select dropdown value tables
    // TODO

    // Finally, drop the table's metadata
    trans.execute(
        "DELETE FROM METADATA_TYPE WHERE OID = ?1;",
        params![table_oid],
    )?;
    return Ok(());
}

#[derive(Serialize)]
/// The most bare-bones version of table metadata, used solely for populating the list of tables
pub struct BasicMetadata {
    pub oid: i64,
    pub name: String,
}

/// Gets metadata for a specified table.
pub fn get_metadata(table_oid: &i64) -> Result<BasicMetadata, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let table_name: String = trans.query_one(
        "SELECT 
            NAME 
        FROM METADATA_TABLE 
        WHERE TRASH = 0 
        WHERE TYPE_OID = ?1;",
        params![table_oid],
        |row| row.get::<_, String>("NAME"),
    )?;
    return Ok(BasicMetadata {
        oid: table_oid.clone(),
        name: table_name,
    });
}

/// Sends a list of tables through the provided channel.
pub fn send_metadata_list(table_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(
        &trans,
        "SELECT 
            OID, 
            NAME 
        FROM METADATA_TABLE 
        WHERE TRASH = 0 
        ORDER BY NAME ASC;",
        [],
        &mut |row| {
            table_channel.send(BasicMetadata {
                oid: row.get::<_, i64>(0)?,
                name: row.get::<_, String>(1)?,
            })?;
            return Ok(());
        },
    )?;
    return Ok(());
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MasterListOption {
    oid: i64,
    name: String,
    hierarchy_level: i64,
    is_disabled: bool,
}

/// Sends a set of possible options for a master list for a table.
pub fn send_master_list_options(
    table_oid: Option<i64>,
    allow_inheritance_from_tables: bool,
    channel: Channel<MasterListOption>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(
        &trans,
        if allow_inheritance_from_tables {
            "
            WITH RECURSIVE EXCLUDED_TABLES (TYPE_OID) AS (
                SELECT ?1 AS TYPE_OID
                UNION
                SELECT
                    u.INHERITOR_TABLE_OID AS TYPE_OID
                FROM EXCLUDED_TABLES s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                WHERE TRASH = 0
            )
            SELECT
                tbl.TYPE_OID,
                tbl.NAME,
                0 AS HIERARCHY_LEVEL,
                CASE WHEN exc.TYPE_OID IS NOT NULL THEN 1 ELSE 0 END AS IS_DISABLED
            FROM METADATA_TABLE tbl
            INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.TYPE_OID
            LEFT JOIN EXCLUDED_TABLES exc ON exc.TYPE_OID = tbl.TYPE_OID
            WHERE typ.MODE IN (3, 4)
            ORDER BY tbl.NAME
            "
        } else {
            "
            WITH RECURSIVE EXCLUDED_TABLES (TYPE_OID) AS (
                SELECT ?1 AS TYPE_OID
                UNION
                SELECT
                    u.INHERITOR_TABLE_OID AS TYPE_OID
                FROM EXCLUDED_TABLES s
                INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.TYPE_OID
                WHERE TRASH = 0
            )
            SELECT
                tbl.TYPE_OID,
                tbl.NAME,
                0 AS HIERARCHY_LEVEL,
                CASE WHEN exc.TYPE_OID IS NOT NULL THEN 1 ELSE 0 END AS IS_DISABLED
            FROM METADATA_TABLE tbl
            INNER JOIN METADATA_TYPE typ ON typ.OID = tbl.TYPE_OID
            LEFT JOIN EXCLUDED_TABLES exc ON exc.TYPE_OID = tbl.TYPE_OID
            WHERE typ.MODE = 4
            ORDER BY tbl.NAME
            "
        },
        params![table_oid],
        &mut |row| {
            channel.send(MasterListOption {
                oid: row.get("TYPE_OID")?,
                name: row.get("NAME")?,
                hierarchy_level: row.get("HIERARCHY_LEVEL")?,
                is_disabled: row.get("IS_DISABLED")?,
            })?;
            return Ok(());
        },
    )?;

    return Ok(());
}

#[derive(Serialize)]
struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    //data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>,
}
