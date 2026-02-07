use std::collections::{HashMap, HashSet, LinkedList};
use serde_json::{Result as SerdeJsonResult, Value};
use rusqlite::{Error as RusqliteError, OptionalExtension, Row, Transaction, params};
use serde::Serialize;
use tauri::ipc::Channel;
use time::format_description::well_known;
use time::macros::{time};
use time::{Date, PrimitiveDateTime, UtcDateTime};
use crate::backend::data_type::Primitive;
use crate::backend::{table_column, data_type, db, table};
use crate::util::error;


#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum Cell {
    RowStart {
        row_oid: i64,
        row_index: i64
    },
    ColumnValue {
        table_oid: i64,
        row_oid: i64,
        column_oid: i64,
        column_type: data_type::MetadataColumnType,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase", untagged)]
pub enum RowCell {
    RowExists {
        row_exists: bool,
        table_oid: i64
    },
    ColumnValue {
        table_oid: i64,
        row_oid: i64,
        column_oid: i64,
        column_name: String,
        column_type: data_type::MetadataColumnType,
        column_ordering: i64,
        true_value: Option<String>,
        display_value: Option<String>,
        failed_validations: Vec<error::FailedValidation>
    }
}

/// Insert a row into the data such that the OID places it before any existing rows with that OID.
pub fn insert(table_oid: i64, row_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // If OID is already in database, shift every row with OID >= row_oid up by 1
    let select_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID = ?1;");
    let existing_row_oid = trans.query_one(&select_cmd, params![row_oid], 
        |row| {
            return Ok(row.get::<_, i64>(0)?);
        }
    ).optional()?;

    match existing_row_oid {
        None => {
            // Insert with OID = row_oid
            let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
            trans.execute(&insert_cmd, params![row_oid])?;
            let row_oid = trans.last_insert_rowid();

            // Return the row_oid
            trans.commit()?;
            return Ok(row_oid);
        },
        Some(_) => {
            let existing_prev_row_oid = trans.query_one(&select_cmd, params![row_oid - 1], 
                |row| {
                    return Ok(row.get::<_, i64>(0)?);
                }
            ).optional()?;
            
            match existing_prev_row_oid {
                None => {
                    // Insert with OID = row_oid - 1
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    trans.execute(&insert_cmd, params![row_oid - 1])?;
                    let row_oid = trans.last_insert_rowid();

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                },
                Some(_) => {
                    // Increment every OID >= row_oid up by 1 to make room for the new row
                    let select_all_cmd = format!("SELECT OID FROM TABLE{table_oid} WHERE OID >= ?1 ORDER BY OID DESC;");
                    db::query_iterate(&trans, &select_all_cmd, params![row_oid], 
                        &mut |row| {
                            let update_cmd = format!("UPDATE TABLE{table_oid} SET OID = OID + 1 WHERE OID = ?1;");
                            trans.execute(&update_cmd, params![row.get::<_, i64>(0)?])?;
                            return Ok(());
                        }
                    )?;

                    // Insert the row
                    let insert_cmd = format!("INSERT INTO TABLE{table_oid} (OID) VALUES (?1);");
                    trans.execute(&insert_cmd, params![row_oid])?;
                    let row_oid = trans.last_insert_rowid();

                    // Return the row_oid
                    trans.commit()?;
                    return Ok(row_oid);
                }
            }
        }
    }
}

/// Push a row into the table with a default OID.
pub fn push(table_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Insert the row
    let insert_cmd = format!("INSERT INTO TABLE{table_oid} DEFAULT VALUES;");
    trans.execute(&insert_cmd, [])?;
    let row_oid = trans.last_insert_rowid();

    // Return the row OID
    trans.commit()?;
    return Ok(row_oid);
}

/// Marks a row as trash.
pub fn move_trash(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move the row to the trash bin
    let update_cmd = format!("UPDATE TABLE{table_oid} SET TRASH = 1 WHERE OID = ?1;");
    trans.execute(&update_cmd, params![row_oid])?;

    // Return the row OID
    trans.commit()?;
    return Ok(());
}

/// Unmarks a row as trash.
pub fn unmove_trash(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Move the row to the trash bin
    let update_cmd = format!("UPDATE TABLE{table_oid} SET TRASH = 0 WHERE OID = ?1;");
    trans.execute(&update_cmd, params![row_oid])?;

    // Return the row OID
    trans.commit()?;
    return Ok(());
}

/// Delete the row with the given OID.
pub fn delete(table_oid: i64, row_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Delete the row
    let delete_cmd = format!("DELETE FROM TABLE{table_oid} WHERE OID = ?1;");
    trans.execute(&delete_cmd, params![row_oid])?;

    // Return the row OID
    trans.commit()?;
    return Ok(());
}

/// Attempts to update a value represented by a primitive in a table.
/// This applies to primitive types, single-select dropdown types, reference types, and object types.
/// Returns the previous value of the cell.
pub fn try_update_primitive_value(table_oid: i64, row_oid: i64, column_oid: i64, mut new_value: Option<String>) -> Result<Option<String>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    
    // Verify that the column has a primitive type
    let column_type = trans.query_one(
        "SELECT
            c.TYPE_OID,
            t.MODE
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.OID = ?1", 
        params![column_oid], 
        |row| {
            Ok(data_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?))
        }
    )?;
    match column_type {
        data_type::MetadataColumnType::Primitive(prim) => {
            match prim {
                data_type::Primitive::JSON => {
                    // If column has JSON type, validate the JSON 
                    match new_value.clone() {
                        Some(json_str) => {
                            match serde_json::from_str::<&'_ str>(&*json_str) {
                                Ok(_) => {},
                                Err(_) => {
                                    return Err(error::Error::AdhocError("The provided value is invalid JSON."));
                                }
                            }
                        },
                        None => {}
                    }
                },
                data_type::Primitive::Integer => {
                    match new_value.clone() {
                        Some(num_str) => {
                            // If the value provided is a floating-point number, truncate it into an integer
                            let num: f64 = match num_str.parse() {
                                Ok(n) => n,
                                Err(_) => {
                                    return Err(error::Error::AdhocError("The provided value cannot be converted into an integer."));
                                }
                            };
                            new_value = Some(format!("{}", num as i64));
                        },
                        None => {}
                    }
                },
                data_type::Primitive::Date => {
                    match new_value.clone() {
                        Some(date_str) => {
                            let date: Date = match Date::parse(&date_str, &well_known::Iso8601::DATE) {
                                Ok(d) => d,
                                Err(_) => {
                                    return Err(error::Error::AdhocError("The provided value cannot be converted into a date."));
                                }
                            };
                            new_value = Some(format!("{}", date.to_julian_day()));
                        },
                        None => {}
                    }
                },
                data_type::Primitive::Timestamp => {
                    match new_value.clone() {
                        Some(timestamp_str) => {
                            let timestamp: UtcDateTime = match UtcDateTime::parse(&timestamp_str, &well_known::Iso8601::DATE_TIME) {
                                Ok(d) => d,
                                Err(_) => {
                                    return Err(error::Error::AdhocError("The provided value cannot be converted into a timestamp."));
                                }
                            };
                            let julian_day: i32 = timestamp.to_julian_day();
                            let dur_numerator = timestamp - UtcDateTime::new(Date::from_julian_day(julian_day).unwrap(), time!(12:00));
                            let dur_denominator = UtcDateTime::new(Date::from_julian_day(julian_day + 1).unwrap(), time!(12:00)) - UtcDateTime::new(Date::from_julian_day(julian_day).unwrap(), time!(12:00));
                            let julian_fraction: f64 = (julian_day as f64) + (dur_numerator.as_seconds_f64() / dur_denominator.as_seconds_f64());
                            new_value = Some(format!("{}", julian_fraction));
                        },
                        None => {}
                    }
                },
                _ => {}
            }
            // Ignore other primitive types
        },
        data_type::MetadataColumnType::MultiSelectDropdown(_)
        | data_type::MetadataColumnType::ChildTable(_) => {
            return Err(error::Error::AdhocError("Value of column cannot be updated like a primitive value."));
        }
        _ => {
            // Ignore the rest
        }
    }

    // Retrieve the previous value
    let select_prev_value_cmd = format!("SELECT CAST(COLUMN{column_oid} AS TEXT) AS PRIOR_VALUE FROM TABLE{table_oid} WHERE OID = ?1;");
    let prev_value: Option<String> = trans.query_one(&select_prev_value_cmd, params![row_oid],
        |row| { return Ok(row.get::<_, Option<String>>(0)?); })?;

    // Update the value
    let update_cmd = format!("UPDATE TABLE{table_oid} SET COLUMN{column_oid} = ?1 WHERE OID = ?2;");
    trans.execute(
        &update_cmd,
        params![new_value, row_oid]
    )?;

    // Return OK
    trans.commit()?;
    return Ok(prev_value);
}


struct Column {
    true_ord: Option<String>,
    display_ord: String,
    table_oid: i64,
    row_ord: String,
    column_oid: i64,
    column_name: String,
    column_type: data_type::MetadataColumnType,
    column_ordering: i64,
    is_nullable: bool,
    is_primary_key: bool,
    invalid_nonunique_oid: HashSet<i64>
}

/// Construct a SELECT query to get data from a table
fn construct_data_query(trans: &Transaction, table_oid: i64, include_row_oid_clause: bool, include_parent_row_oid_clause: bool) -> Result<(String, LinkedList<Column>), error::Error> {
    // Build the SELECT query
    let (mut select_cols_cmd, mut select_tbls_cmd) = trans.query_one(
        "WITH RECURSIVE SUPERTYPE_QUERY (LEVEL, FINAL_TYPE_OID, SUPERTYPE_OID, INHERITOR_TYPE_OID) AS (
            SELECT
                1 AS LEVEL,
                u.INHERITOR_TABLE_OID AS FINAL_TYPE_OID,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID
            FROM METADATA_TABLE_INHERITANCE u ON 
            WHERE u.TRASH = 0 AND u.INHERITOR_TABLE_OID = ?1
            UNION
            SELECT
                s.LEVEL + 1 AS LEVEL,
                s.FINAL_TYPE_OID,
                u.MASTER_TABLE_OID AS SUPERTYPE_OID,
                u.INHERITOR_TABLE_OID AS INHERITOR_TYPE_OID
            FROM SUPERTYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.SUPERTYPE_OID
            WHERE u.TRASH = 0
        ),
        CONDENSED_SUPERTYPE_QUERY (MAX_LEVEL, FINAL_TYPE_OID, SUPERTYPE_OID, JOIN_CLAUSE) AS (
            SELECT
                MAX(LEVEL) AS MAX_LEVEL,
                FINAL_TYPE_OID,
                SUPERTYPE_OID,
                'INNER JOIN TABLE' || FORMAT('%d', SUPERTYPE_OID') || ' m' || FORMAT('%d', SUPERTYPE_OID) || ' ' || GROUP_CONCAT(
                    CASE WHEN INHERITOR_TYPE_OID = FINAL_TYPE_OID THEN 't'
                    ELSE 'm' || FORMAT('%d', INHERITOR_TYPE_OID)
                    END || '.MASTER' || FORMAT('%d', SUPERTYPE_OID) || '_OID = m' || FORMAT('%d', SUPERTYPE_OID) || '.OID',
                    ' AND '
                ) AS JOIN_CLAUSE
            FROM SUPERTYPE_QUERY
            GROUP BY
                FINAL_TYPE_OID,
                SUPERTYPE_OID
        )
        SELECT
            'ROW_NUMBER() OVER (ORDER BY t.OID) AS ROW_INDEX, t.OID AS t_OID' || COALESCE(', ' || GROUP_CONCAT('m' || FORMAT('%d', SUPERTYPE_OID) || '.OID AS m' || FORMAT('%d', SUPERTYPE_OID) || '_OID', ', '), '') AS OID_CLAUSE,
            'FROM TABLE' || FORMAT('%d', FINAL_TYPE_OID) || ' t ' || COALESCE(GROUP_CONCAT(JOIN_CLAUSE, ' ' ORDER BY MAX_LEVEL ASC), '') AS FROM_CLAUSE
        FROM CONDENSED_SUPERTYPE_QUERY
        GROUP BY FINAL_TYPE_OID", 
        params![table_oid], 
        |row| { 
            Ok((row.get("OID_CLAUSE")?, row.get("FROM_CLAUSE")?))
        }
    )?;
    let mut columns = LinkedList::<Column>::new();
    let mut tbl_count: usize = 1;

    db::query_iterate(trans,
        "WITH RECURSIVE SUPERTYPE_QUERY (TYPE_OID) AS (
            SELECT
                ?1
            UNION
            SELECT
                u.MASTER_TABLE_OID AS TYPE_OID
            FROM SUPERTYPE_QUERY s
            INNER JOIN METADATA_TABLE_INHERITANCE u ON u.INHERITOR_TABLE_OID = s.TYPE_OID
            WHERE u.TRASH = 0
        )
        SELECT 
            c.OID,
            c.TABLE_OID,
            c.TYPE_OID,
            t.MODE,
            c.IS_NULLABLE,
            c.IS_UNIQUE,
            c.IS_PRIMARY_KEY,
            c.NAME,
            c.COLUMN_ORDERING
        FROM METADATA_TABLE_COLUMN c
        INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
        WHERE c.TABLE_OID IN (SELECT * FROM SUPERTYPE_QUERY) AND c.TRASH = 0
        ORDER BY c.COLUMN_ORDERING;",
        params![table_oid], 
        &mut |row| {
            let column_oid: i64 = row.get("OID")?;
            let column_type: data_type::MetadataColumnType = data_type::MetadataColumnType::from_database(row.get("TYPE_OID")?, row.get("MODE")?);
            let column_ordering: i64 = row.get("COLUMN_ORDERING")?;

            let column_source_table_oid: i64 = row.get("TABLE_OID")?;
            let source_alias: String = if column_source_table_oid == table_oid { String::from("t") } else { format!("m{column_source_table_oid}") };
            
            let enforce_uniqueness: bool = row.get("IS_UNIQUE")?;
            let mut invalid_nonunique_oid: HashSet<i64> = HashSet::<i64>::new();

            let display_ord: String = format!("COLUMN{column_oid}");
            let true_ord: Option<String>;
            match &column_type {
                data_type::MetadataColumnType::Primitive(prim) => {
                    // Primitive type
                    match prim {
                        data_type::Primitive::Any 
                        | data_type::Primitive::Boolean
                        | data_type::Primitive::Integer
                        | data_type::Primitive::Number
                        | data_type::Primitive::Text
                        | data_type::Primitive::JSON => {
                            select_cols_cmd = format!("{select_cols_cmd}, CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS COLUMN{column_oid}");
                        },
                        data_type::Primitive::Date => {
                            select_cols_cmd = format!("{select_cols_cmd}, DATE({source_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN{column_oid}");
                        },
                        data_type::Primitive::Timestamp => {
                            select_cols_cmd = format!("{select_cols_cmd}, STRFTIME('%FT%TZ', {source_alias}.COLUMN{column_oid}, 'julianday') AS COLUMN{column_oid}");
                        },
                        data_type::Primitive::File => {
                            select_cols_cmd = format!("{select_cols_cmd}, CASE 
                            WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL 
                            ELSE 
                                CASE 
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000000 THEN FORMAT('%.1f GB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000000001)
                                    WHEN LENGTH({source_alias}.COLUMN{column_oid}) > 1000000 THEN FORMAT('%.1f MB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.000001)
                                    ELSE FORMAT('%.1f KB', LENGTH({source_alias}.COLUMN{column_oid}) * 0.001)
                                END
                            END AS COLUMN{column_oid}");
                        },
                        data_type::Primitive::Image => {
                            select_cols_cmd = format!("{select_cols_cmd}, CASE WHEN {source_alias}.COLUMN{column_oid} IS NULL THEN NULL ELSE 'Thumbnail' END AS COLUMN{column_oid}");
                        }
                    }
                    true_ord = Some(display_ord.clone());
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                data_type::MetadataColumnType::SingleSelectDropdown(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, t{tbl_count}.VALUE AS COLUMN{column_oid}, CAST(t{tbl_count}.OID AS TEXT) AS _COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{column_type_oid} t{tbl_count} ON t{tbl_count}.OID = {source_alias}.COLUMN{column_oid}");
                    tbl_count += 1;
                    true_ord = Some(format!("_COLUMN{column_oid}"));
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                data_type::MetadataColumnType::MultiSelectDropdown(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, 
                        (SELECT 
                            '[' || GROUP_CONCAT(b.VALUE) || ']' 
                        FROM TABLE{column_type_oid}_MULTISELECT a 
                        INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                        WHERE a.ROW_OID = {source_alias}.OID GROUP BY a.ROW_OID) AS COLUMN{column_oid},
                        (SELECT 
                            GROUP_CONCAT(CAST(b.OID AS TEXT))
                        FROM TABLE{column_type_oid}_MULTISELECT a 
                        INNER JOIN TABLE{column_type_oid} b ON b.OID = a.VALUE_OID 
                        WHERE a.ROW_OID = {source_alias}.OID GROUP BY a.ROW_OID) AS _COLUMN{column_oid}
                        ");
                    true_ord = Some(format!("_COLUMN{column_oid}"));

                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            WITH TABLE_SURROGATE AS (
                                SELECT 
                                    ROW_OID,
                                    GROUP_CONCAT(CAST(VALUE_OID AS TEXT)) AS COLUMN{column_oid}
                                FROM TABLE{column_type_oid}_MULTISELECT 
                                GROUP BY OID
                            )
                            SELECT t.ROW_OID AS OID FROM TABLE_SURROGATE t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE_SURROGATE
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                data_type::MetadataColumnType::Reference(referenced_table_oid) 
                | data_type::MetadataColumnType::ChildObject(referenced_table_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, COALESCE(t{tbl_count}.DISPLAY_VALUE, CASE WHEN {source_alias}.COLUMN{column_oid} IS NOT NULL THEN '— DELETED —' ELSE NULL END) AS COLUMN{column_oid}, CAST({source_alias}.COLUMN{column_oid} AS TEXT) AS _COLUMN{column_oid}");
                    select_tbls_cmd = format!("{select_tbls_cmd} LEFT JOIN TABLE{referenced_table_oid}_SURROGATE t{tbl_count} ON t{tbl_count}.OID = {source_alias}.COLUMN{column_oid}");
                    tbl_count += 1;
                    true_ord = Some(format!("_COLUMN{column_oid}"));
                    
                    // Check for invalid nonunique rows
                    if enforce_uniqueness {
                        let check_nonunique_cmd = format!("
                            SELECT t.OID FROM TABLE{column_source_table_oid} t
                            INNER JOIN (
                                SELECT COLUMN{column_oid}, COUNT(OID) AS ROW_COUNT
                                FROM TABLE{column_source_table_oid} 
                                GROUP BY COLUMN{column_oid} 
                                HAVING COUNT(OID) > 1
                            ) a ON a.COLUMN{column_oid} = t.COLUMN{column_oid}
                        ");
                        db::query_iterate(trans, &check_nonunique_cmd, [], 
                            &mut |row| {
                                invalid_nonunique_oid.insert(row.get(0)?);
                                return Ok(());
                            }
                        )?;
                    }
                },
                data_type::MetadataColumnType::ChildTable(column_type_oid) => {
                    select_cols_cmd = format!("{select_cols_cmd}, (SELECT '[' || GROUP_CONCAT(a.DISPLAY_VALUE) || ']' FROM TABLE{column_type_oid}_SURROGATE a WHERE a.PARENT_OID = {source_alias}.OID GROUP BY a.PARENT_OID) AS COLUMN{column_oid}");
                    true_ord = None;
                }
            }

            // Push the column information
            columns.push_back(Column {
                true_ord: true_ord, 
                display_ord: display_ord,
                table_oid: column_source_table_oid,
                row_ord: format!("{source_alias}_OID"),
                column_oid: column_oid,
                column_name: row.get("NAME")?,
                column_type: column_type,
                column_ordering,
                is_nullable: row.get("IS_NULLABLE")?,
                invalid_nonunique_oid: invalid_nonunique_oid,
                is_primary_key: row.get("IS_PRIMARY_KEY")?
            });
            return Ok(());
        }
    )?;
    return Ok((
        format!(
            "SELECT {select_cols_cmd} {select_tbls_cmd} WHERE t.TRASH = 0 {}",
            if include_row_oid_clause { 
                "AND t.OID = ?1"
            } else if include_parent_row_oid_clause { 
                "AND t.PARENT_OID = ?1 LIMIT ?1 OFFSET ?2"
            } else {
                "LIMIT ?1 OFFSET ?2"
            }
        ), 
        columns
    ));
}

/// Sends all cells for the table through a channel.
pub fn send_table_data(table_oid: i64, parent_row_oid: Option<i64>, page_num: i64, page_size: i64, cell_channel: Channel<Cell>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, false, match parent_row_oid { Some(_) => true, None => false })?;
    let table_select_cmd_params = match parent_row_oid {
        Some(o) => params![o.clone(), page_size, page_size * (page_num - 1)],
        None => params![page_size, page_size * (page_num - 1)]
    };

    println!("{table_select_cmd}");

    // Iterate over the results, sending each cell to the frontend
    db::query_iterate(&trans, 
        &table_select_cmd, 
        table_select_cmd_params,
        &mut |row| {
            // Start by sending the index and OID, which are the first and second ordinal respectively
            let row_index: i64 = row.get("ROW_INDEX")?;
            cell_channel.send(Cell::RowStart {
                row_oid: row.get("t_OID")?,
                row_index: row_index
            })?;

            let invalid_key: bool = false; // TODO

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let row_oid: i64 = row.get(&*column.row_ord)?;

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(Cell::ColumnValue {
                    table_oid: column.table_oid,
                    row_oid: row_oid,
                    column_oid: column.column_oid, 
                    column_type: column.column_type.clone(), 
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // Conclude the row's iteration
            return Ok(());
        }
    )?;
    return Ok(());
}

/// Sends all cells for a row in the table through a channel.
pub fn send_table_row(table_oid: i64, row_oid: i64, cell_channel: Channel<RowCell>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;
    let (table_select_cmd, columns) = construct_data_query(&trans, table_oid, true, false)?;

    // Query for the specified row
    match trans.query_row_and_then(
        &table_select_cmd, 
        params![row_oid], 
        |row| -> Result<(), error::Error> {
            // Start by sending message that confirms the row exists
            cell_channel.send(RowCell::RowExists { row_exists: true, table_oid })?;

            let invalid_key = false;

            // Iterate over the columns, sending over the displayed value of that cell in the current row for each
            for column in columns.iter() {
                let row_oid: i64 = row.get(&*column.row_ord)?;

                let true_value: Option<String> = match column.true_ord.clone() {
                    Some(ord) => row.get::<&str, Option<String>>(&*ord)?,
                    None => None
                };
                let display_value: Option<String> = row.get(&*column.display_ord.clone())?;
                let mut failed_validations: Vec<error::FailedValidation> = Vec::<error::FailedValidation>::new();

                // Nullability validation
                if !column.is_nullable && display_value == None {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} cannot be NULL!", column.column_name)
                    });
                }

                // Uniqueness validation
                if column.invalid_nonunique_oid.contains(&row_oid) {
                    failed_validations.push(error::FailedValidation {
                        description: format!("{} value is not unique!", column.column_name)
                    });
                }

                // Primary key validation
                if column.is_primary_key && invalid_key {
                    failed_validations.push(error::FailedValidation {
                        description: format!("Primary key for this row is not unique!")
                    });
                }

                // Send the cell value to frontend
                cell_channel.send(RowCell::ColumnValue {
                    table_oid: column.table_oid,
                    row_oid: row_oid,
                    column_oid: column.column_oid, 
                    column_name: column.column_name.clone(),
                    column_type: column.column_type.clone(), 
                    column_ordering: column.column_ordering,
                    true_value: true_value,
                    display_value: display_value,
                    failed_validations: failed_validations
                })?;
            }

            // 
            return Ok(());
        }
    ) {
        Err(error::Error::RusqliteError(e)) => {
            match e {
                RusqliteError::QueryReturnedNoRows => {
                    cell_channel.send(RowCell::RowExists { row_exists: false, table_oid })?;
                    return Ok(());
                },
                _ => {
                    return Err(error::Error::from(e));
                }
            }
        },
        Err(e) => {
            return Err(e);
        }
        Ok(_) => {
            return Ok(());
        }
    }
}