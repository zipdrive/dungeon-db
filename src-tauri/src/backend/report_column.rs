use crate::backend::{data_type, db, table};
use crate::util::error;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Error as RusqliteError, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use std::cell::Ref;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use tauri::ipc::Channel;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
/// The most bare-bones version of table column metadata, used solely for populating the list of table columns
pub enum Metadata {
    Formula {
        oid: i64,
        name: String,
        column_ordering: i64,
        column_style: String,
        formula: String 
    },
    Subreport {
        oid: i64,
        name: String,
        column_ordering: i64,
        column_style: String,
        subreport_oid: i64
    }
}

/// Create a column based on a formula.
/// This may include columns that are just a static reference to a column in a table.
pub fn create_formula(
    report_oid: i64,
    column_name: &str,
    column_ordering: Option<i64>,
    column_style: &str,
    column_formula: &str,
) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_RPT_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE RPT_OID = ?1 AND COLUMN_ORDERING >= ?2;",
                params![report_oid, o]
            )?;
            o
        }
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING), 0) AS NEW_COLUMN_ORDERING FROM METADATA_RPT_COLUMN WHERE RPT_OID = ?1", 
                params![report_oid], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    // Create the metadata for the column
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN (NAME, COLUMN_ORDERING, CSS_COLUMN_STYLE) VALUES (?1, ?2, ?3);",
        params![column_name, column_ordering, column_style]
    )?;
    let column_oid: i64 = trans.last_insert_rowid();

    // Create the metadata for the formula
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN__FORMULA (RPT_COLUMN_OID, FORMULA) VALUES (?1, ?2);",
        params![column_oid, column_formula],
    )?;

    // Return the OID of the created column
    return Ok(column_oid);
}

/// Creates a column that is a report on rows linked to a row in the original report.
pub fn create_subreport(
    report_oid: i64,
    column_name: &str,
    column_ordering: Option<i64>,
    column_style: &str,
    base_parameter_oid: i64,
) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    let column_ordering: i64 = match column_ordering {
        Some(o) => {
            // If an explicit ordering was given, shift every column to its right by 1 in order to make space
            trans.execute(
                "UPDATE METADATA_RPT_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE RPT_OID = ?1 AND COLUMN_ORDERING >= ?2;",
                params![report_oid, o]
            )?;
            o
        }
        None => {
            // If no explicit ordering was given, insert at the back
            trans.query_one(
                "SELECT COALESCE(MAX(COLUMN_ORDERING), 0) AS NEW_COLUMN_ORDERING FROM METADATA_RPT_COLUMN WHERE RPT_OID = ?1", 
                params![report_oid], 
                |row| row.get::<_, i64>(0)
            )?
        }
    };

    // Create the metadata for the column
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN (NAME, COLUMN_ORDERING, CSS_COLUMN_STYLE) VALUES (?1, ?2, ?3);",
        params![column_name, column_ordering, column_style]
    )?;
    let column_oid: i64 = trans.last_insert_rowid();

    // Create the metadata for the subreport
    trans.execute("INSERT INTO METADATA_RPT DEFAULT VALUES;", [])?;
    let subreport_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_RPT_COLUMN__SUBREPORT (RPT_COLUMN_OID, RPT_OID, RPT_PARAMETER_OID) VALUES (?1, ?2, ?3);",
        params![column_oid, subreport_oid, base_parameter_oid]
    )?;

    return Ok(column_oid);
}

/// Flags a column as being trash.
pub fn trash(rpt_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the report as trash
    trans.execute(
        "UPDATE METADATA_RPT_COLUMN SET TRASH = 1 WHERE OID = ?1;",
        params![column_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a column as being trash.
pub fn untrash(rpt_oid: i64, column_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Unflag the report as trash
    trans.execute(
        "UPDATE METADATA_RPT_COLUMN SET TRASH = 0 WHERE OID = ?1;",
        params![column_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}



/// Get the metadata for a particular column.
pub fn get_metadata(column_oid: i64) -> Result<Option<Metadata>, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    return Ok(trans
        .query_one(
            "SELECT 
                c.OID, 
                c.NAME,
                c.COLUMN_ORDERING, 
                c.COLUMN_CSS_STYLE,
                CASE WHEN cf.RPT_COLUMN_OID IS NULL THEN 1 ELSE 0 END AS IS_SUBREPORT,
                cf.FORMULA,
                cs.RPT_OID AS SUBREPORT_OID,
                cs.RPT_PARAMETER_OID AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT_COLUMN c
            LEFT JOIN METADATA_RPT_COLUMN__SUBREPORT cs ON cs.RPT_COLUMN_OID = c.OID
            LEFT JOIN METADATA_RPT_COLUMN__FORMULA cf ON cf.RPT_COLUMN_OID = c.OID
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1",
            params![column_oid],
            |row| {
                let is_subreport: bool = row.get("IS_SUBREPORT")?;
                let column_oid: i64 = row.get("OID")?;
                let column_name: String = row.get("NAME")?;
                let column_ordering: i64 = row.get("COLUMN_ORDERING")?;
                let column_style: String = row.get("COLUMN_CSS_STYLE")?;
                return Ok(
                    if is_subreport {
                        Metadata::Subreport { 
                            oid: column_oid, 
                            name: column_name, 
                            column_ordering, 
                            column_style, 
                            subreport_oid: row.get("SUBREPORT_OID")? 
                        } 
                    } else {
                        Metadata::Formula {
                            oid: column_oid,
                            name: column_name,
                            column_ordering,
                            column_style,
                            formula: row.get("FORMULA")?
                        }
                    }
                );
            },
        )
        .optional()?);
}

/// Send a metadata list of columns.
pub fn send_metadata_list(
    report_oid: i64,
    column_channel: Channel<Metadata>,
) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(
        &trans,
        "SELECT 
                c.OID, 
                c.NAME,
                c.COLUMN_ORDERING, 
                c.COLUMN_CSS_STYLE,
                CASE WHEN cf.RPT_COLUMN_OID IS NULL THEN 1 ELSE 0 END AS IS_SUBREPORT,
                cf.FORMULA,
                cs.RPT_OID AS SUBREPORT_OID,
                cs.RPT_PARAMETER_OID AS SUBREPORT_BASE_PARAMETER_OID
            FROM METADATA_RPT_COLUMN c
            LEFT JOIN METADATA_RPT_COLUMN__SUBREPORT cs ON cs.RPT_COLUMN_OID = c.OID
            LEFT JOIN METADATA_RPT_COLUMN__FORMULA cf ON cf.RPT_COLUMN_OID = c.OID
            INNER JOIN METADATA_TYPE t ON t.OID = c.TYPE_OID
            WHERE c.OID = ?1 AND c.TRASH = 0
            ORDER BY c.COLUMN_ORDERING ASC;",
        params![report_oid],
        &mut |row| {
            let is_subreport: bool = row.get("IS_SUBREPORT")?;
            let column_oid: i64 = row.get("OID")?;
            let column_name: String = row.get("NAME")?;
            let column_ordering: i64 = row.get("COLUMN_ORDERING")?;
            let column_style: String = row.get("COLUMN_CSS_STYLE")?;
            column_channel.send(
                if is_subreport {
                    Metadata::Subreport { 
                        oid: column_oid, 
                        name: column_name, 
                        column_ordering, 
                        column_style, 
                        subreport_oid: row.get("SUBREPORT_OID")? 
                    } 
                } else {
                    Metadata::Formula {
                        oid: column_oid,
                        name: column_name,
                        column_ordering,
                        column_style,
                        formula: row.get("FORMULA")?
                    }
                }
            )?;
            return Ok(());
        },
    )?;
    return Ok(());
}

