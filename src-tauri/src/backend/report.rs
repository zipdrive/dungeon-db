use crate::backend::{data_type, db, table};
use crate::util::error;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{params, Error as RusqliteError, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use std::cell::Ref;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use tauri::ipc::Channel;

/// Creates a report.
pub fn create(report_name: &str, base_table_oid: i64) -> Result<i64, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Create the metadata for the subreport
    trans.execute("INSERT INTO METADATA_RPT DEFAULT VALUES;", [])?;
    let report_oid: i64 = trans.last_insert_rowid();
    trans.execute(
        "INSERT INTO METADATA_RPT__REPORT (RPT_OID, BASE_TABLE_OID, NAME) VALUES (?1, ?2, ?3);",
        params![report_oid, base_table_oid, report_name],
    )?;

    return Ok(report_oid);
}

/// Flags a report as trash.
pub fn move_trash(report_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the report as trash
    trans.execute(
        "UPDATE METADATA_RPT SET TRASH = 1 WHERE OID = ?1;",
        params![report_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}

/// Unflags a table as trash.
pub fn unmove_trash(report_oid: i64) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Flag the table as trash
    trans.execute(
        "UPDATE METADATA_RPT SET TRASH = 0 WHERE OID = ?1;",
        params![report_oid],
    )?;

    // Commit and return
    trans.commit()?;
    return Ok(());
}
