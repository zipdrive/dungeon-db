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
        "INSERT INTO METADATA_RPT__REPORT (OID, BASE_TABLE_OID, NAME) VALUES (?1, ?2, ?3);",
        params![report_oid, base_table_oid, report_name],
    )?;

    // Commit the transaction
    trans.commit()?;
    return Ok(report_oid);
}

/// Edits the metadata of a report.
pub fn edit(report_oid: i64, report_name: &str) -> Result<String, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Record the old name of the table in metadata
    let old_report_name: String = trans.query_one(
        "SELECT NAME FROM METADATA_RPT__REPORT WHERE OID = ?1", 
        params![report_oid], 
        |row| row.get::<_, String>(0)
    )?;

    // Edit the name of the table in metadata
    trans.execute(
        "UPDATE METADATA_RPT__REPORT SET NAME = ?1 WHERE OID = ?2",
        params![report_name, report_oid],
    )?;

    // Commit the transaction
    trans.commit()?;
    return Ok(old_report_name);
}

/// Flags a report as trash.
pub fn trash(report_oid: i64) -> Result<(), error::Error> {
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
pub fn untrash(report_oid: i64) -> Result<(), error::Error> {
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



#[derive(Serialize)]
#[serde(rename_all="camelCase")]
pub struct BasicMetadata {
    pub oid: i64,
    pub name: String
}

/// Sends a list of reports through the provided channel.
pub fn send_metadata_list(report_channel: Channel<BasicMetadata>) -> Result<(), error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    db::query_iterate(
        &trans,
        "SELECT 
            rpt.OID, 
            rpt.NAME
        FROM METADATA_RPT__REPORT rpt
        WHERE rpt.TRASH = 0 
        ORDER BY rpt.NAME ASC;",
        [],
        &mut |row| {
            report_channel.send(BasicMetadata {
                oid: row.get::<_, i64>("OID")?,
                name: row.get::<_, String>("NAME")?,
            })?;
            return Ok(());
        },
    )?;
    return Ok(());
}


#[derive(Serialize)]
#[serde(rename_all="camelCase")]
pub struct Metadata {
    pub oid: i64,
    pub name: String,
    pub base_table_oid: i64
}

/// Gets metadata for a specified table.
pub fn get_metadata(report_oid: &i64) -> Result<Metadata, error::Error> {
    let mut conn = db::open()?;
    let trans = conn.transaction()?;

    // Retrieve the report name and the OID of the base table
    let (report_name, base_table_oid) = trans.query_one(
        "SELECT 
            NAME,
            BASE_TABLE_OID
        FROM METADATA_RPT__REPORT 
        WHERE TRASH = 0 AND OID = ?1;",
        params![report_oid],
        |row| { 
            Ok((row.get::<_, String>("NAME")?, row.get::<_, i64>("BASE_TABLE_OID")?))
        }
    )?;

    return Ok(Metadata {
        oid: report_oid.clone(),
        name: report_name,
        base_table_oid
    });
}