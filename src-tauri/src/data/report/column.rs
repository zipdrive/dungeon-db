use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use crate::util::db::{RowWrapper, sql_collect, sql_execute, sql_one};
use crate::util::db;
use crate::util::error::Error;
use crate::data::report::column_type::ReportColumnType;

#[derive(Serialize, Deserialize, Clone)]
pub struct TableColumnMetadata {
    pub oid: i64,
    pub name: String,
    pub column_type: ReportColumnType,
    pub size: i64,
    pub style: String,
    pub is_primary_key: bool
}

impl TableColumnMetadata {
    /// Constructs a new TableColumnMetadata.
    fn new(row: &RowWrapper) -> Result<Self, Error> {
        Ok(Self {
            oid: row.get("OID")?,
            name: row.get("NAME")?,
            column_type: {
                let column_type_id: String = row.get("COLUMNTYPE")?;
                match column_type_id.as_str() {
                    "Formula" => ReportColumnType::Formula { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        formula: row.get("FORMULA")?
                    },
                    "Subreport" => ReportColumnType::Subreport { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        report_oid: row.get("COLUMNTYPE_REPORT_OID")? 
                    },
                    _ => {
                        return Err(Error::adhoc(format!("\"{column_type_id}\" is not a known column type for reports!")));
                    }
                }
            },
            size: row.get("SIZE")?,
            style: row.get("STYLE")?,
            is_primary_key: row.get("IS_PRIMARY_KEY")?
        })
    }

    /// Retrieves the metadata for a column.
    /// Returns a tuple of the owner table OID and the column metadata.
    pub fn get(oid: i64) -> Result<(i64, Self), Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, oid)
    }

    /// Retrieves the metadata for a column.
    /// Uses the given connection.
    /// Returns a tuple of the owner table OID and the column metadata.
    pub fn conn_get(conn: &Connection, oid: i64) -> Result<(i64, Self), Error> {
        sql_one(
            conn, 
            "SELECT * FROM METADATA_REPORT_COLUMN WHERE OID = ?1", 
            params![oid], 
            |row| Ok((
                row.get("REPORT_OID")?,
                Self::new(row)?
            ))
        )
    }

    /// Queries for all columns that are directly owned by the given report.
    pub fn query_all(report_oid: i64) -> Result<Vec<Self>, Error> {
        let conn = db::open()?;
        Self::conn_query_all(&conn, report_oid)
    }

    /// Queries for all columns that are directly owned by the given report.
    /// Uses the given connection.
    pub fn conn_query_all(conn: &Connection, report_oid: i64) -> Result<Vec<Self>, Error> {
        sql_collect(
            conn, 
            "SELECT * FROM METADATA_REPORT_COLUMN WHERE REPORT_OID = ?1 ORDER BY ORDERING", 
            params![report_oid], 
            |row| Self::new(row)
        )
    }


    /// Creates the column on the given table.
    /// Returns the OID of the owning table.
    pub fn create(&mut self, report_oid: i64) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create the column metadata and storage
        let report_oid = self.conn_create(&trans, report_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(report_oid)
    }

    /// Creates the column on the given table.
    /// This function does not trigger a rebuild of the table views.
    /// Uses the given connection.
    /// Returns the OID of the owning table.
    fn conn_create(&mut self, conn: &Connection, report_oid: i64) -> Result<i64, Error> {
        // Create row in column type metadata
        self.column_type.create(conn)?;

        // Create row in column metadata
        sql_execute(
            conn,
            "
INSERT INTO __METADATA_REPORT_COLUMN (
    REPORT_OID,
    NAME,
    COLUMNTYPE_OID,
    SIZE,
    STYLE,
    IS_PRIMARY_KEY 
) VALUES (
    ?1,
    ?2,
    ?3,
    ?4,
    ?5,
    ?6
)
            ", 
            params![
                report_oid,
                self.name,
                self.column_type.oid(),
                self.size,
                self.style,
                self.is_primary_key
            ]
        )?;
        self.oid = conn.last_insert_rowid();
        Ok(report_oid)
    }

    /// Sets the metadata for the column.
    /// Ignores column type and ordering.
    /// Returns the OID of the owning table.
    pub fn set_metadata(&self) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the report that owns the column being replaced
        let report_oid: i64 = sql_one(
            &trans, 
            "SELECT REPORT_OID FROM METADATA_REPORT_COLUMN WHERE OID = ?1", 
            params![self.oid], 
            |row| row.get("REPORT_OID")
        )?;

        // Overwrite the old metadata
        sql_execute(
            &trans, 
            "
UPDATE __METADATA_REPORT_COLUMN SET 
    NAME = ?1,
    SIZE = ?2,
    STYLE = ?3,
    IS_PRIMARY_KEY = ?4
WHERE OID = ?5
            ", 
            params![
                self.name,
                self.size,
                self.style,
                self.is_primary_key,
                self.oid
            ]
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(report_oid)
    }

    /// Sets the ordering for the column.
    /// Returns the OID of the owning table.
    pub fn set_ordering(&self, ordering: i64) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the report that owns the column being replaced
        let report_oid: i64 = sql_one(
            &trans, 
            "SELECT REPORT_OID FROM METADATA_REPORT_COLUMN WHERE OID = ?1", 
            params![self.oid], 
            |row| row.get("REPORT_OID")
        )?;

        // Make space for the inserted ordering
        sql_execute(
            &trans, 
            "UPDATE __METADATA_REPORT_COLUMN SET ORDERING = -ORDERING WHERE ORDERING >= ?1",
            params![ordering]
        )?;
        // Set the specified column's ordering
        sql_execute(
            &trans, 
            "UPDATE __METADATA_REPORT_COLUMN SET ORDERING = ?1 WHERE OID = ?2", 
            params![ordering, self.oid]
        )?;
        // Shift all greater orderings to ensure there is space
        sql_execute(
            &trans, 
            "UPDATE __METADATA_REPORT_COLUMN SET ORDERING = 1-ORDERING WHERE ORDERING < 0", 
            []
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(report_oid)
    }

    /// Replaces a column.
    /// Returns the OID of the owning table.
    pub fn replace(&mut self, other: &Self) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the report that owns the column being replaced
        let report_oid: i64 = sql_one(
            &trans, 
            "SELECT REPORT_OID FROM METADATA_REPORT_COLUMN WHERE OID = ?1", 
            params![self.oid], 
            |row| row.get("REPORT_OID")
        )?;

        // Create the column metadata and storage
        self.conn_create(&trans, report_oid)?;

        // Trash the column being replaced
        Self::conn_trash(&trans, other.oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(report_oid)
    }


    /// Trash the column.
    pub fn trash(report_oid: i64, column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Trash the column
        Self::conn_trash(&trans, column_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Trash the column.
    /// This function does not trigger a rebuild of the table views.
    /// Uses the given connection.
    pub fn conn_trash(conn: &Connection, oid: i64) -> Result<(), Error> {
        sql_execute(
            conn, 
            "UPDATE __METADATA_REPORT_COLUMN SET TRASH = TRUE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }

    /// Untrash the column.
    pub fn untrash(report_oid: i64, column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Untrash the column
        Self::conn_untrash(&trans, column_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Untrash the column.
    /// This function does not trigger a rebuild of the table views.
    /// Uses the given connection.
    pub fn conn_untrash(conn: &Connection, oid: i64) -> Result<(), Error> {
        sql_execute(
            conn, 
            "UPDATE __METADATA_REPORT_COLUMN SET TRASH = FALSE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }

    /// Trashes one column, and untrashs another.
    pub fn swap(report_oid: i64, trash_column_oid: i64, untrash_column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Trash one column
        Self::conn_trash(&trans, trash_column_oid)?;
        // Untrash the other column
        Self::conn_untrash(&trans, untrash_column_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}