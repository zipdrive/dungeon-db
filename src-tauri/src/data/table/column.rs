use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use crate::util::db::{RowWrapper, sql_collect, sql_execute, sql_one};
use crate::util::db;
use crate::util::error::Error;
use crate::data::table::column_type::{TableColumnType, Primitive};
use crate::data::table::view;

#[derive(Serialize, Deserialize, Clone)]
pub struct TableColumnMetadata {
    pub oid: i64,
    pub name: String,
    pub column_type: TableColumnType,
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
                    "Text" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Text, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Text/Json" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::TextJson, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Text/Xml" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::TextXml, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Text/Markdown" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::TextMarkdown, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Text/BBCode" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::TextBBCode, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Boolean" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Boolean, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Integer" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Integer, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Number" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Number, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Date" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Date, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "Datetime" => TableColumnType::Primitive { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        primitive: Primitive::Datetime, 
                        default_value: row.get("DEFAULT_VALUE")? 
                    },
                    "File" => TableColumnType::File { 
                        oid: row.get("COLUMNTYPE_OID")?
                    },
                    "Object" => TableColumnType::Object { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        table_oid: row.get("COLUMNTYPE_TABLE_OID")? 
                    },
                    "SingleSelect" => TableColumnType::SingleSelect { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        table_oid: row.get("COLUMNTYPE_TABLE_OID")? 
                    },
                    "MultiSelect" => TableColumnType::MultiSelect { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        table_oid: row.get("COLUMNTYPE_TABLE_OID")? 
                    },
                    "Subreport" => TableColumnType::Subreport { 
                        oid: row.get("COLUMNTYPE_OID")?, 
                        report_oid: row.get("COLUMNTYPE_REPORT_OID")? 
                    },
                    _ => {
                        return Err(Error::adhoc(format!("\"{column_type_id}\" is not a known column type for tables!")));
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
            "SELECT * FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
            params![oid], 
            |row| Ok((
                row.get("TABLE_OID")?,
                Self::new(row)?
            ))
        )
    }

    /// Queries for all columns that are directly owned by the given table.
    pub fn query_direct_ownership(table_oid: i64) -> Result<Vec<Self>, Error> {
        let conn = db::open()?;
        Self::conn_query_direct_ownership(&conn, table_oid)
    }

    /// Queries for all columns that are directly owned by the given table.
    /// Uses the given connection.
    pub fn conn_query_direct_ownership(conn: &Connection, table_oid: i64) -> Result<Vec<Self>, Error> {
        sql_collect(
            conn, 
            "SELECT * FROM METADATA_TABLE_COLUMN WHERE TABLE_OID = ?1 ORDER BY ORDERING", 
            params![table_oid], 
            |row| Self::new(row)
        )
    }

    /// Queries for all columns that are directly owned or inherited by the given table.
    /// Returns tuples of the owning table OID, the datasource path from the given table, and the column metadata.
    pub fn query_all(table_oid: i64) -> Result<Vec<(i64, String, Self)>, Error> {
        let conn = db::open()?;
        Self::conn_query_all(&conn, table_oid)
    }

    /// Queries for all columns that are directly owned or inherited by the given table.
    /// Uses the given connection.
    /// Returns tuples of the owning table OID, the datasource path from the given table, and the column metadata.
    pub fn conn_query_all(conn: &Connection, table_oid: i64) -> Result<Vec<(i64, String, Self)>, Error> {
        sql_collect(
            conn, 
            "SELECT * FROM METADATA_TABLE_COLUMN_PATH WHERE TABLE_OID = ?1 ORDER BY ORDERING", 
            params![table_oid], 
            |row| Ok((
                row.get("BASE_TABLE_OID")?,
                row.get("DATASOURCE_PATH")?,
                Self::new(row)?
            ))
        )
    }


    /// Creates the column on the given table.
    /// Returns the OID of the owning table.
    pub fn create(&mut self, table_oid: i64) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create the column metadata and storage
        let table_oid = self.conn_create(&trans, table_oid)?;

        // Rebuild the table views
        view::rebuild(&trans, table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(table_oid)
    }

    /// Creates the column on the given table.
    /// This function does not trigger a rebuild of the table views.
    /// Uses the given connection.
    /// Returns the OID of the owning table.
    fn conn_create(&mut self, conn: &Connection, table_oid: i64) -> Result<i64, Error> {
        // Create row in column type metadata
        self.column_type.create(conn)?;

        // Create row in column metadata
        sql_execute(
            conn,
            "
INSERT INTO __METADATA_TABLE_COLUMN (
    TABLE_OID,
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
                table_oid,
                self.name,
                self.column_type.oid(),
                self.size,
                self.style,
                self.is_primary_key
            ]
        )?;
        self.oid = conn.last_insert_rowid();

        // Add to the storage table, if appropriate
        match &self.column_type {
            TableColumnType::Primitive { oid, primitive, default_value } => {
                let default_value_params = match default_value {
                    Some(default_value) => params![default_value.clone()],
                    None => params![]
                };
                sql_execute(
                    conn, 
                    format!(
                        "ALTER TABLE __TABLE{table_oid} ADD COLUMN COLUMN{} {} {}",
                        self.oid,
                        match primitive {
                            Primitive::Text
                            | Primitive::TextJson
                            | Primitive::TextXml
                            | Primitive::TextMarkdown
                            | Primitive::TextBBCode => "TEXT",
                            Primitive::Boolean
                            | Primitive::Integer => "INTEGER",
                            Primitive::Number
                            | Primitive::Date
                            | Primitive::Datetime => "REAL"
                        },
                        match default_value {
                            Some(_) => "DEFAULT ?1",
                            None => ""
                        }
                    ), 
                    default_value_params
                )?;
            }
            TableColumnType::File { oid } => {
                sql_execute(
                    conn, 
                    format!(
                        "ALTER TABLE __TABLE{table_oid} ADD COLUMN COLUMN{} INTEGER REFERENCES __METADATA_FILE (OID) ON UPDATE CASCADE ON DELETE SET NULL",
                        self.oid
                    ),
                    []
                )?;
            }
            TableColumnType::Object { oid, table_oid: referenced_table_oid }
            | TableColumnType::SingleSelect { oid, table_oid: referenced_table_oid } => {
                sql_execute(
                    conn, 
                    format!(
                        "ALTER TABLE __TABLE{table_oid} ADD COLUMN COLUMN{} INTEGER REFERENCES __TABLE{referenced_table_oid} (OID) ON UPDATE CASCADE ON DELETE SET NULL",
                        self.oid
                    ),
                    []
                )?;
            }
            TableColumnType::MultiSelect { oid, table_oid: referenced_table_oid } => {
                sql_execute(
                    conn, 
                    format!(
                        "
CREATE TABLE __MULTISELECT{} (
    TABLE{table_oid}_OID INTEGER NOT NULL REFERENCES __TABLE{table_oid} (OID) 
        ON UPDATE CASCADE 
        ON DELETE CASCADE,
    TABLE{referenced_table_oid}_OID INTEGER NOT NULL REFERENCES __TABLE{referenced_table_oid} (OID) 
        ON UPDATE CASCADE 
        ON DELETE CASCADE,
    PRIMARY KEY (TABLE{table_oid}_OID, TABLE{referenced_table_oid}_OID)
)
                        ",
                        self.oid
                    ),
                    []
                )?;
                sql_execute(
                    conn, 
                    format!(
                        "
CREATE VIEW MULTISELECT{} AS 
    SELECT 
        m.TABLE{table_oid}_OID,
        m.TABLE{referenced_table_oid}_OID 
    FROM __MULTISELECT{} m 
    INNER JOIN __TABLE{table_oid} t1 ON t1.OID = m.TABLE{table_oid}_OID 
    INNER JOIN __TABLE{referenced_table_oid} t2 ON t2.OID = m.TABLE{referenced_table_oid}_OID 
    WHERE NOT t1.TRASH AND NOT t2.TRASH
                        ",
                        self.oid,
                        self.oid
                    ),
                    []
                )?;
                sql_execute(
                    conn, 
                    format!(
                        "CREATE INDEX __MULTISELECT{}_INDEX_BY_TABLE{referenced_table_oid}_OID ON __MULTISELECT{} (TABLE{referenced_table_oid}_OID)",
                        self.oid,
                        self.oid
                    ),
                    []
                )?;
            }
            _ => {
                // Other column types are virtual and are not stored
            }
        }
        Ok(table_oid)
    }

    /// Sets the metadata for the column.
    /// Ignores column type and ordering.
    /// Returns the OID of the owning table.
    pub fn set_metadata(&self) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the table that owns the column being replaced
        let table_oid: i64 = sql_one(
            &trans, 
            "SELECT TABLE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
            params![self.oid], 
            |row| row.get("TABLE_OID")
        )?;

        // Overwrite the old metadata
        sql_execute(
            &trans, 
            "
UPDATE __METADATA_TABLE_COLUMN SET 
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
        Ok(table_oid)
    }

    /// Sets the ordering for the column.
    /// Returns the OID of the owning table.
    pub fn set_ordering(&self, ordering: i64) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the table that owns the column being replaced
        let table_oid: i64 = sql_one(
            &trans, 
            "SELECT TABLE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
            params![self.oid], 
            |row| row.get("TABLE_OID")
        )?;

        // Make space for the inserted ordering
        sql_execute(
            &trans, 
            "UPDATE __METADATA_TABLE_COLUMN SET ORDERING = -ORDERING WHERE ORDERING >= ?1",
            params![ordering]
        )?;
        // Set the specified column's ordering
        sql_execute(
            &trans, 
            "UPDATE __METADATA_TABLE_COLUMN SET ORDERING = ?1 WHERE OID = ?2", 
            params![ordering, self.oid]
        )?;
        // Shift all greater orderings to ensure there is space
        sql_execute(
            &trans, 
            "UPDATE __METADATA_TABLE_COLUMN SET ORDERING = 1-ORDERING WHERE ORDERING < 0", 
            []
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(table_oid)
    }

    /// Replaces a column.
    /// Returns the OID of the owning table.
    pub fn replace(&mut self, other: &Self) -> Result<i64, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Query for the OID of the table that owns the column being replaced
        let table_oid: i64 = sql_one(
            &trans, 
            "SELECT TABLE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1", 
            params![other.oid], 
            |row| row.get("TABLE_OID")
        )?;

        // Create the column metadata and storage
        self.conn_create(&trans, table_oid)?;

        // Copy data from the column being replaced
        // TODO

        // Trash the column being replaced
        Self::conn_trash(&trans, other.oid)?;

        // Rebuild the table views
        view::rebuild(&trans, table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(table_oid)
    }


    /// Trash the column.
    pub fn trash(table_oid: i64, column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Trash the column
        Self::conn_trash(&trans, column_oid)?;

        // Rebuild the table views
        view::rebuild(&trans, table_oid)?;

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
            "UPDATE __METADATA_TABLE_COLUMN SET TRASH = TRUE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }

    /// Untrash the column.
    pub fn untrash(table_oid: i64, column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Untrash the column
        Self::conn_untrash(&trans, column_oid)?;

        // Rebuild the table views
        view::rebuild(&trans, table_oid)?;

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
            "UPDATE __METADATA_TABLE_COLUMN SET TRASH = FALSE WHERE OID = ?1", 
            params![oid]
        )?;
        Ok(())
    }

    /// Trashes one column, and untrashs another.
    pub fn swap(table_oid: i64, trash_column_oid: i64, untrash_column_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Trash one column
        Self::conn_trash(&trans, trash_column_oid)?;
        // Untrash the other column
        Self::conn_untrash(&trans, untrash_column_oid)?;

        // Rebuild the table views
        view::rebuild(&trans, table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}