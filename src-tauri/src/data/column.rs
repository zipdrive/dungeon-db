use crate::data::column_type;
use crate::data::schema;
use crate::data::view::regenerate_schema_views;
use crate::util::channel::Sender;
use crate::util::db;
use crate::util::error::Error;
use rusqlite::OptionalExtension;
use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Serialize, Clone)]
pub struct DropdownValue {
    pub label: String,
    pub value: i64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FullMetadata {
    pub oid: i64,
    pub hidden: bool,
    pub schema: schema::FullMetadata,
    pub name: String,
    pub column_type: column_type::ColumnType,
    pub style: String,
    pub ordering: i64,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl FullMetadata {
    /// Get the metadata of a column from its OID.
    pub fn get_transact(conn: &Connection, oid: i64) -> Result<FullMetadata, Error> {
        let (
            hidden,
            schema_oid,
            name,
            column_type_oid,
            style,
            ordering,
            default_value,
            is_primary_key,
        ) = conn.query_one(
            "
            SELECT
                c.HIDDEN,
                c.SCHEMA_OID,
                c.NAME,
                c.TYPE_OID,
                c.STYLE,
                c.ORDERING,
                c.DEFAULT_VALUE,
                c.IS_PRIMARY_KEY
            FROM METADATA_COLUMN c
            WHERE c.OID = ?1
            ",
            params![oid],
            |row| {
                Ok((
                    row.get::<_, bool>("HIDDEN")?,
                    row.get::<_, i64>("SCHEMA_OID")?,
                    row.get::<_, String>("NAME")?,
                    row.get::<_, i64>("TYPE_OID")?,
                    row.get::<_, String>("STYLE")?,
                    row.get::<_, i64>("ORDERING")?,
                    row.get::<_, Option<String>>("DEFAULT_VALUE")?,
                    row.get::<_, bool>("IS_PRIMARY_KEY")?,
                ))
            },
        )?;

        let schema: schema::FullMetadata = schema::FullMetadata::get(&conn, schema_oid)?;
        let column_type: column_type::ColumnType =
            column_type::ColumnType::get_transact(&conn, column_type_oid)?;
        Ok(Self {
            oid,
            hidden,
            schema,
            name,
            column_type,
            style,
            ordering,
            default_value,
            is_primary_key,
        })
    }

    /// Get the metadata of a column from its OID.
    pub fn get(oid: i64) -> Result<FullMetadata, Error> {
        let conn = db::open()?;
        Self::get_transact(&conn, oid)
    }

    /// Flags the column for garbage collection.
    pub fn trash(oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute(
            "UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1",
            params![oid],
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Unflags the column for garbage collection.
    pub fn untrash(oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute(
            "UPDATE METADATA_COLUMN SET TRASH = FALSE WHERE OID = ?1",
            params![oid],
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Simultaneously flags one column for garbage collection while unflagging another.
    pub fn trash_and_untrash(untrash_oid: i64, trash_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute(
            "UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1",
            params![trash_oid],
        )?;
        trans.execute(
            "UPDATE METADATA_COLUMN SET TRASH = FALSE WHERE OID = ?1",
            params![untrash_oid],
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Queries all columns belonging to a particular schema.
    pub fn query_by_schema(mut sender: Sender<Self>, schema_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        let mut select_statement = conn.prepare(
            "
            SELECT
                c.OID,
                c.HIDDEN,
                c.SCHEMA_OID,
                c.NAME,
                c.TYPE_OID,
                c.STYLE,
                c.ORDERING,
                c.DEFAULT_VALUE,
                c.IS_NULLABLE,
                c.IS_PRIMARY_KEY
            FROM METADATA_SCHEMA_COLUMN_VIEW sc
            INNER JOIN METADATA_COLUMN c ON c.OID = sc.COLUMN_OID
            WHERE sc.SCHEMA_OID = ?1
            ORDER BY c.ORDERING
            ",
        )?;
        for row_result in select_statement.query_map(params![schema_oid], |row| {
            Ok((
                row.get::<_, i64>("OID")?,
                row.get::<_, bool>("HIDDEN")?,
                row.get::<_, i64>("SCHEMA_OID")?,
                row.get::<_, String>("NAME")?,
                row.get::<_, i64>("TYPE_OID")?,
                row.get::<_, String>("STYLE")?,
                row.get::<_, i64>("ORDERING")?,
                row.get::<_, Option<String>>("DEFAULT_VALUE")?,
                row.get::<_, bool>("IS_PRIMARY_KEY")?,
            ))
        })? {
            let (
                oid,
                hidden,
                schema_oid,
                name,
                column_type_oid,
                style,
                ordering,
                default_value,
                is_primary_key,
            ) = row_result?;

            let schema: schema::FullMetadata = schema::FullMetadata::get(&conn, schema_oid)?;
            let column_type: column_type::ColumnType =
                column_type::ColumnType::get(column_type_oid)?;
            sender.send(Self {
                oid,
                hidden,
                schema,
                name,
                column_type,
                style,
                ordering,
                default_value,
                is_primary_key,
            })?;
        }
        Ok(())
    }

    /// Queries the tables that can be associated with an Object, Select, or Multiselect column.
    pub fn query_associated_tables(mut sender: Sender<DropdownValue>) -> Result<(), Error> {
        let conn = db::open()?;

        let mut select_stmt = conn.prepare("SELECT s.OID, s.NAME AS LABEL FROM METADATA_SCHEMA s INNER JOIN METADATA_TABLE t ON s.OID = t.OID WHERE NOT s.TRASH ORDER BY s.NAME")?;
        let select_rows = select_stmt.query_and_then([], |row| {
            Ok::<(i64, String), rusqlite::Error>((
                row.get::<_, i64>("OID")?,
                row.get::<_, String>("LABEL")?,
            ))
        })?;
        for row_result in select_rows {
            let (value, label) = row_result?;
            sender.send(DropdownValue { label, value })?;
        }

        Ok(())
    }

    /// Queries the reports that can be associated with an Object, Select, or Multiselect column.
    pub fn query_associated_reports(mut sender: Sender<DropdownValue>) -> Result<(), Error> {
        let conn = db::open()?;

        let mut select_stmt = conn.prepare("SELECT s.OID, s.NAME AS LABEL FROM METADATA_SCHEMA s INNER JOIN METADATA_REPORT r ON s.OID = r.OID WHERE NOT s.TRASH ORDER BY s.NAME")?;
        let select_rows = select_stmt.query_and_then([], |row| {
            Ok::<(i64, String), rusqlite::Error>((
                row.get::<_, i64>("OID")?,
                row.get::<_, String>("LABEL")?,
            ))
        })?;
        for row_result in select_rows {
            let (value, label) = row_result?;
            sender.send(DropdownValue { label, value })?;
        }

        Ok(())
    }

    /// Queries the values of a Select or Multiselect column for a schema.
    pub fn query_values(mut sender: Sender<DropdownValue>, schema_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        // Select the label from the schema's main view
        let sql_select = format!(
            "SELECT l.OID, COALESCE(l.PLAIN_LABEL, l.JSON_LABEL, '— NULL PRIMARY KEY —') AS LABEL FROM SCHEMA{schema_oid}_VIEW"
        );
        let mut select_stmt = conn.prepare(&sql_select)?;
        let select_rows = select_stmt.query_and_then([], |row| {
            Ok::<(i64, String), rusqlite::Error>((
                row.get::<_, i64>("OID")?,
                row.get::<_, String>("LABEL")?,
            ))
        })?;
        for row_result in select_rows {
            let (value, label) = row_result?;
            sender.send(DropdownValue { label, value })?;
        }

        Ok(())
    }

    /// Creates a new column.
    fn _create(&mut self, trans: &Transaction) -> Result<(), Error> {
        // Find the column type OID
        let column_type: column_type::ColumnType = self.column_type.clone();
        self.column_type = column_type.find_transact(trans)?;

        if self.ordering < 0 {
            // Set the ordering to the maximum
            self.ordering = trans
                .query_one(
                    "SELECT MAX(ORDERING) + 1 AS ORDERING FROM METADATA_COLUMN",
                    [],
                    |row| row.get::<_, Option<i64>>("ORDERING"),
                )
                .optional()?
                .unwrap_or(Some(1))
                .unwrap_or(1);
        } else {
            // Make space for the column by adjusting the ordering of any columns to the left of it
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = -ORDERING WHERE ORDERING >= ?1",
                params![self.ordering],
            )?;
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = 1 - ORDERING WHERE ORDERING < 0",
                [],
            )?;
        }

        // Insert the column metadata
        trans.execute(
            "
            INSERT INTO METADATA_COLUMN (
                HIDDEN,
                SCHEMA_OID,
                NAME,
                TYPE_OID,
                STYLE,
                ORDERING,
                IS_PRIMARY_KEY,
                DEFAULT_VALUE
            ) VALUES (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6,
                ?7,
                ?8
            )
            ",
            params![
                self.hidden,
                self.schema.oid,
                self.name,
                self.column_type.get_oid(),
                self.style,
                self.ordering,
                self.is_primary_key,
                self.default_value
            ],
        )?;
        self.oid = trans.last_insert_rowid();

        // If the column is not virtual, add it to the table
        match &self.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let cmd: String = format!(
                    "ALTER TABLE TABLE{} ADD COLUMN COLUMN{} {}", 
                    self.schema.oid,
                    self.oid,
                    match prim {
                        column_type::Primitive::PlainText
                        | column_type::Primitive::MarkdownText
                        | column_type::Primitive::JsonText
                        | column_type::Primitive::XmlText => "TEXT",
                        column_type::Primitive::Boolean
                        | column_type::Primitive::Integer => "INTEGER",
                        column_type::Primitive::Number
                        | column_type::Primitive::Date
                        | column_type::Primitive::Datetime => "REAL",
                        column_type::Primitive::File
                        | column_type::Primitive::Image => "INTEGER REFERENCES METADATA_FILE (OID) ON UPDATE CASCADE ON DELETE SET NULL"
                    }
                );
                trans.execute(&cmd, [])?;
            }
            column_type::ColumnType::Object { table_oid, .. }
            | column_type::ColumnType::Select { table_oid, .. } => {
                let cmd: String = format!(
                    "
                    ALTER TABLE TABLE{} ADD COLUMN COLUMN{} INTEGER 
                        REFERENCES TABLE{table_oid} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE SET DEFAULT
                    ",
                    self.schema.oid, self.oid
                );
                trans.execute(&cmd, [])?;
            }
            column_type::ColumnType::Multiselect { table_oid, .. } => {
                let cmd: String = format!(
                    "
                    CREATE TABLE MULTISELECT{} (
                        TABLE{}_OID INTEGER NOT NULL REFERENCES TABLE{} (OID)
                            ON UPDATE CASCADE
                            ON DELETE CASCADE,
                        TABLE{table_oid}_OID INTEGER NOT NULL REFERENCES TABLE{table_oid} (OID)
                            ON UPDATE CASCADE
                            ON DELETE CASCADE,
                        PRIMARY KEY (TABLE{}_OID, TABLE{table_oid}_OID)
                    );
                    CREATE VIEW MULTISELECT{}_VIEW AS
                        SELECT 
                            m.TABLE{}_OID,
                            m.TABLE{table_oid}_OID
                        FROM MULTISELECT{} m
                        INNER JOIN TABLE{} t1 ON t1.OID = m.TABLE{}_OID
                        INNER JOIN TABLE{table_oid} t2 ON t2.OID = m.TABLE{table_oid}_OID
                        WHERE NOT t1.TRASH AND NOT t2.TRASH
                    ;
                    ",
                    self.oid,
                    self.schema.oid,
                    self.schema.oid,
                    self.schema.oid,
                    self.oid,
                    self.schema.oid,
                    self.oid,
                    self.schema.oid,
                    self.schema.oid
                );
                trans.execute_batch(&cmd)?;
            }
            _ => {
                // Otherwise, a virtual column that requires nothing to be done
            }
        }

        // Regenerate the views for the schema hosting this column
        regenerate_schema_views(&trans, self.schema.oid)?;

        Ok(())
    }

    /// Create the column.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create the column
        self._create(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the column metadata.
    pub fn set(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let mut trans = conn.transaction()?;

        // Find the column type OID
        let old_column: Self = Self::get(self.oid)?;
        // Trash the old column
        trans.execute(
            "UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1",
            params![old_column.oid],
        )?;

        // Create a new column
        self._create(&trans)?;

        if old_column.column_type == self.column_type {
            // Do a batch update to copy over the data from the old column
            match self.column_type {
                column_type::ColumnType::Multiselect { table_oid, .. } => {
                    let sql_insert: String = format!(
                        "INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) SELECT TABLE{}_OID, TABLE{}_OID FROM MULTISELECT{}",
                        self.oid,
                        table_oid,
                        self.schema.oid,
                        table_oid,
                        self.schema.oid,
                        old_column.oid
                    );
                    trans.execute(&sql_insert, [])?;
                }
                column_type::ColumnType::Primitive(_)
                | column_type::ColumnType::Object { .. }
                | column_type::ColumnType::Select { .. } => {
                    let sql_update: String = format!(
                        "UPDATE TABLE{} SET COLUMN{} = COLUMN{}",
                        self.schema.oid, self.oid, old_column.oid
                    );
                    trans.execute(&sql_update, [])?;
                }
                _ => {} // Do nothing, because column is virtual
            }
        } else {
            // Try to update each row individually, copying over the data from the old column
            match &self.column_type {
                column_type::ColumnType::Primitive(prim) => {
                    match prim {
                        column_type::Primitive::PlainText 
                        | column_type::Primitive::MarkdownText
                        | column_type::Primitive::JsonText
                        | column_type::Primitive::XmlText => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = l.COLUMN{}_LABEL 
                                FROM SCHEMA{}_VIEW l 
                                WHERE l.OID = t.OID
                                ",
                                self.schema.oid, 
                                self.oid, self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::Integer => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = 
                                    COALESCE(
                                        NULLIF(CAST(l.COLUMN{}_LABEL AS INTEGER), 0),
                                        IF(l.COLUMN{}_LABEL LIKE '0%', 0, NULL)
                                    )
                                FROM SCHEMA{}_VIEW l 
                                WHERE t.OID = l.OID
                                ",
                                self.schema.oid, 
                                self.oid, 
                                self.oid, self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::Number => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = 
                                    COALESCE(
                                        NULLIF(CAST(l.COLUMN{}_LABEL AS REAL), 0.0),
                                        IF(l.COLUMN{}_LABEL LIKE '0%', 0.0, NULL)
                                    )
                                FROM SCHEMA{}_VIEW l 
                                WHERE t.OID = l.OID
                                ",
                                self.schema.oid, 
                                self.oid, 
                                self.oid, self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::Date => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = JULIANDAY(l.COLUMN{}_LABEL, 'start of day')
                                FROM SCHEMA{}_VIEW l 
                                WHERE t.OID = l.OID
                                ",
                                self.schema.oid, 
                                self.oid,
                                self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::Datetime => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = JULIANDAY(l.COLUMN{}_LABEL)
                                FROM SCHEMA{}_VIEW l 
                                WHERE t.OID = l.OID
                                ",
                                self.schema.oid, 
                                self.oid, 
                                self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::Boolean => {
                            // Do batch update, because there shouldn't be any chance of failure
                            let sql_update: String = format!(
                                "
                                UPDATE TABLE{} AS t 
                                SET COLUMN{} = 
                                    CASE 
                                        WHEN l.COLUMN{}_LABEL IS NULL THEN NULL
                                        ELSE (l.COLUMN{}_LABEL IS NOT 'false' 
                                            AND l.COLUMN{}_LABEL IS NOT '0')
                                    END
                                FROM SCHEMA{}_VIEW l 
                                WHERE t.OID = l.OID
                                ",
                                self.schema.oid, 
                                self.oid, 
                                self.oid, self.oid, self.oid,
                                self.schema.oid
                            );
                            trans.execute(&sql_update, [])?;
                        }
                        column_type::Primitive::File | column_type::Primitive::Image => {
                            if let Some(file_expr) = match &old_column.column_type {
                                column_type::ColumnType::Primitive(old_prim) => {
                                    let old_column_expr: String =
                                        format!("t.COLUMN{}", old_column.oid);

                                    // Only copy if the previous column was also a file type
                                    // TODO otherwise try to match up the file label?
                                    match old_prim {
                                        column_type::Primitive::File
                                        | column_type::Primitive::Image => Some(old_column_expr),
                                        column_type::Primitive::Boolean
                                        | column_type::Primitive::Date
                                        | column_type::Primitive::Datetime
                                        | column_type::Primitive::Integer
                                        | column_type::Primitive::Number
                                        | column_type::Primitive::PlainText
                                        | column_type::Primitive::MarkdownText
                                        | column_type::Primitive::JsonText 
                                        | column_type::Primitive::XmlText => None, // No conversion from other primitive to File
                                    }
                                }
                                _ => None, // No data to transfer to File column
                            } {
                                // Do batch update, because there shouldn't be any chance of failure
                                let sql_update: String = format!(
                                    "UPDATE TABLE{} AS t SET COLUMN{} = {file_expr}",
                                    self.schema.oid, self.oid
                                );
                                trans.execute(&sql_update, [])?;
                            }
                        }
                    }
                }
                column_type::ColumnType::Object { table_oid, .. } 
                | column_type::ColumnType::Select { table_oid, .. } => {
                    // Do batch update, because there shouldn't be any chance of failure
                    let sql_update: String = format!(
                        "
                        UPDATE TABLE{} AS t 
                        SET COLUMN{} = l2.OID
                        FROM SCHEMA{}_VIEW l 
                        LEFT JOIN SCHEMA{table_oid} VIEW l2 
                            ON l2.PLAIN_LABEL = l.COLUMN{}_LABEL 
                                OR l2.JSON_LABEL = l.COLUMN{}_LABEL 
                                OR l2.OBJECT_LABEL = l.COLUMN{}_LABEL
                        WHERE t.OID = l.OID
                        ",
                        self.schema.oid, 
                        self.oid, 
                        self.schema.oid,
                        self.oid, self.oid, self.oid,
                    );
                    trans.execute(&sql_update, [])?;
                }
                column_type::ColumnType::Multiselect { table_oid, .. } => {
                    // Match rows in the new associated table on an individual basis, using the JSON label
                    let sql_insert: String = format!(
                        "
                        INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) 
                        SELECT 
                            t1.OID AS TABLE{}_OID,
                            t2.OID AS TABLE{}_OID
                        FROM SCHEMA{}_VIEW t1 
                        INNER JOIN SCHEMA{}_VIEW t2 
                            ON t2.PLAIN_LABEL = t1.COLUMN{}_LABEL 
                                OR t2.JSON_LABEL = t1.COLUMN{}_LABEL 
                                OR t2.OBJECT_LABEL = t1.COLUMN{}_LABEL
                        ",
                        self.oid, self.schema.oid, table_oid,
                        self.schema.oid, table_oid,
                        self.schema.oid,
                        table_oid,
                        self.oid, self.oid, self.oid
                    );
                    trans.execute(&sql_insert, [])?;
                }
                _ => {} // No copy necessary
            }
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Sets only the CSS style of the column.
    pub fn set_style(&mut self, new_style: String) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Update the style in the database
        self.style = new_style;
        trans.execute(
            "UPDATE METADATA_COLUMN SET STYLE = ?1 WHERE OID = ?2",
            params![self.style, self.oid],
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Sets only the ordering of the column.
    pub fn set_ordering(&mut self, new_ordering: Option<i64>) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Update the ordering in the database
        if let Some(new_ordering) = new_ordering {
            self.ordering = new_ordering;
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = -ORDERING WHERE ORDERING >= ?1",
                params![self.ordering],
            )?;
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = ?1 WHERE OID = ?2",
                params![self.ordering, self.oid],
            )?;
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = 1 - ORDERING WHERE ORDERING < 0",
                [],
            )?;
        } else {
            self.ordering = trans
                .query_one("SELECT MAX(ORDERING) + 1 FROM METADATA_COLUMN", [], |row| {
                    row.get::<_, Option<i64>>(0)
                })
                .optional()?
                .unwrap_or(Some(1))
                .unwrap_or(1);
            trans.execute(
                "UPDATE METADATA_COLUMN SET ORDERING = ?1 WHERE OID = ?2",
                params![self.ordering, self.oid],
            )?;
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}
