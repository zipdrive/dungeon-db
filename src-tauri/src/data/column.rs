use crate::util::error::Error;
use crate::util::db;
use crate::util::channel::Sender;
use crate::data::schema;
use crate::data::column_type;
use rusqlite::{params};
use serde::{Serialize, Deserialize};
use std::hash::{Hash, Hasher};

#[derive(Serialize, Clone)]
pub struct DropdownValue {
    label: Option<String>,
    value: Option<i64>
}



#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all="camelCase")]
pub struct FullMetadata {
    pub oid: i64,
    pub hidden: bool,
    pub schema: schema::FullMetadata,
    pub name: String,
    pub column_type: column_type::ColumnType,
    pub style: String,
    pub ordering: i64,
    pub default_value: Option<String>,
    pub is_nullable: bool,
    pub is_unique: bool,
    pub is_primary_key: bool
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.oid.hash(state)
    }
}

impl FullMetadata {
    /// Get the metadata of a column from its OID.
    pub fn get(oid: i64) -> Result<FullMetadata, Error> {
        let conn = db::open()?;
        let (
            hidden,
            schema_oid,
            name,
            column_type_oid,
            style,
            ordering,
            default_value,
            is_nullable,
            is_unique,
            is_primary_key
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
                c.IS_NULLABLE,
                c.IS_UNIQUE,
                c.IS_PRIMARY_KEY
            FROM METADATA_COLUMN c
            WHERE c.OID = ?1
            ",
            params![oid],
            |row| {Ok((
                row.get::<_, bool>("HIDDEN")?,
                row.get::<_, i64>("SCHEMA_OID")?,
                row.get::<_, String>("NAME")?,
                row.get::<_, i64>("TYPE_OID")?,
                row.get::<_, String>("STYLE")?,
                row.get::<_, i64>("ORDERING")?,
                row.get::<_, Option<String>>("DEFAULT_VALUE")?,
                row.get::<_, bool>("IS_NULLABLE")?,
                row.get::<_, bool>("IS_UNIQUE")?,
                row.get::<_, bool>("IS_PRIMARY_KEY")?
            ))}
        )?;

        let schema: schema::FullMetadata = schema::FullMetadata::get(&conn, schema_oid)?;
        let column_type: column_type::ColumnType = column_type::ColumnType::get(column_type_oid)?;
        Ok(Self {
            oid,
            hidden,
            schema,
            name,
            column_type,
            style,
            ordering,
            default_value,
            is_nullable,
            is_unique,
            is_primary_key
        })
    }

    /// Flags the column for garbage collection.
    pub fn trash(oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute("UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1", params![oid])?;
        trans.commit()?;
        Ok(())
    }

    /// Unflags the column for garbage collection.
    pub fn untrash(oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute("UPDATE METADATA_COLUMN SET TRASH = FALSE WHERE OID = ?1", params![oid])?;
        trans.commit()?;
        Ok(())
    }

    /// Simultaneously flags one column for garbage collection while unflagging another.
    pub fn trash_and_untrash(untrash_oid: i64, trash_oid: i64) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;
        trans.execute("UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1", params![trash_oid])?;
        trans.execute("UPDATE METADATA_COLUMN SET TRASH = FALSE WHERE OID = ?1", params![untrash_oid])?;
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
                c.IS_UNIQUE,
                c.IS_PRIMARY_KEY
            FROM SUPERSCHEMAS s
            INNER JOIN METADATA_COLUMN c ON c.SCHEMA_OID = s.OID
            ORDER BY c.ORDERING
            "
        )?;
        for row_result in select_statement.query_map(
            params![schema_oid],
            |row| {Ok((
                row.get::<_, i64>("OID")?,
                row.get::<_, bool>("HIDDEN")?,
                row.get::<_, i64>("SCHEMA_OID")?,
                row.get::<_, String>("NAME")?,
                row.get::<_, i64>("TYPE_OID")?,
                row.get::<_, String>("STYLE")?,
                row.get::<_, i64>("ORDERING")?,
                row.get::<_, Option<String>>("DEFAULT_VALUE")?,
                row.get::<_, bool>("IS_NULLABLE")?,
                row.get::<_, bool>("IS_UNIQUE")?,
                row.get::<_, bool>("IS_PRIMARY_KEY")?
            ))}
        )? {
            let (
                oid,
                hidden,
                schema_oid,
                name,
                column_type_oid,
                style,
                ordering,
                default_value,
                is_nullable,
                is_unique,
                is_primary_key
            ) = row_result?;

            let schema: schema::FullMetadata = schema::FullMetadata::get(&conn, schema_oid)?;
            let column_type: column_type::ColumnType = column_type::ColumnType::get(column_type_oid)?;
            sender.send(Self {
                oid,
                hidden,
                schema,
                name,
                column_type,
                style,
                ordering,
                default_value,
                is_nullable,
                is_unique,
                is_primary_key
            })?;
        }
        Ok(())
    }

    /// Queries the values of a Select or Multiselect column for a schema.
    pub fn query_values(mut sender: Sender<DropdownValue>, schema_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        let sql_select = format!("SELECT OID, LABEL FROM TABLE{schema_oid}_SURROGATE");
        let mut select_stmt = conn.prepare(&sql_select)?;
        let select_rows = select_stmt.query_and_then([], |row| Ok::<(i64, Option<String>), rusqlite::Error>((row.get::<_, i64>("OID")?, row.get::<_, Option<String>>("LABEL")?)))?;
        for row_result in select_rows {
            let (value, label) = row_result?;
            sender.send(DropdownValue { label, value: Some(value) })?;
        }

        Ok(())
    }

    /// Create the column.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Find the column OID
        let column_type: column_type::ColumnType = self.column_type.clone();
        self.column_type = column_type.find()?;

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
                IS_NULLABLE,
                IS_UNIQUE,
                IS_PRIMARY_KEY
            ) VALUES (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6,
                ?7,
                ?8,
                ?9
            )
            ", 
            params![
                self.hidden,
                self.schema.oid,
                self.name,
                self.column_type.get_oid(),
                self.style,
                self.ordering,
                self.is_nullable,
                self.is_unique,
                self.is_primary_key
            ]
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
                        column_type::Primitive::Text
                        | column_type::Primitive::JSON => "TEXT",
                        column_type::Primitive::Checkbox
                        | column_type::Primitive::Integer => "INTEGER",
                        column_type::Primitive::Number
                        | column_type::Primitive::Date
                        | column_type::Primitive::Datetime => "REAL",
                        column_type::Primitive::File
                        | column_type::Primitive::Image => "BLOB"
                    }
                );
                trans.execute(&cmd, [])?;
            }
            column_type::ColumnType::Object { table_oid, .. }
            | column_type::ColumnType::Select { table_oid, .. } => {
                let cmd: String = format!(
                    "
                    ALTER TABLE TABLE{} ADD COLUMN COLUMN{} INTEGER 
                        REFERENCES TABLE{} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE SET DEFAULT
                    ", 
                    self.schema.oid,
                    self.oid,
                    table_oid
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
                        TABLE{}_OID INTEGER NOT NULL REFERENCES TABLE{} (OID)
                            ON UPDATE CASCADE
                            ON DELETE CASCADE,
                        PRIMARY KEY (TABLE{}_OID, TABLE{}_OID)
                    );
                    ",
                    self.oid,
                    self.schema.oid,
                    self.schema.oid,
                    table_oid,
                    table_oid,
                    self.schema.oid,
                    table_oid
                );
                trans.execute(&cmd, [])?;
            }
            column_type::ColumnType::Formula { oid, formula } => {
                // If the column is a formula, make a view for the values therein
            }
            _ => {
                // Otherwise, a virtual column that requires nothing to be done
            }
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the column metadata.
    pub fn set(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let mut trans = conn.transaction()?;

        // Find the column type OID
        self.column_type = self.column_type.clone().find()?;

        // Update the column metadata

        trans.execute(
            "
            UPDATE METADATA_COLUMN
            SET
                HIDDEN = ?1,
                NAME = ?2,
                TYPE_OID = ?3,
                STYLE = ?4,
                ORDERING = ?5,
                IS_NULLABLE = ?6,
                IS_UNIQUE = ?7,
                IS_PRIMARY_KEY = ?8
            WHERE OID = ?9
            ", 
            params![
                self.hidden,
                self.name,
                self.column_type.get_oid(),
                self.style,
                self.ordering,
                self.is_nullable,
                self.is_unique,
                self.is_primary_key,
                self.oid
            ]
        )?;

        // Drop any existing non-virtual columns
        {
            let cmd: String = format!(
                "
                DROP TABLE IF EXISTS MULTISELECT{};
                DROP VIEW IF EXISTS FORMULA{};
                ", 
                self.oid,
                self.oid
            );
            trans.execute(&cmd, [])?;
        }
        {
            let savepoint = trans.savepoint()?;
            let cmd: String = format!("ALTER TABLE TABLE{} DROP COLUMN COLUMN{}", self.schema.oid, self.oid);
            match savepoint.execute(&cmd, []) {
                Ok(_) => {
                    savepoint.commit()?;
                }
                _ => {}
            }
        }

        // If the column is not virtual, add it to the table
        match &self.column_type {
            column_type::ColumnType::Primitive(prim) => {
                let cmd: String = format!(
                    "ALTER TABLE TABLE{} ADD COLUMN COLUMN{} {}", 
                    self.schema.oid,
                    self.oid,
                    match prim {
                        column_type::Primitive::Text
                        | column_type::Primitive::JSON => "TEXT",
                        column_type::Primitive::Checkbox
                        | column_type::Primitive::Integer => "INTEGER",
                        column_type::Primitive::Number
                        | column_type::Primitive::Date
                        | column_type::Primitive::Datetime => "REAL",
                        column_type::Primitive::File
                        | column_type::Primitive::Image => "BLOB"
                    }
                );
                trans.execute(&cmd, [])?;
            }
            column_type::ColumnType::Object { table_oid, .. }
            | column_type::ColumnType::Select { table_oid, .. } => {
                let cmd: String = format!(
                    "
                    ALTER TABLE TABLE{} ADD COLUMN COLUMN{} INTEGER 
                        REFERENCES TABLE{} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE SET DEFAULT
                    ", 
                    self.schema.oid,
                    self.oid,
                    table_oid
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
                        TABLE{}_OID INTEGER NOT NULL REFERENCES TABLE{} (OID)
                            ON UPDATE CASCADE
                            ON DELETE CASCADE,
                        PRIMARY KEY (TABLE{}_OID, TABLE{}_OID)
                    );
                    ",
                    self.oid,
                    self.schema.oid,
                    self.schema.oid,
                    table_oid,
                    table_oid,
                    self.schema.oid,
                    table_oid
                );
                trans.execute(&cmd, [])?;
            }
            _ => {
                // Virtual column, so do nothing
            }
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}