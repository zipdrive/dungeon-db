use crate::util::error::Error;
use crate::util::db;
use crate::util::channel::Sender;
use crate::data::schema;
use crate::data::column_type;
use rusqlite::Transaction;
use rusqlite::{params};
use serde::{Serialize, Deserialize};
use std::hash::{Hash, Hasher};

#[derive(Serialize, Clone)]
pub struct DropdownValue {
    label: String,
    value: i64
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
            FROM METADATA_COLUMN c
            LEFT JOIN METADATA_SCHEMA_INHERITANCE_VIEW s ON c.SCHEMA_OID = s.MASTER_SCHEMA_OID
            WHERE COALESCE(s.INHERITOR_SCHEMA_OID, c.SCHEMA_OID) = ?1 AND NOT c.TRASH
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
                is_primary_key
            })?;
        }
        Ok(())
    }

    /// Queries the tables that can be associated with an Object, Select, or Multiselect column.
    pub fn query_associated_tables(mut sender: Sender<DropdownValue>) -> Result<(), Error> {
        let conn = db::open()?;

        let mut select_stmt = conn.prepare("SELECT s.OID, s.NAME AS LABEL FROM METADATA_SCHEMA s INNER JOIN METADATA_TABLE t ON s.OID = t.OID")?;
        let select_rows = select_stmt.query_and_then([], |row| Ok::<(i64, String), rusqlite::Error>((row.get::<_, i64>("OID")?, row.get::<_, String>("LABEL")?)))?;
        for row_result in select_rows {
            let (value, label) = row_result?;
            sender.send(DropdownValue { label, value })?;
        }

        Ok(())
    }

    /// Queries the values of a Select or Multiselect column for a schema.
    pub fn query_values(mut sender: Sender<DropdownValue>, schema_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        let sql_select = format!("SELECT OID, LABEL FROM TABLE{schema_oid}_SURROGATE");
        let mut select_stmt = conn.prepare(&sql_select)?;
        let select_rows = select_stmt.query_and_then([], |row| Ok::<(i64, String), rusqlite::Error>((row.get::<_, i64>("OID")?, row.get::<_, String>("LABEL")?)))?;
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
        let trans = conn.transaction()?;

        // Find the column type OID
        let old_column: Self = Self::get(self.oid)?;
        // Trash the old column
        trans.execute("UPDATE METADATA_COLUMN SET TRASH = TRUE WHERE OID = ?1", params![old_column.oid])?;

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
                        self.schema.oid,
                        self.oid,
                        old_column.oid
                    );
                    trans.execute(&sql_update, [])?;
                }
            }
        } else {
            // Try to update each row individually, copying over the data from the old column
            if let Some(sql_update) = match &self.column_type {
                column_type::ColumnType::Primitive(prim) => {

                }
                column_type::ColumnType::Object { table_oid, .. } 
                | column_type::ColumnType::Select { table_oid, .. } => {
                    // Match rows in the new associated table on an individual basis, using the JSON label
                    if let Some(json_label_expr) = match &old_column.column_type {
                        column_type::ColumnType::Primitive(old_prim) => {
                            let old_column_label: String = format!("t.COLUMN{}", old_column.oid);
                            match old_prim {
                                column_type::Primitive::File 
                                | column_type::Primitive::Image => None,
                                column_type::Primitive::Date => Some(format!(r#"COALESCE('"' || DATE({old_column_label}, 'julianday') || '"', 'null')"#, old_column.oid)),
                                column_type::Primitive::Datetime => Some(format!(r#"COALESCE('"' || STRFTIME('%FT%TZ', {old_column_label}, 'julianday') || '"', 'null')"#, old_column.oid)),
                                column_type::Primitive::Checkbox => Some(format!(r#"CASE WHEN {old_column_label} IS NULL THEN 'null' WHEN {old_column_label} THEN 'true' ELSE 'false' END"#, old_column.oid, old_column.oid)),
                                _ => Some(format!(r#"COALESCE('"' || CAST({old_column_label} AS TEXT) || '"', 'null')"#))
                            }
                        }
                        column_type::ColumnType::Object { old_table_oid, .. } 
                        | column_type::ColumnType::Select { old_table_oid, .. } => {
                            if table_oid == old_table_oid {
                                // Can be updated directly, rather than needing to use labels to identify rows
                                let sql_update: String = format!(
                                    "UPDATE TABLE{} SET COLUMN{} = COLUMN{}",
                                    self.schema.oid,
                                    self.oid,
                                    old_column.oid
                                );
                                trans.execute(&sql_update, [])?;

                                // Don't update rows individually
                                None 
                            } else {
                                let old_column_label: String = format!("t.COLUMN{}", old_column.oid);
                                Some(format!("(SELECT JSON_LABEL FROM TABLE{old_table_oid}_SURROGATE WHERE OID = {old_column_label})"))
                            }
                        }
                        column_type::ColumnType::Multiselect { old_table_oid, .. } => {
                            if table_oid == old_table_oid {
                                // Can be updated more or less directly, rather than needing to use labels to identify rows
                                // Pick which one to assign to the new column arbitrarily
                                let sql_update: String = format!(
                                    "UPDATE TABLE{} t SET t.COLUMN{} = (SELECT MIN(TABLE{table_oid}_OID) FROM MULTISELECT{} WHERE TABLE{}_OID = t.OID)",
                                    self.schema.oid,
                                    old_column.oid,
                                    old_column.schema.oid
                                );
                                trans.execute(&sql_update, [])?;

                                // Don't update rows individually
                                None 
                            } else {
                                // Use the array of selected rows as the label
                                Some(format!(
                                    "(SELECT GROUP_CONCAT('[' || s.JSON_LABEL || ']') FROM MULTISELECT{} m INNER JOIN TABLE{old_table_oid}_SURROGATE s ON s.OID = m.TABLE{old_table_oid}_OID WHERE m.TABLE{}_OID = t.OID)", 
                                    old_column.oid,
                                    old_column.schema.oid
                                ))
                            }
                        }
                        _ => None
                    } {
                        Some(format!(
                            "
                            UPDATE TABLE{} t SET
                                t.COLUMN{} = {json_label_expr}
                            WHERE t.OID = ?1
                            "
                        ))
                    } else {
                        None
                    }
                }
                column_type::ColumnType::Multiselect { table_oid, .. } => {
                    // Match rows in the new associated table on an individual basis, using the JSON label
                    match &old_column.column_type {
                        column_type::ColumnType::Primitive(old_prim) => {
                            let old_column_label: String = format!("t.COLUMN{}", old_column.oid);
                            if let Some(json_label_expr) = match old_prim {
                                column_type::Primitive::File 
                                | column_type::Primitive::Image => None,
                                column_type::Primitive::Date => Some(format!(r#"COALESCE('"' || DATE({old_column_label}, 'julianday') || '"', 'null')"#, old_column.oid)),
                                column_type::Primitive::Datetime => Some(format!(r#"COALESCE('"' || STRFTIME('%FT%TZ', {old_column_label}, 'julianday') || '"', 'null')"#, old_column.oid)),
                                column_type::Primitive::Checkbox => Some(format!(r#"CASE WHEN {old_column_label} IS NULL THEN 'null' WHEN {old_column_label} THEN 'true' ELSE 'false' END"#, old_column.oid, old_column.oid)),
                                _ => Some(format!(r#"COALESCE('"' || CAST({old_column_label} AS TEXT) || '"', 'null')"#))
                            } {
                                Some(format!(
                                    "
                                    INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) VALUES (?1, (SELECT t.OID FROM TABLE{table_oid}_SURROGATE t WHERE t.JSON_LABEL = {json_label_expr}))
                                    ",
                                    self.oid,
                                    self.schema.oid,
                                    table_oid
                                ))
                            } else {
                                None
                            }
                        }
                        column_type::ColumnType::Object { old_table_oid, .. } 
                        | column_type::ColumnType::Select { old_table_oid, .. } => {
                            if table_oid == old_table_oid {
                                // Can be updated directly, rather than needing to use labels to identify rows
                                let sql_update: String = format!(
                                    "
                                    INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) 
                                    SELECT 
                                        OID,
                                        COLUMN{}
                                    FROM TABLE{}",
                                    self.oid,
                                    self.schema.oid,
                                    table_oid,
                                    old_column.oid,
                                    self.schema.oid
                                );
                                trans.execute(&sql_update, [])?;

                                // Don't update rows individually
                                None 
                            } else {
                                let old_column_label: String = format!("t.COLUMN{}", old_column.oid);
                                Some(format!(
                                    "
                                    INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{}_OID) VALUES (
                                        ?1, 
                                        (
                                            SELECT OID FROM TABLE{table_oid}_SURROGATE WHERE JSON_LABEL = (
                                                SELECT 
                                                    JSON_LABEL 
                                                FROM TABLE{} t 
                                                INNER JOIN TABLE{old_table_oid}_SURROGATE s ON s.OID = {old_column_label}
                                                WHERE t.OID = ?1
                                            )
                                        )
                                    )
                                    ",
                                    self.oid,
                                    self.schema.oid,
                                    table_oid,
                                    self.schema.oid
                                ))
                            }
                        }
                        column_type::ColumnType::Multiselect { old_table_oid, .. } => {
                            Some(format!(
                                "
                                INSERT INTO MULTISELECT{} (TABLE{}_OID, TABLE{table_oid}_OID) 
                                SELECT
                                    u.TABLE{}_OID, 
                                    (
                                        SELECT t.OID FROM TABLE{table_oid}_SURROGATE t WHERE t.JSON_LABEL = (
                                            SELECT JSON_LABEL FROM TABLE{old_table_oid}_SURROGATE WHERE OID = u.TABLE{old_table_oid}_OID
                                        )
                                    )
                                FROM MULTISELECT{} u
                                WHERE u.TABLE{}_OID = ?1
                                ",
                                self.oid,
                                self.schema.oid,
                                self.schema.oid,
                                old_column.oid,
                                self.schema.oid
                            ))
                        }
                        _ => None
                    }
                }
                _ => {} // No copy necessary
            } {
                // Iterate over each row and try to update each row on an individual basis
            }


            match &old_column.column_type {
                column_type::ColumnType::Primitive(old_prim) => {
                    match old_prim {
                        column_type::Primitive::File 
                        | column_type::Primitive::Image => {
                            // Only copy if the new column type is also a File
                            match &self.column_type {
                                column_type::ColumnType::Primitive(prim) => {
                                    match prim {
                                        column_type::Primitive::File 
                                        | column_type::Primitive::Image => {

                                        }
                                        _ => {} // Apples to oranges
                                    }
                                }
                                _ => {} // Apples to oranges
                            }
                        }
                        _ => {
                            let label_expr: String = format!("CAST()")
                            
                        }
                    }
                },
                column_type::ColumnType::Object { table_oid: old_table_oid, .. } => {
                    match &self.column_type {
                        column_type::ColumnType::Primitive(prim) => {

                        }
                        column_type::ColumnType::Object { table_oid, .. } => {

                        }
                        column_type::ColumnType::Select { table_oid, .. } => {

                        }
                        column_type::ColumnType::Multiselect { table_oid, .. } => {

                        }
                        _ => {} // No copy necessary
                    }
                },
                column_type::ColumnType::Select { table_oid: old_table_oid, .. } => {
                    match &self.column_type {
                        column_type::ColumnType::Primitive(prim) => {

                        }
                        column_type::ColumnType::Object { table_oid, .. } => {

                        }
                        column_type::ColumnType::Select { table_oid, .. } => {

                        }
                        column_type::ColumnType::Multiselect { table_oid, .. } => {

                        }
                        _ => {} // No copy necessary
                    }
                },
                column_type::ColumnType::Multiselect { table_oid: old_table_oid, .. } => {
                    match &self.column_type {
                        column_type::ColumnType::Primitive(prim) => {

                        }
                        column_type::ColumnType::Object { table_oid, .. } => {

                        }
                        column_type::ColumnType::Select { table_oid, .. } => {

                        }
                        column_type::ColumnType::Multiselect { table_oid, .. } => {

                        }
                        _ => {} // No copy necessary
                    }
                },
                _ => {} // No copy necessary
            }
        }

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}