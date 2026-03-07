use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::{schema, datasource};
use rusqlite::{Transaction, OptionalExtension, params};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;

/// Data structure representing the table metadata
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all="camelCase")]
pub struct FullMetadata {
    pub schema: schema::FullMetadata
}

impl Hash for FullMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state)
    }
}

impl Borrow<schema::FullMetadata> for FullMetadata {
    fn borrow(&self) -> &schema::FullMetadata {
        &self.schema
    }
}

impl FullMetadata {
    /// Gets the metadata for a table.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Get the schema metadata
        let schema_metadata = schema::FullMetadata::get(&conn, oid)?;

        // Return the metadata
        Ok(Self {
            schema: schema_metadata
        })
    }

    /// Creates a new table.
    pub fn create(&mut self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create the table
        let create_table_cmd: String = format!(
            "
            CREATE TABLE TABLE{} (
                OID INTEGER PRIMARY KEY, 
                TRASH INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            ",
            self.schema.oid
        );
        trans.execute(&create_table_cmd, [])?;

        // Create schema
        self.schema.create(&trans)?;
        // Create the table metadata
        trans.execute("INSERT INTO METADATA_TABLE (OID) VALUES (?1)", params![self.schema.oid])?;
        // Create a datasource for the table
        datasource::Datasource::Table { oid: 0, table: self.clone(), label: self.schema.name.clone() }.find(&trans, Vec::new())?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Overwrites the metadata for the table.
    pub fn set(&self) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Overwrite the schema metadata
        self.schema.set(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Inserts a row into the table.
    /// Optionally, a specific OID for the row can be provided.
    fn _insert_row(&self, trans: &Transaction, row_oid: Option<i64>, master_rows: &mut HashMap<Self, i64>) -> Result<(), Error> {
        if !master_rows.contains_key(self) {
            // Add a related row to every master table
            let mut cols: Vec<(String, String)> = Vec::new();
            for master_schema in self.schema.master_schemas.iter() {
                if let schema::Schema::Table(master_table) = master_schema {
                    master_table._insert_row(trans, None, master_rows)?;
                    cols.push((format!("MASTER{}_OID", master_table.schema.oid), format!("{}", master_rows[master_table])));
                }
            }

            // Add a related row for every non-nullable Object column
            {
                let mut col_query_stmt = trans.prepare(
                    "
                    SELECT c.OID, typ.TABLE_OID 
                    FROM METADATA_COLUMN c
                    INNER JOIN METADATA_COLUMN_TYPE__OBJECT typ ON typ.OID = c.TYPE_OID
                    WHERE c.SCHEMA_OID = ?1 
                        AND NOT c.IS_NULLABLE
                    "
                )?;
                let col_query_rows = col_query_stmt.query_map(params![self.schema.oid], |row| {
                    let column_oid: i64 = row.get("OID")?;
                    let object_schema_oid: i64 = row.get("TABLE_OID")?;
                    Ok::<(String, i64), rusqlite::Error>((format!("COLUMN{column_oid}"), object_schema_oid))
                })?;
                for col_query_row_result in col_query_rows {
                    let (column_name, object_schema_oid) = col_query_row_result?;

                    let object_schema: Self = Self::get(object_schema_oid)?;
                    let mut object_master_rows: HashMap<Self, i64> = HashMap::new();
                    object_schema._insert_row(trans, None, &mut object_master_rows)?;
                    let object_row_oid: i64 = object_master_rows[&object_schema];

                    cols.push((column_name, format!("{object_row_oid}")));
                }
            }

            // Query for any default values that need to be populated
            {
                let mut col_query_stmt = trans.prepare(
                    "
                    SELECT c.OID, c.DEFAULT_VALUE 
                    FROM METADATA_COLUMN c
                    INNER JOIN METADATA_COLUMN_TYPE__PRIMITIVE typ ON typ.OID = c.TYPE_OID
                    WHERE c.SCHEMA_OID = ?1 
                        AND c.DEFAULT_VALUE IS NOT NULL 
                        AND typ.MODE NOT IN ('file', 'image')
                    "
                )?;
                col_query_stmt.query_and_then(params![self.schema.oid], |row| {
                    let column_oid: i64 = row.get("OID")?;
                    let default_value: String = row.get("DEFAULT_VALUE")?;
                    cols.push((format!("COLUMN{column_oid}"), default_value));
                    Ok::<(), rusqlite::Error>(())
                })?;
            }

            // Handle insertion at a specific location in the table
            if let Some(o) = row_oid {
                // Make space for the new row at the designated OID
                let sql_invert_oids: String = format!("UPDATE TABLE{} SET OID = -OID WHERE OID > ?1", self.schema.oid);
                trans.execute(&sql_invert_oids, params![o])?;
                let sql_revert_oids: String = format!("UPDATE TABLE{} SET OID = 1 - OID WHERE OID < 0", self.schema.oid);
                trans.execute(&sql_revert_oids, [])?;

                // Add initial value for the OID
                cols.push((String::from("OID"), format!("{o}")));
            }

            // Compile the INSERT statement and execute
            let sql_insert_row_params: Vec<String> = cols.iter().map(|(_, column_value)| column_value.clone()).collect();
            let sql_insert_row: String = format!("INSERT INTO TABLE{} {}",
                self.schema.oid,
                if cols.len() == 0 {
                    String::from("DEFAULT VALUES")
                } else {
                    let (column_names, column_params) = cols.into_iter().enumerate().fold(
                        (String::from(""), String::from("")), 
                        |(acc_column_names, acc_column_params), (e_idx, (e_column_name, _))| (
                            if acc_column_names == "" { e_column_name } else { format!("{acc_column_names}, {e_column_name}") }, 
                            if acc_column_params == "" { format!("?{e_idx}") } else { format!("{acc_column_params}, ?{e_idx}") }
                        )
                    );
                    format!("({column_names}) VALUES ({column_params})")
                }
            );
            trans.execute(&sql_insert_row, rusqlite::params_from_iter(sql_insert_row_params.into_iter()))?;

            // Get the OID and add to the HashMap of master tables
            master_rows.insert(self.clone(), trans.last_insert_rowid());
        }
        Ok(())
    }

    /// Inserts a row into the table.
    /// Optionally, a specific OID for the new row can be provided.
    /// Returns the OID of the new row.
    pub fn insert_row(&self, row_oid: Option<i64>) -> Result<i64, Error> {
        // Start a transaction
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;

        // Insert the row into the table, + related rows for each master table
        let mut master_rows: HashMap<Self, i64> = HashMap::new();
        self._insert_row(&trans, row_oid, &mut master_rows)?;

        // Commit the transaction
        trans.commit()?;
        Ok(master_rows[self])
    }
}
