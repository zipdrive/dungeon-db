use std::collections::HashMap;
use rusqlite::{params, Row};
use serde::Serialize;
use crate::backend::db;
use crate::util::error;

#[derive(Serialize)]
pub struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    //data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>
}

impl Table {
    /// Creates a new table.
    pub fn create(name: String) -> Result<i64, error::Error> {
        let action = db::begin_db_action()?;
        action.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (3);", [])?;
        let table_id: i64 = action.trans.last_insert_rowid();
        action.execute(
            "INSERT INTO METADATA_TABLE (OID, NAME) VALUES (?1, ?2);",
            params![table_id, &name]
        )?;
        let create_cmd: String = format!("CREATE TABLE TABLE{} (OID INTEGER PRIMARY KEY);", table_id);
        action.execute(&create_cmd, [])?;
        return Ok(table_id);
    }

    // Deletes the table and all associated local columns.
    pub fn delete(oid: i64) -> Result<(), error::Error> {
        let action = db::begin_db_action()?;

        // Drop data from the table
        let drop_cmd: String = format!("DROP TABLE TABLE{};", oid);
        action.execute(&drop_cmd, [])?;

        // Drop tables associated locally with the table


        // Finally, drop the table's metadata
        action.execute("DELETE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = ?1;", [oid])?;
        return Ok(());
    }
    
    /*
    fn get(oid: i64) -> Result<Self, error::Error> {
        let action = db::begin_readonly_db_action()?;
        return action.trans.query_one<Table>(
            "SELECT OID, PARENT_TABLE_OID, NAME, SURROGATE_KEY_COLUMN_OID FROM METADATA_TABLE WHERE OID = ?1;",
            params![oid],
            |row: &Row<'_>| -> Result<Self, error::Error> {
                return Ok(Self {
                    oid: row.get<usize, i64>(0),
                    parent_table_oid: row.get<usize, i64>(1),
                    name: row.get<usize, &str>(2),
                    surrogate_key_column_oid: row.get<usize, i64>(3)
                });
            }
        );
    }
    */
}