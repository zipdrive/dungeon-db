use rusqlite::{Connection, params};
use serde::{Serialize, Deserialize};

use crate::util::db::sql_execute;
use crate::util::error::Error;

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ReportColumnType {
    Formula { 
        oid: i64
    },
    Subreport { 
        oid: i64, 
        report_oid: i64 
    },
}

impl ReportColumnType {
    /// Retrieves the column type OID.
    pub fn oid(&self) -> &i64 {
        match self {
            Self::Formula { oid, .. }
            | Self::Subreport { oid, .. } => oid
        }
    }

    /// Inserts the column type into the database.
    pub fn create(&mut self, conn: &Connection) -> Result<(), Error> {
        sql_execute(conn, "INSERT INTO __METADATA_REPORT_COLUMNTYPE DEFAULT VALUES", [])?;
        let inserted_oid: i64 = conn.last_insert_rowid();
        
        match self {
            Self::Formula { oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_REPORT_COLUMNTYPE_FORMULA (OID, FORMULA) VALUES (?1, ?2)", 
                    params![*oid, todo!("formula")]
                )?;
            }
            Self::Subreport { oid, report_oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_REPORT_COLUMNTYPE_SUBREPORT (OID, REPORT_OID) VALUES (?1, ?2)", 
                    params![*oid, *report_oid]
                )?;
            }
        }
        Ok(())
    }
}