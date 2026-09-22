use rusqlite::{Connection, params};
use serde::{Serialize, Deserialize};

use crate::util::db::sql_execute;
use crate::util::error::Error;

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Primitive {
    Text,
    TextJson,
    TextXml,
    TextMarkdown,
    TextBBCode,
    Boolean,
    Integer,
    Number,
    Date,
    Datetime
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum TableColumnType {
    Primitive { 
        oid: i64, 
        primitive: Primitive, 
        default_value: Option<String>
    },
    File {
        oid: i64
    },
    Object { 
        oid: i64, 
        table_oid: i64 
    },
    SingleSelect { 
        oid: i64, 
        table_oid: i64 
    },
    MultiSelect { 
        oid: i64, 
        table_oid: i64 
    },
    Subreport { 
        oid: i64, 
        report_oid: i64 
    },
}

impl TableColumnType {
    /// Retrieves the column type OID.
    pub fn oid(&self) -> &i64 {
        match self {
            Self::Primitive { oid, .. }
            | Self::File { oid }
            | Self::Object { oid, .. }
            | Self::SingleSelect { oid, .. }
            | Self::MultiSelect { oid, .. }
            | Self::Subreport { oid, .. } => oid
        }
    }

    /// Inserts the column type into the database.
    pub fn create(&mut self, conn: &Connection) -> Result<(), Error> {
        sql_execute(conn, "INSERT INTO __METADATA_TABLE_COLUMNTYPE DEFAULT VALUES", [])?;
        let inserted_oid: i64 = conn.last_insert_rowid();
        
        match self {
            Self::Primitive { oid, primitive, default_value } => {
                *oid = inserted_oid;
                let mode: &'static str = match *primitive {
                    Primitive::Text => "Text",
                    Primitive::TextJson => "Text/Json",
                    Primitive::TextXml => "Text/Xml",
                    Primitive::TextMarkdown => "Text/Markdown",
                    Primitive::TextBBCode => "Text/BBCode",
                    Primitive::Boolean => "Boolean",
                    Primitive::Integer => "Integer",
                    Primitive::Number => "Number",
                    Primitive::Date => "Date",
                    Primitive::Datetime => "Datetime"
                };
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_TABLE_COLUMNTYPE_PRIMITIVE (OID, MODE, DEFAULT_VALUE) VALUES (?1, ?2, ?3)", 
                    params![*oid, mode, *default_value]
                )?;
            }
            Self::File { oid } => {
                *oid = inserted_oid;
            }
            Self::Object { oid, table_oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_TABLE_COLUMNTYPE_OBJECT (OID, TABLE_OID) VALUES (?1, ?2)", 
                    params![*oid, *table_oid]
                )?;
            }
            Self::SingleSelect { oid, table_oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_TABLE_COLUMNTYPE_SINGLESELECT (OID, TABLE_OID) VALUES (?1, ?2)", 
                    params![*oid, *table_oid]
                )?;
            }
            Self::MultiSelect { oid, table_oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_TABLE_COLUMNTYPE_MULTISELECT (OID, TABLE_OID) VALUES (?1, ?2)", 
                    params![*oid, *table_oid]
                )?;
            }
            Self::Subreport { oid, report_oid } => {
                *oid = inserted_oid;
                sql_execute(
                    conn, 
                    "INSERT INTO __METADATA_TABLE_COLUMNTYPE_SUBREPORT (OID, REPORT_OID) VALUES (?1, ?2)", 
                    params![*oid, *report_oid]
                )?;
            }
        }
        Ok(())
    }
}