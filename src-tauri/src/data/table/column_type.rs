use rusqlite::{Connection, params};
use serde::{Serialize, Deserialize};

use crate::util::db::sql_execute;
use crate::util::error::Error;

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
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

impl Primitive {
    /// Gets the string ID of the primitive.
    pub fn id(&self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Date => "Date",
            Self::Datetime => "Datetime",
            Self::Integer => "Integer",
            Self::Number => "Number",
            Self::Text => "Text",
            Self::TextBBCode => "Text/BBCode",
            Self::TextJson => "Text/Json",
            Self::TextMarkdown => "Text/Markdown",
            Self::TextXml => "Text/Xml"
        }
    }

    /// Gets the name of the primitive.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Date => "Date",
            Self::Datetime => "Datetime",
            Self::Integer => "Integer",
            Self::Number => "Number",
            Self::Text => "Plain Text",
            Self::TextBBCode => "BBCode",
            Self::TextJson => "JSON",
            Self::TextMarkdown => "Markdown",
            Self::TextXml => "XML"
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
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

    /// Gets a string ID for the column type.
    /// For use to determine value type of formula's return value.
    pub fn id(&self) -> String {
        match self {
            Self::Primitive { primitive, .. } => String::from(primitive.id()),
            Self::File { .. } => String::from("File"),
            Self::Object { table_oid, .. } => format!("Object/{table_oid}"),
            Self::SingleSelect { table_oid, .. } => format!("SingleSelect/{table_oid}"),
            Self::MultiSelect { table_oid, .. } => format!("MultiSelect/{table_oid}"),
            Self::Subreport { report_oid, .. } => format!("Report/{report_oid}")
        }
    }

    /// Gets the name of the column type.
    /// For displaying to the user (e.g. in the case of errors).
    pub fn name(&self) -> &'static str {
        match self {
            Self::Primitive { primitive, .. } => primitive.name(),
            Self::File { .. } => "File",
            Self::Object { .. } => "Object",
            Self::SingleSelect { .. } => "Single-Select Dropdown",
            Self::MultiSelect { .. } => "Multi-Select Dropdown",
            Self::Subreport { .. } => "Drill-Down Report"
        }
    }
}