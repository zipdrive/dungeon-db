use crate::util::db;
use crate::util::error::Error;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Primitive {
    PlainText,
    Integer,
    Number,
    Boolean,
    Date,
    Datetime,
    File,
    Image,
    JsonText,
    MarkdownText,
    XmlText
}

impl Primitive {
    /// Gets the OID of the primitive type.
    fn get_oid(&self) -> i64 {
        match self {
            Self::PlainText => -1,
            Self::Integer => -2,
            Self::Number => -3,
            Self::Boolean => -4,
            Self::Date => -5,
            Self::Datetime => -6,
            Self::File => -7,
            Self::Image => -8,
            Self::JsonText => -9,
            Self::MarkdownText => -10,
            Self::XmlText => -11
        }
    }

    /// Returns a static str representing the column type.
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Boolean => "Boolean",
            Self::Date => "Date",
            Self::Datetime => "Datetime",
            Self::File => "File",
            Self::Image => "Image",
            Self::Integer => "Integer",
            Self::JsonText => "TextJson",
            Self::Number => "Number",
            Self::PlainText => "TextPlain",
            Self::MarkdownText => "TextMarkdown",
            Self::XmlText => "TextXml"
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ColumnType {
    Formula { oid: i64, formula: String },
    Subreport { oid: i64, report_oid: i64 },
    Primitive(Primitive),
    Object { oid: i64, table_oid: i64 },
    Select { oid: i64, table_oid: i64 },
    Multiselect { oid: i64, table_oid: i64 },
}

impl ColumnType {
    /// Gets the column type metadata from its OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::get_transact(&conn, oid)
    }

    pub fn get_transact(conn: &Connection, oid: i64) -> Result<Self, Error> {
        Ok(conn.query_one(
            "
            SELECT
                MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__PRIMITIVE
            WHERE OID = ?1

            UNION ALL

            SELECT 
                'formula' AS MODE,
                FORMULA,
                NULL AS REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__FORMULA
            WHERE OID = ?1

            UNION ALL

            SELECT 
                'subreport' AS MODE,
                NULL AS FORMULA,
                REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__SUBREPORT
            WHERE OID = ?1

            UNION ALL

            SELECT 
                'object' AS MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                TABLE_OID
            FROM METADATA_COLUMN_TYPE__OBJECT
            WHERE OID = ?1

            UNION ALL

            SELECT 
                'select' AS MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                TABLE_OID
            FROM METADATA_COLUMN_TYPE__SELECT
            WHERE OID = ?1

            UNION ALL

            SELECT 
                'multiselect' AS MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                TABLE_OID
            FROM METADATA_COLUMN_TYPE__MULTISELECT
            WHERE OID = ?1
            ",
            params![oid],
            |row| {
                let mode: String = row.get("MODE")?;
                if mode == "formula" {
                    Ok(Self::Formula {
                        oid,
                        formula: row.get("FORMULA")?,
                    })
                } else if mode == "subreport" {
                    Ok(Self::Subreport {
                        oid,
                        report_oid: row.get("REPORT_OID")?,
                    })
                } else if mode == "object" {
                    Ok(Self::Object {
                        oid,
                        table_oid: row.get("TABLE_OID")?,
                    })
                } else if mode == "select" {
                    Ok(Self::Select {
                        oid,
                        table_oid: row.get("TABLE_OID")?,
                    })
                } else if mode == "multiselect" {
                    Ok(Self::Multiselect {
                        oid,
                        table_oid: row.get("TABLE_OID")?,
                    })
                } else if mode == "integer" {
                    Ok(Self::Primitive(Primitive::Integer))
                } else if mode == "number" {
                    Ok(Self::Primitive(Primitive::Number))
                } else if mode == "checkbox" {
                    Ok(Self::Primitive(Primitive::Boolean))
                } else if mode == "date" {
                    Ok(Self::Primitive(Primitive::Date))
                } else if mode == "datetime" {
                    Ok(Self::Primitive(Primitive::Datetime))
                } else if mode == "file" {
                    Ok(Self::Primitive(Primitive::File))
                } else if mode == "image" {
                    Ok(Self::Primitive(Primitive::Image))
                } else if mode == "JSON" {
                    Ok(Self::Primitive(Primitive::JsonText))
                } else {
                    Ok(Self::Primitive(Primitive::PlainText))
                }
            },
        )?)
    }

    /// Finds or creates the OID of the column type, as part of a transaction.
    pub fn find_transact(self, trans: &Transaction) -> Result<Self, Error> {
        match self {
            Self::Formula { formula, .. } => {
                // Create the column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                let oid: i64 = trans.last_insert_rowid();

                // Create the formula column type metadata
                trans.execute(
                    "INSERT INTO METADATA_COLUMN_TYPE__FORMULA (OID, FORMULA) VALUES (?1, ?2)",
                    params![oid, formula],
                )?;

                return Ok(Self::Formula { oid, formula });
            }
            Self::Subreport { report_oid, .. } => {
                match trans
                    .query_one(
                        "SELECT OID FROM METADATA_COLUMN_TYPE__SUBREPORT WHERE REPORT_OID = ?1",
                        params![report_oid],
                        |row| row.get(0),
                    )
                    .optional()?
                {
                    Some(oid) => {
                        return Ok(Self::Subreport { oid, report_oid });
                    }
                    None => {
                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the subreport column type metadata
                        trans.execute(
                            "INSERT INTO METADATA_COLUMN_TYPE__SUBREPORT (OID, REPORT_OID) VALUES (?1, ?2)", 
                            params![oid, report_oid]
                        )?;

                        return Ok(Self::Subreport { oid, report_oid });
                    }
                }
            }
            Self::Primitive(_) => {
                return Ok(self);
            }
            Self::Object { table_oid, .. } => {
                match trans
                    .query_one(
                        "SELECT OID FROM METADATA_COLUMN_TYPE__OBJECT WHERE TABLE_OID = ?1",
                        params![table_oid],
                        |row| row.get(0),
                    )
                    .optional()?
                {
                    Some(oid) => {
                        return Ok(Self::Object { oid, table_oid });
                    }
                    None => {
                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the object column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__OBJECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        return Ok(Self::Object { oid, table_oid });
                    }
                }
            }
            Self::Select { table_oid, .. } => {
                match trans
                    .query_one(
                        "SELECT OID FROM METADATA_COLUMN_TYPE__SELECT WHERE TABLE_OID = ?1",
                        params![table_oid],
                        |row| row.get(0),
                    )
                    .optional()?
                {
                    Some(oid) => {
                        return Ok(Self::Select { oid, table_oid });
                    }
                    None => {
                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the select column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__SELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        return Ok(Self::Select { oid, table_oid });
                    }
                }
            }
            Self::Multiselect { table_oid, .. } => {
                // Always create a new Multiselect column type

                // Create the column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                let oid: i64 = trans.last_insert_rowid();

                // Create the multiselect column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE__MULTISELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                return Ok(Self::Multiselect { oid, table_oid });
            }
        }
    }

    /// Find or create the column type matching the metadata.
    pub fn find(self) -> Result<Self, Error> {
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;
        self.find_transact(&trans)
    }

    /// Get the OID associated with the column type.
    pub fn get_oid(&self) -> i64 {
        match self {
            Self::Primitive(prim) => prim.get_oid(),
            Self::Formula { oid, .. } => oid.clone(),
            Self::Subreport { oid, .. } => oid.clone(),
            Self::Object { oid, .. } => oid.clone(),
            Self::Select { oid, .. } => oid.clone(),
            Self::Multiselect { oid, .. } => oid.clone(),
        }
    }

    /// Return a static str representing the column type.
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Primitive(prim) => prim.to_str(),
            Self::Object { .. } => "Object",
            Self::Select { .. } => "Select",
            Self::Multiselect { .. } => "Multiselect",
            Self::Formula { .. } => "Formula",
            Self::Subreport { .. } => "Subreport",
        }
    }

    /// Clean up unusued types.
    pub fn clean() -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Remove all unused formulas
        trans.execute_batch(
            "
            DELETE FROM METADATA_COLUMN_TYPE
            WHERE OID NOT IN (SELECT DISTINCT TYPE_OID FROM METADATA_COLUMN);
            ",
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}
