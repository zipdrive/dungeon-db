use crate::util::error::Error;
use crate::util::db;
use rusqlite::{params};
use serde::Serialize;

#[derive(Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Primitive {
    Text,
    Integer,
    Number,
    Checkbox,
    Date,
    Datetime,
    File,
    Image,
    JSON
}

impl Primitive {
    /// Gets the OID of the primitive type.
    fn get_oid(&self) -> i64 {
        match self {
            Self::Text => -1,
            Self::Integer => -2,
            Self::Number => -3,
            Self::Checkbox => -4,
            Self::Date => -5,
            Self::Datetime => -6,
            Self::File => -7,
            Self::Image => -8,
            Self::JSON => -9
        }
    }
}

#[derive(Serialize, Clone, PartialEq, Eq)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum ColumnType {
    Formula {
        oid: i64,
        formula: String
    },
    Subreport {
        oid: i64,
        report_oid: i64
    },
    Primitive(Primitive),
    Object {
        oid: i64,
        table_oid: i64
    },
    Select {
        oid: i64,
        table_oid: i64 
    },
    Multiselect {
        oid: i64,
        table_oid: i64
    }
}

impl ColumnType {
    /// Gets the column type metadata from its OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Ok(conn.query_one(
            "
            SELECT
                MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__PRIMITIVE
            WHERE OID = ?1

            UNION

            SELECT 
                'formula' AS MODE,
                FORMULA,
                NULL AS REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__FORMULA
            WHERE OID = ?1

            UNION

            SELECT 
                'subreport' AS MODE,
                NULL AS FORMULA,
                REPORT_OID,
                NULL AS TABLE_OID
            FROM METADATA_COLUMN_TYPE__SUBREPORT
            WHERE OID = ?1

            UNION

            SELECT 
                'object' AS MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                TABLE_OID
            FROM METADATA_COLUMN_TYPE__OBJECT
            WHERE OID = ?1

            UNION

            SELECT 
                'select' AS MODE,
                NULL AS FORMULA,
                NULL AS REPORT_OID,
                TABLE_OID
            FROM METADATA_COLUMN_TYPE__SELECT
            WHERE OID = ?1

            UNION

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
                    Ok(Self::Formula { oid, formula: row.get("FORMULA")? })
                } else if mode == "subreport" {
                    Ok(Self::Subreport { oid, report_oid: row.get("REPORT_OID")? })
                } else if mode == "object" {
                    Ok(Self::Object { oid, table_oid: row.get("TABLE_OID")? })
                } else if mode == "select" {
                    Ok(Self::Select { oid, table_oid: row.get("TABLE_OID")? })
                } else if mode == "multiselect" {
                    Ok(Self::Multiselect { oid, table_oid: row.get("TABLE_OID")? })
                } else if mode == "integer" {
                    Ok(Self::Primitive(Primitive::Integer))
                } else if mode == "number" {
                    Ok(Self::Primitive(Primitive::Number))
                } else if mode == "checkbox" {
                    Ok(Self::Primitive(Primitive::Checkbox))
                } else if mode == "date" {
                    Ok(Self::Primitive(Primitive::Date))
                } else if mode == "datetime" {
                    Ok(Self::Primitive(Primitive::Datetime))
                } else if mode == "file" {
                    Ok(Self::Primitive(Primitive::File))
                } else if mode == "image" {
                    Ok(Self::Primitive(Primitive::Image))
                } else if mode == "JSON" {
                    Ok(Self::Primitive(Primitive::JSON))
                } else {
                    Ok(Self::Primitive(Primitive::Text))
                }
            }
        )?)
    }

    /// Find the column type matching the metadata.
    pub fn find(self) -> Result<Self, Error> {
        let mut conn = db::open()?;

        match self {
            Self::Formula { formula, .. } => {
                let trans = conn.transaction()?;

                // Create the column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                let oid: i64 = trans.last_insert_rowid();

                // Create the formula column type metadata
                trans.execute(
                    "INSERT INTO METADATA_COLUMN_TYPE__FORMULA (OID, FORMULA) VALUES (?1, ?2)", 
                    params![oid, formula]
                )?;

                // Create a view for the formula, in case the formula is ever used as a parameter
                let formula_query = String::from("");
                let cmd_formula_view: String = format!("CREATE VIEW FORMULA{oid} AS ({formula_query})");
                trans.execute(&cmd_formula_view, [])?;

                // Commit the transaction
                trans.commit()?;
                return Ok(Self::Formula {
                    oid,
                    formula
                });
            }
            Self::Subreport { report_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__SUBREPORT WHERE REPORT_OID = ?1",
                    params![report_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Subreport {
                            oid,
                            report_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the subreport column type metadata
                        trans.execute(
                            "INSERT INTO METADATA_COLUMN_TYPE__SUBREPORT (OID, REPORT_OID) VALUES (?1, ?2)", 
                            params![oid, report_oid]
                        )?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Subreport {
                            oid,
                            report_oid
                        });
                    }
                }
            }
            Self::Primitive(_) => {
                return Ok(self);
            }
            Self::Object { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__OBJECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Object {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the object column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__OBJECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Object {
                            oid,
                            table_oid
                        });
                    }
                }
            }
            Self::Select { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__SELECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the select column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__SELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                }
            }
            Self::Multiselect { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__MULTISELECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Multiselect {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the multiselect column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__MULTISELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                }
            }
        }
    }

    /// Get the OID associated with the column type.
    pub fn get_oid(&self) -> i64 {
        match self {
            Self::Primitive(prim) => prim.get_oid(),
            Self::Formula { oid, .. } => oid.clone(),
            Self::Subreport { oid, .. } => oid.clone(),
            Self::Object { oid, .. } => oid.clone(),
            Self::Select { oid, .. } => oid.clone(),
            Self::Multiselect { oid, .. } => oid.clone()
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
            "
        )?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }
}