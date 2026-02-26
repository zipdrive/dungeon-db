use crate::{data::column, util::error::Error};
use crate::util::db;
use crate::data::table;
use rusqlite::OptionalExtension;
use rusqlite::types::Value;
use rusqlite::{Transaction, params, vtab::array::Array};
use std::hash::{Hash, Hasher};

pub enum Datasource {
    Table {
        oid: i64,
        table: table::Metadata
    },
    Inheritance {
        oid: i64,
        parent_datasource: Box<Datasource>,
        table: table::Metadata
    },
    Object {
        oid: i64,
        parent_datasource: Box<Datasource>,
        column: column::Metadata 
    },
    Select {
        oid: i64,
        parent_datasource: Box<Datasource>,
        column: column::Metadata 
    },
    Multiselect {
        oid: i64,
        parent_datasource: Box<Datasource>,
        column: column::Metadata 
    }
}

impl Hash for Datasource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_oid().hash(state)
    }
}

impl Borrow<i64> for Datasource {
    fn borrow(&self) -> &i64 {
        match self {
            Self::Table { oid, .. }
            | Self::Inheritance { oid, .. } 
            | Self::Object { oid, .. }
            | Self::Select { oid, .. }
            | Self::Multiselect { oid, .. } => oid
        }
    }
}

impl Datasource {
    /// Retrieve a datasource by OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        let (mode, parent_datasource_oid, table_oid, column_oid) = conn.query_one(
            "
            SELECT
                'table' AS MODE,
                NULL AS PARENT_DATASOURCE_OID,
                TABLE_OID,
                NULL AS COLUMN_OID
            FROM METADATA_DATASOURCE__TABLE
            WHERE OID = ?1

            UNION

            SELECT
                'inheritance' AS MODE,
                PARENT_DATASOURCE_OID,
                TABLE_OID,
                NULL AS COLUMN_OID
            FROM METADATA_DATASOURCE__INHERITANCE
            WHERE OID = ?1

            UNION

            SELECT
                'object' AS MODE,
                PARENT_DATASOURCE_OID,
                NULL AS TABLE_OID,
                COLUMN_OID
            FROM METADATA_DATASOURCE__OBJECT
            WHERE OID = ?1

            UNION

            SELECT
                'select' AS MODE,
                PARENT_DATASOURCE_OID,
                NULL AS TABLE_OID,
                COLUMN_OID
            FROM METADATA_DATASOURCE__SELECT
            WHERE OID = ?1

            UNION

            SELECT
                'multiselect' AS MODE,
                PARENT_DATASOURCE_OID,
                NULL AS TABLE_OID,
                COLUMN_OID
            FROM METADATA_DATASOURCE__MULTISELECT
            WHERE OID = ?1
            ",
            params![oid],
            |row| { Ok((
                row.get::<_, String>("MODE")?,
                row.get::<_, Option<i64>>("PARENT_DATASOURCE_OID")?,
                row.get::<_, Option<i64>>("TABLE_OID")?,
                row.get::<_, Option<i64>>("COLUMN_OID")?
            )) }
        )?;

        if mode == "table" {
            Ok(Self::Table {
                oid,
                table: table::Metadata::get(table_oid.expect("TABLE_OID should not be NULL if datasource is a table!"))?
            })
        } else if mode == "inheritance" {
            Ok(Self::Inheritance {
                oid,
                parent_datasource: Box::from(Self::get(parent_datasource_oid.expect("PARENT_DATASOURCE_OID should not be NULL if datasource is an inheritance relationship!"))?),
                table: table::Metadata::get(table_oid.expect("TABLE_OID should not be NULL if datasource is an inheritance relationship!"))?
            })
        } else if mode == "object" {
            Ok(Self::Object { 
                oid, 
                parent_datasource: Box::from(Self::get(parent_datasource_oid.expect("PARENT_DATASOURCE_OID should not be NULL if datasource is an Object column!"))?), 
                column: column::Metadata::get(column_oid.expect("COLUMN_OID should not be NULL if datasource is an Object column!"))?
            })
        } else if mode == "select" {
            Ok(Self::Select { 
                oid, 
                parent_datasource: Box::from(Self::get(parent_datasource_oid.expect("PARENT_DATASOURCE_OID should not be NULL if datasource is a Select column!"))?), 
                column: column::Metadata::get(column_oid.expect("COLUMN_OID should not be NULL if datasource is a Select column!"))?
            })
        } else if mode == "multiselect" {
            Ok(Self::Multiselect { 
                oid, 
                parent_datasource: Box::from(Self::get(parent_datasource_oid.expect("PARENT_DATASOURCE_OID should not be NULL if datasource is a Multiselect column!"))?), 
                column: column::Metadata::get(column_oid.expect("COLUMN_OID should not be NULL if datasource is a Multiselect column!"))?
            })
        } else {
            Err(Error::AdhocError("Unknown datasource type."))
        }
    }

    /// Finds an existing datasource OID, or creates a new datasource if one does not exist in the database 
    /// (excluding datasources whose root is one of the ones specified).
    pub fn find(self, trans: &Transaction, exclude_root_datasources: Vec<i64>) -> Result<Self, Error> {
        match self {
            Self::Table { table, .. } => {
                // Make sure there isn't an existing datasource that can be reused
                match trans.query_row(
                    "
                    SELECT OID FROM METADATA_DATASOURCE__TABLE
                    WHERE TABLE_OID = ?1 AND OID NOT IN ?2
                    ", 
                    params![table.schema.oid, Array::new(exclude_root_datasources.into_iter().map(|o| Value::Integer(o)).collect())],
                    |row| row.get::<_, i64>("OID") 
                ).optional()? {
                    Some(oid) => {
                        // Return the already-existing datasource
                        Ok(Self::Table { oid, table })
                    },
                    None => {
                        // If no datasource exists, or all existing datasources are excluded, create a new datasource
                        trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();
                        trans.execute("INSERT INTO METADATA_DATASOURCE__TABLE (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table.schema.oid])?;
                        Ok(Self::Table { oid, table })
                    }
                }
            }
            Self::Inheritance { parent_datasource, table, .. } => {
                let parent_datasource: Self = parent_datasource.find(trans, exclude_root_datasources)?;
                
                // Make sure there isn't an existing datasource that can be reused
                match trans.query_row(
                    "
                    SELECT OID FROM METADATA_DATASOURCE__INHERITANCE
                    WHERE TABLE_OID = ?1 AND PARENT_DATASOURCE_OID = ?2
                    ", 
                    params![table.schema.oid, parent_datasource.get_oid()],
                    |row| row.get::<_, i64>("OID") 
                ).optional()? {
                    Some(oid) => {
                        // Return the already-existing datasource
                        Ok(Self::Inheritance { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            table 
                        })
                    },
                    None => {
                        // If no datasource exists, or all existing datasources are excluded, create a new datasource
                        trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();
                        trans.execute(
                            "
                            INSERT INTO METADATA_DATASOURCE__INHERITANCE (OID, PARENT_DATASOURCE_OID, TABLE_OID) 
                            VALUES (?1, ?2, ?3)
                            ", 
                            params![oid, parent_datasource.get_oid(), table.schema.oid]
                        )?;
                        Ok(Self::Inheritance { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            table 
                        })
                    }
                }
            }
            Self::Object { parent_datasource, column, .. } => {
                let parent_datasource: Self = parent_datasource.find(trans, exclude_root_datasources)?;
                
                // Make sure there isn't an existing datasource that can be reused
                match trans.query_row(
                    "
                    SELECT OID FROM METADATA_DATASOURCE__OBJECT
                    WHERE COLUMN_OID = ?1 AND PARENT_DATASOURCE_OID = ?2
                    ", 
                    params![column.oid, parent_datasource.get_oid()],
                    |row| row.get::<_, i64>("OID") 
                ).optional()? {
                    Some(oid) => {
                        // Return the already-existing datasource
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    },
                    None => {
                        // If no datasource exists, or all existing datasources are excluded, create a new datasource
                        trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();
                        trans.execute(
                            "
                            INSERT INTO METADATA_DATASOURCE__OBJECT (OID, PARENT_DATASOURCE_OID, COLUMN_OID) 
                            VALUES (?1, ?2, ?3)
                            ", 
                            params![oid, parent_datasource.get_oid(), column.oid]
                        )?;
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    }
                }
            }
            Self::Select { parent_datasource, column, .. } => {
                let parent_datasource: Self = parent_datasource.find(trans, exclude_root_datasources)?;
                
                // Make sure there isn't an existing datasource that can be reused
                match trans.query_row(
                    "
                    SELECT OID FROM METADATA_DATASOURCE__SELECT
                    WHERE COLUMN_OID = ?1 AND PARENT_DATASOURCE_OID = ?2
                    ", 
                    params![column.oid, parent_datasource.get_oid()],
                    |row| row.get::<_, i64>("OID") 
                ).optional()? {
                    Some(oid) => {
                        // Return the already-existing datasource
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    },
                    None => {
                        // If no datasource exists, or all existing datasources are excluded, create a new datasource
                        trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();
                        trans.execute(
                            "
                            INSERT INTO METADATA_DATASOURCE__SELECT (OID, PARENT_DATASOURCE_OID, COLUMN_OID) 
                            VALUES (?1, ?2, ?3)
                            ", 
                            params![oid, parent_datasource.get_oid(), column.oid]
                        )?;
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    }
                }
            }
            Self::Multiselect { parent_datasource, column, .. } => {
                let parent_datasource: Self = parent_datasource.find(trans, exclude_root_datasources)?;
                
                // Make sure there isn't an existing datasource that can be reused
                match trans.query_row(
                    "
                    SELECT OID FROM METADATA_DATASOURCE__MULTISELECT
                    WHERE COLUMN_OID = ?1 AND PARENT_DATASOURCE_OID = ?2
                    ", 
                    params![column.oid, parent_datasource.get_oid()],
                    |row| row.get::<_, i64>("OID") 
                ).optional()? {
                    Some(oid) => {
                        // Return the already-existing datasource
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    },
                    None => {
                        // If no datasource exists, or all existing datasources are excluded, create a new datasource
                        trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();
                        trans.execute(
                            "
                            INSERT INTO METADATA_DATASOURCE__MULTISELECT (OID, PARENT_DATASOURCE_OID, COLUMN_OID) 
                            VALUES (?1, ?2, ?3)
                            ", 
                            params![oid, parent_datasource.get_oid(), column.oid]
                        )?;
                        Ok(Self::Object { 
                            oid, 
                            parent_datasource: Box::from(parent_datasource),
                            column 
                        })
                    }
                }
            }
        }
    }

    /// Gets the OID of the datasource.
    pub fn get_oid(&self) -> i64 {
        match self {
            Self::Table { oid, .. }
            | Self::Inheritance { oid, .. } 
            | Self::Object { oid, .. }
            | Self::Select { oid, .. }
            | Self::Multiselect { oid, .. } => *oid
        }
    }

    /// Gets the OID of the root datasource.
    pub fn get_root_datasource_oid(&self) -> i64 {
        match self {
            Self::Table { oid, .. } => oid.clone(),
            Self::Inheritance { parent_datasource, .. }
            | Self::Object { parent_datasource, .. }
            | Self::Select { parent_datasource, .. }
            | Self::Multiselect { parent_datasource, .. } => parent_datasource.get_root_datasource_oid()
        }
    }

    /// Gets the metadata for the schema of the datasource.
    pub fn get_schema(&self) -> schema::Metadata {
        match self {
            Self::Table { table, .. }
            | Self::Inheritance { table, .. } => table.schema.clone(),
            Self::Object { column, .. }
            | Self::Select { column, .. }
            | Self::Multiselect { column, .. } => column.schema.clone()
        }
    }
}

/// Creates a new datasource.
pub fn create(trans: &Transaction) -> Result<i64, Error> {
    // Create datasource
    trans.execute("INSERT INTO METADATA_DATASOURCE DEFAULT VALUES", [])?;
    Ok(trans.last_insert_rowid())
}