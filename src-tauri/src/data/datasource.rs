use crate::util::error::Error;
use crate::util::db;
use crate::data::{column, column_type, schema, table};
use regex::Regex;
use rusqlite::OptionalExtension;
use rusqlite::types::Value;
use rusqlite::{Transaction, params, vtab::array::Array};
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::rc::Rc;



#[derive(PartialEq, Eq, Clone)]
pub enum Relationship {
    One,
    Many
}



#[derive(PartialEq, Eq, Clone)]
pub enum Datasource {
    Table {
        oid: i64,
        table_oid: i64,
        label: String
    },
    MasterTable {
        parent_datasource: Box<Datasource>,
        table_oid: i64 
    },
    InheritorTable {
        parent_datasource: Box<Datasource>,
        table_oid: i64 
    },
    Column {
        parent_datasource: Box<Datasource>,
        column: column::FullMetadata
    }
}

impl Hash for Datasource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_alias().hash(state)
    }
}

impl Datasource {
    fn from_parent_and_path(parent: Self, path: &[String]) -> Result<Self, Error> {
        if path.len() == 0 {
            return Ok(parent);
        } else {
            let next_component: &String = &path[0];

            let master_regex: Regex = Regex::new(r#"^MASTER(\d+)$"#).unwrap();
            if let Some(master_caps) = master_regex.captures(next_component) {
                let (_, [master_table_oid_str]) = master_caps.extract();
                return Self::from_parent_and_path(Self::MasterTable { 
                    parent_datasource: Box::new(parent), 
                    table_oid: master_table_oid_str.parse::<i64>().unwrap()
                }, &path[1..]);
            }

            let inheritor_regex: Regex = Regex::new(r#"^INHERITOR(\d+)$"#).unwrap();
            if let Some(inheritor_caps) = inheritor_regex.captures(next_component) {
                let (_, [inheritor_table_oid_str]) = inheritor_caps.extract();
                return Self::from_parent_and_path(Self::InheritorTable { 
                    parent_datasource: Box::new(parent), 
                    table_oid: inheritor_table_oid_str.parse::<i64>().unwrap()
                }, &path[1..]);
            }

            let column_regex: Regex = Regex::new(r#"^COLUMN(\d+)$"#).unwrap();
            if let Some(column_caps) = column_regex.captures(next_component) {
                let (_, [column_oid_str]) = column_caps.extract();
                let column_oid: i64 = column_oid_str.parse::<i64>().unwrap();
                let column: column::FullMetadata = column::FullMetadata::get(column_oid)?;
                return Self::from_parent_and_path(Self::Column { 
                    parent_datasource: Box::new(parent), 
                    column
                }, &path[1..]);
            }

            return Err(Error::AdhocError("Datasource path contains an unknown link type."));
        }
    }

    pub fn from_path(path: Vec<String>) -> Result<Self, Error> {
        if path.len() == 0 {
            return Err(Error::AdhocError("Datasource cannot be empty!"));
        }
        let Ok(root_datasource_oid) = path[0].parse::<i64>() else {
            return Err(Error::AdhocError("Root datasource is expected to be an OID of a row in METADATA_DATASOURCE."));
        };
        let root: Self = Self::get(root_datasource_oid)?;
        Self::from_parent_and_path(root, &path[1..])
    }

    /// Retrieve a root datasource by OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        let (table_oid, label) = conn.query_one(
            "
            SELECT
                d.TABLE_OID,
                COALESCE(d.LABEL, s.NAME) AS LABEL
            FROM METADATA_DATASOURCE d
            INNER JOIN METADATA_SCHEMA s ON s.OID = d.TABLE_OID
            WHERE d.OID = ?1
            ",
            params![oid],
            |row| { Ok((
                row.get::<_, i64>("TABLE_OID")?,
                row.get::<_, String>("LABEL")?
            )) }
        )?;

        Ok(Self::Table {
            oid,
            table_oid,
            label
        })
    }

    /// Gets the alias of the datasource.
    pub fn get_alias(&self) -> String {
        match self {
            Self::Table { oid, .. } => format!("d{oid}"),
            Self::MasterTable { parent_datasource, table_oid } => format!("{}_MASTER{table_oid}", parent_datasource.get_alias()),
            Self::InheritorTable { parent_datasource, table_oid } => format!("{}_INHERITOR{table_oid}", parent_datasource.get_alias()),
            Self::Column { parent_datasource, column } => format!("{}_COLUMN{}", parent_datasource.get_alias(), column.oid)
        }
    }

    /// Gets the OID of the schema that this datasource points towards.
    pub fn get_schema_oid(&self) -> Result<i64, Error> {
        Ok(match self {
            Self::Table { table_oid, .. }
            | Self::MasterTable { table_oid, .. }
            | Self::InheritorTable { table_oid, .. } => table_oid.clone(),
            Self::Column { parent_datasource, column } => {
                let parent_datasource_schema_oid: i64 = parent_datasource.get_schema_oid()?;
                if parent_datasource_schema_oid == column.schema.oid {
                    match &column.column_type {
                        column_type::ColumnType::Object { table_oid, .. }
                        | column_type::ColumnType::Select { table_oid, .. }
                        | column_type::ColumnType::Multiselect { table_oid, .. } => table_oid.clone(),
                        _ => {
                            return Err(Error::AdhocError("Only columns of types Object, Select, and Multiselect can be used as links to a datasource."));
                        }
                    }
                } else {
                    // Normal relationship is reversed, and the column points back to the schema it is contained in
                    column.schema.oid.clone()
                }
            }
        })
    }
}