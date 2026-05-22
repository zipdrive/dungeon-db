use crate::data::column::DropdownValue;
use crate::util::channel::Sender;
use crate::util::error::Error;
use crate::util::db;
use crate::data::{column, column_type, schema, table};
use regex::Regex;
use rusqlite::{Connection, OptionalExtension};
use rusqlite::types::Value;
use rusqlite::{Transaction, params, vtab::array::Array};
use std::hash::{Hash, Hasher};
use std::borrow::Borrow;
use std::rc::Rc;
use serde::{Deserialize, Serialize};



#[derive(PartialEq, Eq, Clone)]
pub enum Relationship {
    One,
    Many
}



#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct DatasourceDropdownValue {
    pub value: Datasource,
    pub label: String
}

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct ParameterDropdownValue {
    pub value: String,
    pub label: String
}



#[derive(PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Datasource {
    Table {
        oid: i64,
        table_oid: i64
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

    /// Construct a datasource from a path.
    pub fn from_alias(alias: String) -> Result<Self, Error> {
        let path: Vec<String> = alias.split('_').map(|s| String::from(s)).collect();
        if path.len() == 0 {
            return Err(Error::AdhocError("Datasource cannot be empty!"));
        }

        // Check for root datasource
        let root_regex: Regex = Regex::new(r#"^ROOT(\d+)$"#).unwrap();
        if let Some(root_caps) = root_regex.captures(&path[0]) {
            let (_, [root_datasource_oid_str]) = root_caps.extract();
            let root_datasource_oid: i64 = root_datasource_oid_str.parse().unwrap();
            let root: Self = Self::get(root_datasource_oid)?;
            return Self::from_parent_and_path(root, &path[1..]);
        } else {
            return Err(Error::AdhocError("Root datasource is expected to be an OID of a row in METADATA_DATASOURCE."));
        };
    }

    /// Construct a datasource from an alias.
    pub fn from_alias_transact(conn: &Connection, alias: String) -> Result<Self, Error> {
        let path: Vec<String> = alias.split('_').map(|s| String::from(s)).collect();
        if path.len() == 0 {
            return Err(Error::AdhocError("Datasource cannot be empty!"));
        }

        // Check for root datasource
        let root_regex: Regex = Regex::new(r#"^ROOT(\d+)$"#).unwrap();
        if let Some(root_caps) = root_regex.captures(&path[0]) {
            let (_, [root_datasource_oid_str]) = root_caps.extract();
            let root_datasource_oid: i64 = root_datasource_oid_str.parse().unwrap();
            let root: Self = Self::get_transact(conn, root_datasource_oid)?;
            return Self::from_parent_and_path(root, &path[1..]);
        } else {
            return Err(Error::AdhocError("Root datasource is expected to be an OID of a row in METADATA_DATASOURCE."));
        };
    }

    /// Retrieve a root datasource by OID, as part of a transaction.
    fn get_transact(conn: &Connection, oid: i64) -> Result<Self, Error> {
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
            table_oid
        })
    }

    /// Retrieve a root datasource by OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::get_transact(&conn, oid)
    }

    /// Retrieves the default root datasource for a particular table.
    pub fn get_default_datasource_transact(conn: &Connection, table_oid: i64) -> Result<Self, Error> {
        Ok(Self::Table {
            oid: conn.query_row("SELECT OID FROM METADATA_DATASOURCE WHERE TABLE_OID = ?1", params![table_oid], |row| row.get(0))?,
            table_oid
        })
    }

    /// Gets the alias of the datasource.
    pub fn get_alias(&self) -> String {
        match self {
            Self::Table { oid, .. } => format!("ROOT{oid}"),
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

    /// Queries for all root datasources.
    pub fn query_roots(mut sender: Sender<DatasourceDropdownValue>) -> Result<(), Error> {
        let conn: Connection = db::open()?;
        for root_result in 
            conn.prepare(
            "
            SELECT 
                d.OID, 
                d.TABLE_OID, 
                COALESCE(d.LABEL, s.NAME) AS LABEL 
            FROM METADATA_DATASOURCE d 
            INNER JOIN METADATA_SCHEMA s ON s.OID = d.TABLE_OID
            WHERE NOT s.TRASH
            ORDER BY 
                d.LABEL NULLS FIRST, 
                s.NAME
            "
            )?
            .query_and_then([], |row| Ok::<(i64, i64, String), rusqlite::Error>((row.get("OID")?,row.get("TABLE_OID")?,row.get("LABEL")?)))? {
            
            let (datasource_oid, table_oid, datasource_label) = root_result?;
            sender.send(DatasourceDropdownValue {
                value: Self::Table { oid: datasource_oid, table_oid }, 
                label: datasource_label
            })?;
        }
        Ok(())
    }

    /// Queries for links from this datasource to another.
    pub fn query_links(&self, mut sender: Sender<DatasourceDropdownValue>) -> Result<(), Error> {
        let conn: Connection = db::open()?;
        let table_oid: i64 = self.get_schema_oid()?;

        // Query for columns on the schema for self
        for column_oid_result in conn.prepare(
            "
            SELECT 
                c.OID
            FROM METADATA_COLUMN c
            WHERE c.SCHEMA_OID = ?1
                AND NOT c.TRASH
                AND c.TYPE_OID IN (
                    SELECT OID FROM METADATA_COLUMN_TYPE__OBJECT
                    UNION
                    SELECT OID FROM METADATA_COLUMN_TYPE__SELECT
                    UNION
                    SELECT OID FROM METADATA_COLUMN_TYPE__MULTISELECT
                )
            ")?
            .query_map(params![table_oid], |row| row.get("OID"))? {

            let column_oid = column_oid_result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;
            let datasource_label: String = format!("REFERENCE: {}", column_metadata.name);
            sender.send(DatasourceDropdownValue {
                value: Self::Column { 
                    parent_datasource: Box::new(self.clone()), 
                    column: column_metadata
                }, 
                label: datasource_label 
            })?;
        }

        // Query for master tables
        for master_table_result in conn.prepare(
            "
            SELECT 
                s.OID,
                s.NAME
            FROM METADATA_SCHEMA_INHERITANCE_VIEW inh
            INNER JOIN METADATA_SCHEMA s ON s.OID = inh.MASTER_SCHEMA_OID
            WHERE inh.INHERITOR_SCHEMA_OID = ?1
                AND EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = inh.MASTER_SCHEMA_OID)
            ")?
            .query_map(params![table_oid], |row| Ok::<(i64, String), rusqlite::Error>((row.get("OID")?, row.get("NAME")?)))? {

            let (master_table_oid, master_table_name) = master_table_result?;
            let datasource_label: String = format!("MASTER: {master_table_name}");
            sender.send(DatasourceDropdownValue {
                value: Self::MasterTable { 
                    parent_datasource: Box::new(self.clone()), 
                    table_oid: master_table_oid
                }, 
                label: datasource_label
            })?;
        }

        // Query for inheritor tables
        for inheritor_table_result in conn.prepare(
            "
            SELECT 
                s.OID,
                s.NAME
            FROM METADATA_SCHEMA_INHERITANCE_VIEW inh
            INNER JOIN METADATA_SCHEMA s ON s.OID = inh.INHERITOR_SCHEMA_OID
            WHERE inh.MASTER_SCHEMA_OID = ?1
                AND EXISTS(SELECT OID FROM METADATA_TABLE WHERE OID = inh.INHERITOR_SCHEMA_OID)
            ")?
            .query_map(params![table_oid], |row| Ok::<(i64, String), rusqlite::Error>((row.get("OID")?, row.get("NAME")?)))? {

            let (inheritor_table_oid, inheritor_table_name) = inheritor_table_result?;
            let datasource_label: String = format!("INHERITOR: {inheritor_table_name}");
            sender.send(DatasourceDropdownValue {
                value: Self::InheritorTable { 
                    parent_datasource: Box::new(self.clone()), 
                    table_oid: inheritor_table_oid
                }, 
                label: datasource_label 
            })?;
        }

        // Query for columns on other tables referencing this one
        for column_oid_result in conn.prepare(
            "
            SELECT 
                c.OID AS COLUMN_OID,
                s.NAME AS SCHEMA_NAME
            FROM METADATA_COLUMN_TYPE__OBJECT typ
            INNER JOIN METADATA_COLUMN c ON typ.OID = c.TYPE_OID
            INNER JOIN METADATA_SCHEMA s ON s.OID = c.SCHEMA_OID
            WHERE typ.TABLE_OID = ?1
                AND NOT c.TRASH
                AND NOT s.TRASH

            UNION

            SELECT 
                c.OID AS COLUMN_OID,
                s.NAME AS SCHEMA_NAME
            FROM METADATA_COLUMN_TYPE__SELECT typ
            INNER JOIN METADATA_COLUMN c ON typ.OID = c.TYPE_OID
            INNER JOIN METADATA_SCHEMA s ON s.OID = c.SCHEMA_OID
            WHERE typ.TABLE_OID = ?1
                AND NOT c.TRASH
                AND NOT s.TRASH
            
            UNION

            SELECT 
                c.OID AS COLUMN_OID,
                s.NAME AS SCHEMA_NAME
            FROM METADATA_COLUMN_TYPE__MULTISELECT typ
            INNER JOIN METADATA_COLUMN c ON typ.OID = c.TYPE_OID
            INNER JOIN METADATA_SCHEMA s ON s.OID = c.SCHEMA_OID
            WHERE typ.TABLE_OID = ?1
                AND NOT c.TRASH
                AND NOT s.TRASH
            ")?
            .query_map(params![table_oid], |row| Ok::<(i64, String), rusqlite::Error>((row.get("COLUMN_OID")?, row.get("SCHEMA_NAME")?)))? {

            let (column_oid, schema_name) = column_oid_result?;
            let column_metadata: column::FullMetadata = column::FullMetadata::get_transact(&conn, column_oid)?;
            let datasource_label: String = format!("BACKREFERENCE: {schema_name} / {}", column_metadata.name);
            sender.send(DatasourceDropdownValue {
                value: Self::Column { 
                    parent_datasource: Box::new(self.clone()), 
                    column: column_metadata
                }, 
                label: datasource_label
            })?;
        }
        Ok(())
    }

    /// Queries for parameters associated with the datasource.
    pub fn query_parameters(&self, mut sender: Sender<ParameterDropdownValue>) -> Result<(), Error> {
        let conn: Connection = db::open()?;
        let table_oid: i64 = self.get_schema_oid()?;

        for column_result in conn.prepare(
            "
            SELECT 
                c.OID,
                c.NAME
            FROM METADATA_COLUMN c
            WHERE c.SCHEMA_OID = ?1
                AND NOT c.TRASH
            ORDER BY c.ORDERING
            ")?
            .query_map(params![table_oid], |row| Ok::<(i64, String), rusqlite::Error>((row.get("OID")?, row.get("NAME")?)))? {

            let (column_oid, column_name) = column_result?;
            let parameter_path: String = format!("{}_COLUMN{column_oid}", self.get_alias());
            sender.send(ParameterDropdownValue {
                value: parameter_path,
                label: column_name
            })?;
        }

        Ok(())
    }

    /// Seeks the deepest parent which is either (a) a Table datasource, or (b) has a 1-to-* relationship with its parent.
    pub fn seek_basis(&self) -> Result<Datasource, Error> {
        Ok(match self {
            Self::Table { .. } => self.clone(),
            Self::MasterTable { parent_datasource, .. }
            | Self::InheritorTable { parent_datasource, .. } => parent_datasource.seek_basis()?,
            Self::Column { parent_datasource, column } => {
                let parent_datasource_schema_oid: i64 = parent_datasource.get_schema_oid()?;
                match &column.column_type {
                    column_type::ColumnType::Object { .. } => parent_datasource.seek_basis()?,
                    | column_type::ColumnType::Select { .. } => if parent_datasource_schema_oid == column.schema.oid {
                        parent_datasource.seek_basis()?
                    } else {
                        self.clone()
                    },
                    | column_type::ColumnType::Multiselect { .. } => self.clone(),
                    _ => {
                        return Err(Error::AdhocError("Only columns of types Object, Select, and Multiselect can be used as links to a datasource."));
                    }
                }
            }
        })
    }
}