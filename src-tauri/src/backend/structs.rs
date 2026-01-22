use serde::ser::{Serialize};
use std::collections::{HashMap};

#[derive(Serialize)]
pub enum TableColumnTypeMode {
    PRIMITIVE,
    ADHOC_SINGLE_SELECT,
    ADHOC_MULTIPLE_SELECT,
    REFERENCE,
    CHILD_OBJECT,
    CHILD_TABLE
}

#[derive(Serialize)]
pub struct TableColumnType {
    oid: i64,
    mode: TableColumnTypeMode
}

#[derive(Serialize)]
pub struct TableColumn {
    oid: i64,
    name: String,
    column_type: TableColumnType,
    column_width: i64,
    column_ordering: i64,
    is_nullable: bool,
    is_unique: bool,
    is_primary_key: bool
}

#[derive(Serialize)]
pub struct Table {
    oid: i64,
    parent_table_oid: Option<i64>,
    name: String,
    data: HashMap<i64, (TableColumn, Vec<Serialize>)>,
    surrogate_key_column_oid: Option<i64>
}