use serde::{Deserialize, Serialize};

use crate::util::{channel::Sender, error::Error};

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct FailedValidation {
    message: String
}

#[derive(Deserialize)]
#[serde(rename_all="camelCase")]
pub struct Page {
    num: i64,
    size: i64 
}

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum Cell {
    Row {
        schema_oid: i64,
        row_oid: i64,
        index: i64,
        validation_failures: Vec<FailedValidation>
    },
    Readonly {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        value: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    Subreport {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        report_oid: i64,
        validation_failures: Vec<FailedValidation>
    },
    PrimitiveEntry {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        value: Option<String>,
        validation_failures: Vec<FailedValidation>
    },
    Object {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        object_schema_oid: i64,
        object_row_oid: Option<i64>,
        validation_failures: Vec<FailedValidation>
    },
    SelectEntry {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        select_schema_oid: i64,
        select_row_oid: Option<i64>,
        validation_failures: Vec<FailedValidation>
    },
    MultiselectEntry {
        schema_oid: i64,
        row_oid: i64,
        column_oid: i64,
        multiselect_schema_oid: i64,
        multiselect_row_oid: Vec<i64>,
        validation_failures: Vec<FailedValidation>
    }
}


#[derive(Clone)]
enum Relationship {
    One,
    Many {
        intermediate_param_oid: Vec<i64>,
        final_param_oid: i64
    }
}

impl Cell {
    /// Sends all cells on a page in a schema.
    pub fn query_by_schema_page(sender: Sender<Self>, schema_oid: i64, page: Page) -> Result<(), Error> {

    }

    /// Sends all cells belonging to a particular row in a schema.
    pub fn query_by_schema_row(sender: Sender<Self>, schema_oid: i64, row_oid: i64) -> Result<(), Error> {

    }
}