use crate::data::formula::query::RecordFuncQuery;
use crate::data::formula::value::Value;
use crate::data::table::column::TableColumnMetadata;
use crate::data::table::row::TableCell; 
use crate::data::formula::func::{Func, RecordFunc};
use crate::util::db::{RowWrapper, sql_collect, sql_iter};
use crate::util::db;
use crate::util::error::Error;

pub struct Context<'a> {
    query: &'a RecordFuncQuery,
    row: &'a RowWrapper<'a>
}

impl<'a> Context<'a> {
    pub fn new(query: &'a RecordFuncQuery, row: &'a RowWrapper<'a>) -> Self {
        Self { query, row }
    }

    /// Gets the cell associated with a column.
    pub fn get_column(&self, record: &RecordFunc, table_oid: i64, column: &TableColumnMetadata) -> Result<TableCell, Error> {
        let alias: String = record.alias();
        TableCell::new(
            table_oid,
            column,
            format!("{alias}_COLUMN{}", column.oid),
            {
                let ord: String = if table_oid == record.table_oid()? {
                    format!("{alias}_OID")
                } else {
                    format!("{alias}_MASTER{table_oid}_OID")
                };
                self.row.get::<&str, _>(&ord)?
            },
            self.row
        )
    }
}