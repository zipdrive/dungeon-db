use rusqlite::params;
use serde::Serialize;

use crate::{data::table::{column::TableColumnMetadata, column_type::{Primitive, TableColumnType}}, util::{channel::Sender, db::{self, RowWrapper, sql_iter, sql_one}, error::Error}};

#[derive(Serialize, Clone)]
pub enum TableCellTextContentFormat {
    Plain,
    Json,
    Xml,
    Markdown,
    BBCode
}

#[derive(Serialize, Clone)]
pub enum TableCellContent {
    Boolean {
        value: bool,
    },
    Integer {
        value: Option<i64>,
    },
    Number {
        value: Option<f64>,
    },
    Date {
        value: Option<i64>,
        label: Option<String>,
    },
    Datetime {
        value: Option<f64>,
        label: Option<String>,
    },
    Text {
        value: Option<String>,
        format: TableCellTextContentFormat,
    },
    File {
        value: Option<i64>,
        label: Option<String>,
    },
    Object {
        table_oid: i64,
        value: Option<i64>,
        label: Option<String>,
    },
    SingleSelectDropdown {
        table_oid: i64,
        value: Option<i64>,
    },
    MultiSelectDropdown {
        table_oid: i64,
        value: Vec<i64>,
    }
}

#[derive(Serialize, Clone)]
pub struct TableCell {
    table_oid: i64,
    column_oid: i64,
    row_oid: i64,
    content: TableCellContent
}

#[derive(Serialize, Clone)]
pub struct TableRow {
    oid: i64,
    index: i64,
    cells: Vec<TableCell>
}

impl TableRow {
    /// Constructs a new row from a row of the TABLE{table_oid} view + a ROW_INDEX column for tracking the index.
    fn new(row: &RowWrapper, table_oid: &i64, columns: &Vec<(i64, TableColumnMetadata)>) -> Result<Self, Error> {
        let mut data: Self = Self {
            oid: row.get("OID")?,
            index: row.get("ROW_INDEX")?,
            cells: Vec::new()
        };

        // Send the cells
        for (column_table_oid, column) in columns.iter() {
            data.cells.push(TableCell { 
                table_oid: column_table_oid.clone(), 
                column_oid: column.oid.clone(), 
                row_oid: if *column_table_oid == *table_oid {
                    data.oid.clone()
                } else {
                    let row_oid_ord: String = format!("MASTER{column_table_oid}_OID");
                    row.get::<&str, _>(&row_oid_ord)?
                }, 
                content: match &column.column_type {
                    TableColumnType::Primitive { primitive, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        match primitive {
                            Primitive::Boolean => TableCellContent::Boolean { 
                                value: row.get::<&str, Option<bool>>(&ord)?.unwrap_or(false)
                            },
                            Primitive::Integer => TableCellContent::Integer { 
                                value: row.get::<&str, _>(&ord)? 
                            },
                            Primitive::Number => TableCellContent::Number { 
                                value: row.get::<&str, _>(&ord)? 
                            },
                            Primitive::Date => TableCellContent::Date { 
                                value: row.get::<&str, _>(&ord)?, 
                                label: todo!("date labels")
                            },
                            Primitive::Datetime => TableCellContent::Datetime { 
                                value: row.get::<&str, _>(&ord)?, 
                                label: todo!("datetime labels")
                            },
                            Primitive::Text => TableCellContent::Text { 
                                value: row.get::<&str, _>(&ord)?, 
                                format: TableCellTextContentFormat::Plain 
                            },
                            Primitive::TextJson => TableCellContent::Text { 
                                value: row.get::<&str, _>(&ord)?, 
                                format: TableCellTextContentFormat::Json 
                            },
                            Primitive::TextXml => TableCellContent::Text { 
                                value: row.get::<&str, _>(&ord)?, 
                                format: TableCellTextContentFormat::Xml 
                            },
                            Primitive::TextMarkdown => TableCellContent::Text { 
                                value: row.get::<&str, _>(&ord)?, 
                                format: TableCellTextContentFormat::Markdown 
                            },
                            Primitive::TextBBCode => TableCellContent::Text { 
                                value: row.get::<&str, _>(&ord)?, 
                                format: TableCellTextContentFormat::BBCode 
                            }
                        }
                    }
                    TableColumnType::File { .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        let value: Option<i64> = row.get::<&str, _>(&ord)?;
                        TableCellContent::File { 
                            value, 
                            label: todo!("file labels") 
                        }
                    }
                    TableColumnType::Object { table_oid: referenced_table_oid, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        TableCellContent::Object { 
                            table_oid: referenced_table_oid.clone(), 
                            value: row.get::<&str, _>(&ord)?,
                            label: todo!("object labels")
                        }
                    }
                    TableColumnType::SingleSelect { table_oid: referenced_table_oid, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        TableCellContent::SingleSelectDropdown { 
                            table_oid: referenced_table_oid.clone(), 
                            value: row.get::<&str, _>(&ord)?
                        }
                    }
                    TableColumnType::MultiSelect { table_oid: referenced_table_oid, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        let s = row.get::<&str, Option<String>>(&ord)?;
                        TableCellContent::MultiSelectDropdown { 
                            table_oid: referenced_table_oid.clone(), 
                            value: match s {
                                Some(s) => s.split(',').filter_map(|n| match i64::from_str_radix(n, 10) {
                                    Ok(n) => Some(n),
                                    Err(_) => None
                                }).collect(),
                                None => Vec::new()
                            }
                        }
                    }
                    TableColumnType::Subreport { report_oid, .. } => {

                    }
                }
            });
        }

        Ok(data)
    }

    /// Gets a single row from the given table.
    pub fn get(table_oid: i64, row_oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;

        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all_inherited(&conn, table_oid)?
            .into_iter().map(|(owning_table_oid, _, column)| (owning_table_oid, column)).collect();
        
        // Send the rows of the table
        sql_one(
            &conn, 
            format!("SELECT ROW_NUMBER() AS ROW_INDEX, * FROM TABLE{table_oid} WHERE OID = ?1"), 
            params![row_oid], 
            |row| {
                Self::new(row, &table_oid, &columns)
            }
        )
    }

    /// Queries for multiple rows from the given table.
    pub fn query(mut sender: Sender<Self>, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all_inherited(&conn, table_oid)?
            .into_iter().map(|(owning_table_oid, _, column)| (owning_table_oid, column)).collect();
        
        // Send the rows of the table
        sql_iter(
            &conn, 
            format!(
                "SELECT ROW_NUMBER() AS ROW_INDEX, * FROM TABLE{table_oid} {}",
                "" // todo limits
            ), 
            [], 
            |row| {
                let payload: Self = Self::new(row, &table_oid, &columns)?;
                sender.send(payload)?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }
}