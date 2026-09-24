use rusqlite::{Connection, params};
use serde::Serialize;

use crate::{data::{file::File, table::{column::TableColumnMetadata, column_type::{Primitive, TableColumnType}, label}}, util::{channel::Sender, db::{self, RowWrapper, sql_iter, sql_one}, error::Error}};

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
        value: Option<File>,
    },
    Object {
        table_oid: i64,
        value: Option<i64>,
    },
    SingleSelectDropdown {
        table_oid: i64,
        value: Option<i64>,
    },
    MultiSelectDropdown {
        table_oid: i64,
        value: Vec<i64>,
    },
    Subreport {
        report_oid: i64,
    }
}

#[derive(Serialize, Clone)]
pub struct TableCell {
    pub table_oid: i64,
    pub column_oid: i64,
    pub row_oid: i64,
    pub content: TableCellContent
}

#[derive(Serialize, Clone)]
pub struct TableRow {
    pub oid: i64,
    pub index: i64,
    pub cells: Vec<TableCell>
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
                            Primitive::Date => {
                                let label_ord: String = format!("COLUMN{}_LABEL", column.oid);
                                TableCellContent::Date { 
                                    value: row.get::<&str, _>(&ord)?, 
                                    label: row.get::<&str, _>(&label_ord)?
                                }
                            },
                            Primitive::Datetime => {
                                let label_ord: String = format!("COLUMN{}_LABEL", column.oid);
                                TableCellContent::Datetime { 
                                    value: row.get::<&str, _>(&ord)?, 
                                    label: row.get::<&str, _>(&label_ord)?
                                }
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
                        let file: Option<File> = if let Some(value) = value { Some(File::get(value)?) } else { None };
                        TableCellContent::File { 
                            value: file,
                        }
                    }
                    TableColumnType::Object { table_oid: referenced_table_oid, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        TableCellContent::Object { 
                            table_oid: referenced_table_oid.clone(), 
                            value: row.get::<&str, _>(&ord)?,
                        }
                    }
                    TableColumnType::SingleSelect { table_oid: referenced_table_oid, .. } => {
                        let ord: String = format!("COLUMN{}", column.oid);
                        TableCellContent::SingleSelectDropdown { 
                            table_oid: referenced_table_oid.clone(), 
                            value: row.get::<&str, _>(&ord)?,
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
                        TableCellContent::Subreport { 
                            report_oid: report_oid.clone()
                        }
                    }
                }
            });
        }

        Ok(data)
    }

    /// Gets a single row from the given table.
    pub fn get(table_oid: i64, row_oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, table_oid, row_oid)
    }

    /// Gets a single row from the given table.
    /// Uses the given connection.
    pub fn conn_get(conn: &Connection, table_oid: i64, row_oid: i64) -> Result<Self, Error> {
        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all(&conn, table_oid)?
            .into_iter().map(|(owning_table_oid, _, column)| (owning_table_oid, column)).collect();
        
        // Send the rows of the table
        sql_one(
            conn, 
            format!("SELECT ROW_NUMBER() AS ROW_INDEX, * FROM TABLE{table_oid} WHERE OID = ?1"), 
            params![row_oid], 
            |row| {
                Self::new(row, &table_oid, &columns)
            }
        )
    }

    /// Queries for multiple rows from the given table.
    pub fn send(mut sender: Sender<Self>, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all(&conn, table_oid)?
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


#[derive(Serialize, Clone)]
pub struct TableRowLabel {
    pub oid: i64,
    pub label: String
}

impl TableRowLabel {
    /// Send all labels for SingleSelect and MultiSelect options.
    pub fn send_all(mut sender: Sender<Self>, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;
        sql_iter(
            &conn,
            format!("SELECT OID FROM TABLE{table_oid}"),
            [],
            |row| {
                let oid: i64 = row.get("OID")?;
                let label: String = label::get_select_label(table_oid.clone(), oid.clone())?;
                sender.send(Self {
                    oid,
                    label
                })?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }
}