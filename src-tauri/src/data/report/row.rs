use base64::engine::general_purpose::{STANDARD as base64standard};
use base64::Engine;
use rusqlite::params;
use rusqlite::types::ValueRef;
use serde::Serialize;
use finetime::IntoDateTime;
use finetime;
use std::borrow::Borrow;
use crate::data::file::File;
use crate::data::formula::context::Context;
use crate::data::formula::func::Func;
use crate::data::formula::query::RecordFuncQuery;
use crate::data::report::ReportMetadata;
use crate::data::report::column::ReportColumnMetadata;
use crate::data::report::column_type::ReportColumnType;
use crate::data::table::row::{TableCellContent, TableCellTextContentFormat};
use crate::data::formula::value::{TableCellReference, Value};
use crate::util::db::{sql_collect, sql_iter, sql_one};
use crate::util::db;
use crate::util::error::Error;
use crate::util::channel::Sender;
use crate::util::mapping::oid_list;

enum ParsedReportColumn {
    Formula {
        column_oid: i64,
        func: Func 
    },
    InvalidSyntaxFormula {
        column_oid: i64,
        msg: String,
        full_formula: String,
        substring_with_error: String
    },
    Subreport {
        column_oid: i64,
        report_oid: i64
    }
}

#[derive(Clone, Serialize)]
pub struct ReportCell {
    column_oid: i64,
    reference: Option<TableCellReference>,
    content: TableCellContent
}

#[derive(Clone, Serialize)]
pub struct ReportRow {
    pub oid_filters: Vec<(String, Vec<i64>)>,
    pub index: i64,
    pub cells: Vec<ReportCell>
}

impl ReportRow {
    /// Queries for multiple rows from the given table.
    pub fn send(mut column_sender: Sender<ReportColumnMetadata>, mut row_sender: Sender<Self>, report_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        let report: ReportMetadata = ReportMetadata::conn_get(&conn, report_oid)?;
        let filter_func: Option<Func> = if let Some(filter_formula) = report.filter_formula {
            Some(Func::parse(filter_formula)?)
        } else {
            None 
        };

        // Send the columns of the report
        let columns: Vec<ParsedReportColumn> = {
            let mut output: Vec<ParsedReportColumn> = Vec::new();
            let columns = ReportColumnMetadata::conn_query_all(&conn, report_oid)?;
            for column in columns {
                column_sender.send(column.clone())?;
                match column.column_type {
                    ReportColumnType::Formula { formula, .. } => {
                        // Parse the formula
                        match Func::parse(formula) {
                            Ok(func) => {
                                output.push(ParsedReportColumn::Formula { 
                                    column_oid: column.oid, 
                                    func
                                });
                            }
                            Err(Error::FormulaSyntaxError { msg, full_formula, substring_with_error }) => {
                                output.push(ParsedReportColumn::InvalidSyntaxFormula { 
                                    column_oid: column.oid, 
                                    msg, 
                                    full_formula, 
                                    substring_with_error 
                                });
                            }
                            Err(e) => {
                                // Bubble any other errors up
                                return Err(e);
                            }
                        }
                    }
                    ReportColumnType::Subreport { report_oid, .. } => {
                        output.push(ParsedReportColumn::Subreport { 
                            column_oid: column.oid,
                            report_oid 
                        });
                    }
                }
            }
            output
        };

        let mut query: RecordFuncQuery = RecordFuncQuery::new();

        // Determine which records are to be automatically excluded from groupings
        if let Some(filter_func) = &filter_func {
            for (record, table_oid, col) in filter_func.get_nonaggregated_columns() {
                query.add_column(&record.linearize(), table_oid, col.name)?;
            }
        }
        for column in columns.iter() {
            if let ParsedReportColumn::Formula { func, .. } = column {
                for (record, table_oid, col) in func.get_nonaggregated_columns() {
                    query.add_column(&record.linearize(), table_oid, col.name)?;
                }
            }
        }
        let auto_group_oids = query.oid_filters();

        // Add all columns used in formulas to the query
        for column in columns.iter() {
            if let ParsedReportColumn::Formula { func, .. } = column {
                for (record, table_oid, col) in func.get_all_columns() {
                    query.add_column(&record.linearize(), table_oid, col.name)?;
                }
            }
        }
        let cte: String = query.sql();
        let all_oids = query.oid_filters();

        // Construct the SQL query
        let sql: String = format!(
            "
WITH {cte}
SELECT
    ROW_NUMBER() OVER () AS ROW_INDEX,
    *
FROM (
    SELECT 
        {}
    FROM WRAPPER
    -- Filter is applied pre-grouping  
    {}
    -- Grouping 
    {}
    -- Ordering 
    {}
)
            ",
            // Convert each column into an equivalent SQL expression
            {
                let mut cols: Vec<String> = Vec::new();
                for column in columns.iter() {
                    if let ParsedReportColumn::Formula { column_oid, func } = column {
                        let sql = func.sql()?;
                        cols.push(format!("{} AS COLUMN{column_oid}", sql.value_expr));
                        cols.push(format!("{} AS COLUMN{column_oid}_REFERENCE", sql.reference_expr));
                    }
                }
                cols
            }
                .into_iter()
            // Also, OID columns grouped by the determined grouping
                .chain(
                    all_oids.iter().map(|ord| format!("GROUP_CONCAT(CAST({ord} AS TEXT), ',') AS {ord}"))
                )
                .reduce(|acc, e| format!("{acc}, {e}"))
                .unwrap_or(String::from("")),

            // Filters on the query, applied pre-grouping
            if let Some(filter_func) = &filter_func {
                let filter_sql = filter_func.sql()?;
                format!("WHERE {}", filter_sql.value_expr)
            } else {
                String::from("")
            },

            // Grouping
            {
                let group_column_oids: Vec<i64> = sql_collect(
                    &conn, 
                    "SELECT COLUMN_OID FROM METADATA_REPORT_GROUPBY WHERE REPORT_OID = ?1", 
                    params![report_oid], 
                    |row| row.get("COLUMN_OID")
                )?;

                match group_column_oids.iter()
                    .filter_map(|column_oid| {
                        let mut idx: i64 = 0;
                        for c in columns.iter() {
                            if let ParsedReportColumn::Formula { column_oid: c_oid, .. } = c {
                                idx += 1;
                                if column_oid == c_oid {
                                    return Some(format!("{idx}"));
                                }
                            }
                        }
                        return None;
                    })
                    .reduce(|acc, e| format!("{acc}, {e}")) {

                    // Query with grouping by specific columns
                    Some(group_column_indices) => format!("GROUP BY {group_column_indices}"),

                    // Try to query with auto-grouping
                    None => match auto_group_oids.into_iter()
                        .reduce(|acc, e| format!("{acc}, {e}")) {
                        
                        // Query with auto-grouping
                        Some(auto_group_expr) => format!("GROUP BY {auto_group_expr}"),
                        
                        // Query with no grouping
                        None => String::from("")
                    }
                }
            },

            // Ordering
            {
                let order_column_oids: Vec<(i64, bool)> = sql_collect(
                    &conn, 
                    "SELECT COLUMN_OID, DIRECTION FROM METADATA_REPORT_ORDERBY WHERE REPORT_OID = ?1", 
                    params![report_oid], 
                    |row| Ok((
                        row.get("COLUMN_OID")?,
                        row.get("DIRECTION")?
                    ))
                )?;

                match order_column_oids.iter()
                    .filter_map(|(column_oid, dir)| {
                        let mut idx: i64 = 0;
                        for c in columns.iter() {
                            if let ParsedReportColumn::Formula { column_oid: c_oid, .. } = c {
                                idx += 1;
                                if column_oid == c_oid {
                                    return Some(format!("{idx} {}", if *dir { "ASC" } else { "DESC" }));
                                }
                            }
                        }
                        return None;
                    })
                    .reduce(|acc, e| format!("{acc}, {e}")) {

                    // Query with ordering by specific columns
                    Some(group_column_indices) => format!("ORDER BY {group_column_indices}"),

                    // Query with native ordering
                    None => String::from("")
                }
            }
        );

        sql_iter(
            &conn, 
            sql, 
            [], 
            |row| {
                let mut oid_filters: Vec<(String, Vec<i64>)> = Vec::new();
                for oid_ord in all_oids.iter() {
                    oid_filters.push((
                        oid_ord.clone(), 
                        if let Some(oid_values) = row.get::<&str, Option<String>>(oid_ord)? {
                            oid_list(oid_values)
                        } else {
                            Vec::new()
                        }
                    ));
                }

                let mut cells: Vec<ReportCell> = Vec::new();
                for column in columns.iter() {
                    cells.push(match column {
                        ParsedReportColumn::Formula { column_oid, func } => {
                            let type_reference_str: String = row.get::<&str, _>(&format!("COLUMN{column_oid}_REFERENCE"))?;
                            let Some((known_type, reference_str)) = type_reference_str.split_once(';') else {
                                return Err(Error::adhoc("Expected ';' to be in formula return value's reference, but it was not."));
                            };
                            let reference: Option<TableCellReference> = if reference_str == "" {
                                None
                            } else {
                                let reference_split: Vec<&str> = reference_str.splitn(3, ':').collect();
                                let Ok(table_oid) = i64::from_str_radix(reference_split[0], 10) else {
                                    return Err(Error::adhoc("Expected table OID to be in formula return value's reference, but it was not."));
                                };
                                let Ok(column_oid) = i64::from_str_radix(reference_split[1], 10) else {
                                    return Err(Error::adhoc("Expected column OID to be in formula return value's reference, but it was not."));
                                };
                                let Ok(row_oid) = i64::from_str_radix(reference_split[2], 10) else {
                                    return Err(Error::adhoc("Expected row OID to be in formula return value's reference, but it was not."));
                                };
                                Some(TableCellReference {
                                    table_oid, 
                                    column_oid, 
                                    row_oid 
                                })
                            };
                            ReportCell {
                                column_oid: column_oid.clone(),
                                reference,
                                content: {
                                    let ord: String = format!("COLUMN{column_oid}");
                                    if known_type == "Boolean" {
                                        TableCellContent::Boolean { value: row.get::<&str, _>(&ord)? }
                                    } else if known_type == "Date" || known_type == "Datetime" {
                                        let value: Option<f64> = row.get::<&str, _>(&ord)?;
                                        let label: Option<String> = if let Some(value) = &value {
                                            let julian: finetime::JulianDay<_, finetime::SecondsPerHalfDay> = finetime::JulianDay::from_time_since_epoch(finetime::Duration::new(value.clone()));
                                            let jul_point = finetime::TaiTime::from_julian_day(julian);
                                            if let Some(sec_point) = jul_point.try_into_unit::<finetime::Second>() {
                                                let (date, hour, minute, second) = sec_point.into_gregorian_datetime();
                                                let month: u8 = match date.month() {
                                                    finetime::Month::January => 1,
                                                    finetime::Month::February => 2,
                                                    finetime::Month::March => 3,
                                                    finetime::Month::April => 4,
                                                    finetime::Month::May => 5,
                                                    finetime::Month::June => 6,
                                                    finetime::Month::July => 7,
                                                    finetime::Month::August => 8,
                                                    finetime::Month::September => 9,
                                                    finetime::Month::October => 10,
                                                    finetime::Month::November => 11,
                                                    finetime::Month::December => 12,
                                                };
                                                Some(format!("{:04}-{:02}-{:02}T{hour:02}:{minute:02}:{second:02}Z", date.year(), month, date.day()))
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        };
                                        if known_type == "Date" {
                                            TableCellContent::Date { 
                                                label,
                                                value 
                                            }
                                        } else {
                                            TableCellContent::Datetime { 
                                                label,
                                                value
                                            }
                                        }
                                    } else if known_type == "Integer" {
                                        TableCellContent::Integer { value: row.get::<&str, _>(&ord)? }
                                    } else if known_type == "Number" {
                                        TableCellContent::Number { value: row.get::<&str, _>(&ord)? }
                                    } else if known_type.starts_with("Text") {
                                        TableCellContent::Text { 
                                            value: {
                                                let value_ref = row.get_ref::<&str>(&ord)?;
                                                match value_ref {
                                                    ValueRef::Null => None,
                                                    ValueRef::Text(text) => Some(match str::from_utf8(text) {
                                                        Ok(text) => String::from(text),
                                                        Err(_) => {
                                                            return Err(Error::adhoc("Unable to extract text value from formula return value."));
                                                        }
                                                    }),
                                                    ValueRef::Integer(value) => Some(format!("{value}")),
                                                    ValueRef::Real(value) => Some(format!("{value}")),
                                                    ValueRef::Blob(buf) => Some(base64standard.encode(&buf))
                                                }
                                            }, 
                                            format: TableCellTextContentFormat::Plain 
                                        }
                                    } else if known_type.starts_with("File") {
                                        TableCellContent::File { 
                                            value: if let Some(file_oid) = row.get::<&str, Option<i64>>(&ord)? {
                                                Some(File::get(file_oid)?)
                                            } else {
                                                None
                                            }
                                        }
                                    } else if known_type.starts_with("Object") {
                                        let Some((_, table_oid_str)) = known_type.split_once('/') else {
                                            return Err(Error::adhoc("Expected referenced table OID to be in formula's return value type, but it was not."));
                                        };
                                        let Ok(table_oid) = i64::from_str_radix(table_oid_str, 10) else {
                                            return Err(Error::adhoc("An invalid referenced table OID was in formula's return value type."));
                                        };
                                        TableCellContent::Object { 
                                            table_oid, 
                                            value: row.get::<&str, _>(&ord)?
                                        }
                                    } else if known_type.starts_with("SingleSelect") {
                                        let Some((_, table_oid_str)) = known_type.split_once('/') else {
                                            return Err(Error::adhoc("Expected referenced table OID to be in formula's return value type, but it was not."));
                                        };
                                        let Ok(table_oid) = i64::from_str_radix(table_oid_str, 10) else {
                                            return Err(Error::adhoc("An invalid referenced table OID was in formula's return value type."));
                                        };
                                        TableCellContent::SingleSelectDropdown { 
                                            table_oid, 
                                            value: row.get::<&str, _>(&ord)?
                                        }
                                    } else if known_type.starts_with("MultiSelect") {
                                        let Some((_, table_oid_str)) = known_type.split_once('/') else {
                                            return Err(Error::adhoc("Expected referenced table OID to be in formula's return value type, but it was not."));
                                        };
                                        let Ok(table_oid) = i64::from_str_radix(table_oid_str, 10) else {
                                            return Err(Error::adhoc("An invalid referenced table OID was in formula's return value type."));
                                        };
                                        TableCellContent::MultiSelectDropdown { 
                                            table_oid, 
                                            value: oid_list(row.get::<&str, String>(&ord)?)
                                        }
                                    } else {
                                        return Err(Error::adhoc(format!("Unknown formula return type \"{known_type}\"")));
                                    }
                                }
                            }
                        }
                        ParsedReportColumn::InvalidSyntaxFormula { column_oid, msg, full_formula, substring_with_error } => {
                            // TODO add error tooltip
                            ReportCell {
                                column_oid: column_oid.clone(),
                                reference: None,
                                content: TableCellContent::Text { 
                                    value: None, 
                                    format: TableCellTextContentFormat::Plain 
                                }
                            }
                        }
                        ParsedReportColumn::Subreport { column_oid, report_oid } => {
                            ReportCell {
                                column_oid: column_oid.clone(),
                                reference: None,
                                content: TableCellContent::Subreport { 
                                    report_oid: report_oid.clone(),
                                    oid_filters: oid_filters.clone()
                                }
                            }
                        }
                    });
                }
                row_sender.send(ReportRow {
                    oid_filters,
                    index: row.get("ROW_INDEX")?,
                    cells
                })?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }
}