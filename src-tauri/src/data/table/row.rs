use rusqlite::{Connection, Transaction, params};
use serde::{Serialize, Deserialize};
use std::collections::{HashSet, HashMap};

use crate::data::file::File;
use crate::data::table::column::TableColumnMetadata; 
use crate::data::table::column_type::{Primitive, TableColumnType};
use crate::data::table::label;
use crate::util::db::{RowWrapper, sql_iter, sql_map_then_iter, sql_one, sql_zero_or_one, sql_execute};
use crate::util::db;
use crate::util::channel::Sender;
use crate::util::error::Error;

#[derive(Serialize, Deserialize, Clone)]
pub enum TableCellTextContentFormat {
    Plain,
    Json,
    Xml,
    Markdown,
    BBCode
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
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
        value: Option<f64>,
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
        oid_filters: Vec<(String, Vec<i64>)>
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase")]
pub struct TableCell {
    pub table_oid: i64,
    pub column_oid: i64,
    pub row_oid: i64,
    pub content: TableCellContent
}

impl TableCell {
    pub fn new(table_oid: i64, column: &TableColumnMetadata, column_base_alias: String, row_oid: i64, row: &RowWrapper) -> Result<Self, Error> {
        Ok(Self {  
            content: match &column.column_type {
                TableColumnType::Primitive { primitive, .. } => {
                    let ord: String = column_base_alias.clone();
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
                            let label_ord: String = format!("{column_base_alias}_LABEL");
                            TableCellContent::Date { 
                                value: row.get::<&str, _>(&ord)?, 
                                label: row.get::<&str, _>(&label_ord)?
                            }
                        },
                        Primitive::Datetime => {
                            let label_ord: String = format!("{column_base_alias}_LABEL");
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
                    let ord: String = column_base_alias;
                    let value: Option<i64> = row.get::<&str, _>(&ord)?;
                    let file: Option<File> = if let Some(value) = value { Some(File::get(value)?) } else { None };
                    TableCellContent::File { 
                        value: file,
                    }
                }
                TableColumnType::Object { table_oid: referenced_table_oid, .. } => {
                    let ord: String = column_base_alias;
                    TableCellContent::Object { 
                        table_oid: referenced_table_oid.clone(), 
                        value: row.get::<&str, _>(&ord)?,
                    }
                }
                TableColumnType::SingleSelect { table_oid: referenced_table_oid, .. } => {
                    let ord: String = column_base_alias;
                    TableCellContent::SingleSelectDropdown { 
                        table_oid: referenced_table_oid.clone(), 
                        value: row.get::<&str, _>(&ord)?,
                    }
                }
                TableColumnType::MultiSelect { table_oid: referenced_table_oid, .. } => {
                    let ord: String = column_base_alias;
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
                        report_oid: report_oid.clone(),
                        oid_filters: vec![(
                            format!("TABLE{table_oid}_OID"),
                            vec![row_oid.clone()]
                        )]
                    }
                }
            },
            table_oid, 
            column_oid: column.oid.clone(), 
            row_oid
        })
    }

    /// Retrieves a specific cell.
    pub fn conn_get(conn: &Connection, table_oid: i64, column: &TableColumnMetadata, row_oid: i64) -> Result<Self, Error> {
        sql_one(
            conn, 
            format!("SELECT * FROM TABLE{table_oid} WHERE OID = ?1"), 
            params![row_oid], 
            |row| Self::new(table_oid, column, format!("COLUMN{}", column.oid), row_oid, row)
        )
    }

    /// Sets the value of the cell in the database.
    /// Uses the given connection.
    fn conn_set(&self, conn: &Connection) -> Result<Self, Error> {
        // Retrieve the old cell's value
        let (_, column) = TableColumnMetadata::conn_get(&conn, self.column_oid)?;
        let old_cell: Self = Self::conn_get(&conn, self.table_oid, &column, self.row_oid)?;

        // 
        macro_rules! update_scalar {
            ($value:expr) => {
                sql_execute(
                    &conn, 
                    format!("UPDATE __TABLE{} SET COLUMN{} = ?1 WHERE OID = ?2", self.table_oid, self.column_oid), 
                    params![$value, self.row_oid]
                )?;
            };
        }
        match &self.content {
            TableCellContent::Boolean { value } => {
                update_scalar!(value);
            }
            TableCellContent::Integer { value }
            | TableCellContent::Object { value, .. }
            | TableCellContent::SingleSelectDropdown { value, .. } => {
                update_scalar!(value);
            }
            TableCellContent::Number { value } => {
                update_scalar!(value);
            }
            TableCellContent::Text { value, .. } => {
                // TODO check format
                update_scalar!(value);
            }
            TableCellContent::Date { label, .. }
            | TableCellContent::Datetime { label, .. } => {
                sql_execute(
                    &conn, 
                    format!("UPDATE __TABLE{} SET COLUMN{} = JULIANDAY(?1) WHERE OID = ?2", self.table_oid, self.column_oid), 
                    params![label, self.row_oid]
                )?;
            }
            TableCellContent::File { value } => {
                update_scalar!(if let Some(value) = value { Some(value.oid().clone()) } else { None });
            }
            TableCellContent::MultiSelectDropdown { table_oid, value } => {
                // Trash all previously-set values
                sql_execute(
                    &conn, 
                    format!(
                        "
UPDATE __MULTISELECT{} SET 
    TRASH = TRUE
WHERE TABLE{}_OID = ?1
                        ",
                        self.column_oid,
                        self.table_oid
                    ), 
                    params![self.row_oid]
                )?;
                // Upsert the new values
                for value in value {
                    sql_execute(
                        &conn, 
                        format!(
                            "
INSERT INTO __MULTISELECT{} (TABLE{table_oid}_OID, TABLE{}_OID) VALUES (?1, ?2)
ON CONFLICT SET TRASH = FALSE
                            ",
                            self.column_oid,
                            self.table_oid
                        ), 
                        params![value, self.row_oid]
                    )?;
                }
            }
            _ => {
                // Ignore virtual column
            }
        }

        // Return the old cell's value
        Ok(old_cell)
    }

    /// Sets the value of the cell in the database.
    pub fn set(&self) -> Result<Self, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Set the value of the cell in the database
        let old_cell = self.conn_set(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(old_cell)
    }

    /// Creates a new, blank Object.
    pub fn create_object(table_oid: i64, column_oid: i64, row_oid: i64, object_table_oid: i64) -> Result<Self, Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Insert new row in referenced table
        let mut master_rows: HashMap<i64, i64> = HashMap::new();
        let object_row_oid: i64 = TableRow::conn_insert(&trans, object_table_oid, None, &mut master_rows)?;

        // Create a cell for the inserted Object value
        let cell: Self = Self {
            table_oid,
            column_oid,
            row_oid,
            content: TableCellContent::Object { 
                table_oid: object_table_oid, 
                value: Some(object_row_oid) 
            }
        };

        // Set the value of the cell in the database
        let old_cell = cell.conn_set(&trans)?;

        // Commit the transaction
        trans.commit()?;
        Ok(old_cell)
    }
}

#[derive(Serialize, Clone)]
#[serde(rename_all="camelCase")]
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
            data.cells.push(TableCell::new(
                column_table_oid.clone(), 
                column, 
                format!("COLUMN{}", column.oid),
                if *column_table_oid == *table_oid {
                    data.oid.clone()
                } else {
                    let row_oid_ord: String = format!("MASTER{column_table_oid}_OID");
                    row.get::<&str, _>(&row_oid_ord)?
                }, 
                row
            )?);
        }

        Ok(data)
    }

    /// Gets a single row from the given table.
    pub fn get(table_oid: i64, row_oid: i64) -> Result<(Vec<(i64, TableColumnMetadata)>, Self), Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, table_oid, row_oid)
    }

    /// Gets a single row from the given table.
    /// Uses the given connection.
    pub fn conn_get(conn: &Connection, table_oid: i64, row_oid: i64) -> Result<(Vec<(i64, TableColumnMetadata)>, Self), Error> {
        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = TableColumnMetadata::conn_query_all(&conn, table_oid)?
            .into_iter().map(|(owning_table_oid, _, column)| (owning_table_oid, column)).collect();
        
        // Send the row of the table
        Ok((
            columns.clone(),
            sql_one(
                conn, 
                format!("SELECT ROW_NUMBER() AS ROW_INDEX, * FROM TABLE{table_oid} WHERE OID = ?1"), 
                params![row_oid], 
                |row| {
                    Self::new(row, &table_oid, &columns)
                }
            )?
        ))
    }

    /// Gets a single row from the given table.
    /// Takes polymorphism into account.
    pub fn get_object(table_oid: i64, row_oid: i64) -> Result<(i64, Vec<(i64, TableColumnMetadata)>, Self), Error> {
        let conn = db::open()?;

        // Query for object table and row
        let (table_oid, row_oid) = sql_one(
            &conn,
            format!("SELECT OBJECT_TABLE_OID, OBJECT_ROW_OID FROM OBJECT{table_oid} WHERE OID = ?1"),
            params![row_oid],
            |row| Ok((
                row.get::<_, i64>("OBJECT_TABLE_OID")?,
                row.get::<_, i64>("OBJECT_ROW_OID")?
            ))
        )?;
        // Query that row in the object table
        let (columns, row) = Self::conn_get(&conn, table_oid.clone(), row_oid)?;
        Ok((
            table_oid,
            columns,
            row
        ))
    }

    /// Queries for multiple rows from the given table.
    pub fn send(mut column_sender: Sender<(i64, TableColumnMetadata)>, mut row_sender: Sender<Self>, table_oid: i64) -> Result<(), Error> {
        let conn = db::open()?;

        // Send the columns of the table
        let columns: Vec<(i64, TableColumnMetadata)> = {
            let mut columns: Vec<(i64, TableColumnMetadata)> = Vec::new();
            for (owning_table_oid, _, column) in TableColumnMetadata::conn_query_all(&conn, table_oid)? {
                let tuple: (i64, TableColumnMetadata) = (owning_table_oid, column);
                column_sender.send(tuple.clone())?;
                columns.push(tuple);
            }
            columns
        };
        
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
                row_sender.send(payload)?;
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }


    /// Inserts a row into the given table.
    pub fn insert(table_oid: i64, row_oid: Option<i64>) -> Result<i64, Error> {
        // Start a transaction
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;

        // Insert the row into the table, + related rows for each master table
        let mut master_rows: HashMap<i64, i64> = HashMap::new();
        let row_oid: i64 = Self::conn_insert(&trans, table_oid, row_oid, &mut master_rows)?;

        // TODO fixed parent datasources

        // Commit the transaction
        trans.commit()?;
        Ok(row_oid)
    }

    /// Inserts a row into the given table.
    /// Uses the given connection.
    pub fn conn_insert(conn: &Connection, table_oid: i64, row_oid: Option<i64>, master_rows: &mut HashMap<i64, i64>) -> Result<i64, Error> {
        if let Some(row_oid) = master_rows.get(&table_oid) {
            let mut completed_table_oid: HashSet<i64> = HashSet::new();
            Self::conn_untrash(conn, table_oid, row_oid.clone(), &mut completed_table_oid)?;
            return Ok(row_oid.clone());
        }

        // Add a related row to every master table
        let mut cols: Vec<(String, String)> = Vec::new();
        sql_iter(
            conn,
            "
SELECT 
    MASTER_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE INHERITOR_TABLE_OID = ?1
            ",
            params![table_oid],
            |row| {
                let master_table_oid: i64 = row.get::<_, i64>(0)?;
                let master_table_row_oid: i64 = Self::conn_insert(
                    conn, 
                    master_table_oid, 
                    None, 
                    master_rows
                )?;

                cols.push((
                    format!("MASTER{master_table_oid}_OID"),
                    format!("{}", master_table_row_oid),
                ));
                Ok(None::<()>)
            }
        )?;

        // Query for any default values that need to be populated
        sql_iter(
            conn,
            "
SELECT c.OID, typ.DEFAULT_VALUE 
FROM METADATA_TABLE_COLUMN c
INNER JOIN METADATA_COLUMN_TYPE__PRIMITIVE typ ON typ.OID = c.TYPE_OID
WHERE c.TABLE_OID = ?1 
    AND typ.DEFAULT_VALUE IS NOT NULL 
            ",
            params![table_oid],
            |row| {
                let column_oid: i64 = row.get::<_, i64>("OID")?;
                let default_value: String = row.get::<_, String>("DEFAULT_VALUE")?;
                cols.push((format!("COLUMN{column_oid}"), default_value));
                Ok(None::<()>)
            }
        )?;

        // Handle insertion at a specific location in the table
        if let Some(o) = row_oid {
            // Make space for the new row at the designated OID
            sql_execute(
                conn, 
                format!(
                    "
UPDATE __TABLE{table_oid} SET 
    OID = -OID 
WHERE OID >= ?1
                    "
                ), 
                params![o]
            )?;
            sql_execute(
                conn, 
                format!(
                    "
UPDATE __TABLE{table_oid} SET 
    OID = 1 - OID 
WHERE OID < 0
                    "
                ), 
                []
            )?;

            // Add initial value for the OID
            cols.push((String::from("OID"), format!("{o}")));
        }

        // Compile the INSERT statement and execute
        let sql_insert_row_params: Vec<String> = cols
            .iter()
            .map(|(_, column_value)| column_value.clone())
            .collect();
        sql_execute(
            conn,
            format!(
                "INSERT INTO __TABLE{} {}",
                table_oid,
                if cols.len() == 0 {
                    String::from("DEFAULT VALUES")
                } else {
                    let (column_names, column_params) = cols.into_iter().enumerate().fold(
                        (String::from(""), String::from("")),
                        |(acc_column_names, acc_column_params), (e_idx, (e_column_name, _))| {
                            (
                                if acc_column_names == "" {
                                    e_column_name
                                } else {
                                    format!("{acc_column_names}, {e_column_name}")
                                },
                                if acc_column_params == "" {
                                    format!("?{}", e_idx + 1)
                                } else {
                                    format!("{acc_column_params}, ?{}", e_idx + 1)
                                },
                            )
                        },
                    );
                    format!("({column_names}) VALUES ({column_params})")
                }
            ),
            rusqlite::params_from_iter(sql_insert_row_params.into_iter()),
        )?;

        // Get the OID and add to the HashMap of master tables
        let row_oid: i64 = conn.last_insert_rowid();
        master_rows.insert(table_oid, row_oid);
        Ok(row_oid)
    }

    /// Sets the flag labelling a row for garbage collection.
    /// Returns the Object table OID and row OID before being trashed.
    pub fn trash(table_oid: i64, row_oid: i64) -> Result<Option<(i64, i64)>, Error> {
        // Start a transaction
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;

        // Trash the row + all related rows up and down the inheritance tree
        let mut completed_table_oid: HashSet<i64> = HashSet::new();
        let deepest_level_trashed_table_and_row: Option<(i64, i64)> =
            Self::conn_trash(&trans, table_oid, row_oid, &mut completed_table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(deepest_level_trashed_table_and_row)
    }

    /// Trashes a row, all of its master rows, and all of the rows inheriting from it.
    /// Returns the Object table OID and row OID before being trashed.
    fn conn_trash(conn: &Connection, table_oid: i64, row_oid: i64, completed_table_oid: &mut HashSet<i64>) -> Result<Option<(i64, i64)>, Error> {
        // Check if the row is already trashed
        if sql_one(
            conn,
            format!(
                "
SELECT 
    TRASH 
FROM __TABLE{table_oid} 
WHERE OID = ?1
                "
            ), 
            params![row_oid], 
            |row| row.get::<_, bool>("TRASH")
        )? {
            return Ok(None); // If it is already trashed, then all of its children should be trash, and its master rows can be handled elsewhere in the recursion tree
        }
        // Trash the row
        sql_execute(
            conn, 
            format!(
                "
UPDATE __TABLE{table_oid} SET 
    TRASH = TRUE 
WHERE OID = ?1
                "
            ), 
            params![row_oid]
        )?;

        // Trash upwards in the inheritance tree
        sql_map_then_iter(
            conn,
            "
SELECT 
    MASTER_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE INHERITOR_TABLE_OID = ?1
            ",
            params![table_oid],
            |row| row.get(0),
            |master_schema_oid| {
                if !completed_table_oid.contains(&master_schema_oid) {
                    completed_table_oid.insert(master_schema_oid);
                    let master_schema_row_oid: i64 = sql_one(
                        conn,
                        format!(
                            "
SELECT 
    MASTER{master_schema_oid}_OID 
FROM __TABLE{table_oid} 
WHERE OID = ?1
                            "
                        ), 
                        params![row_oid], 
                        |row| row.get(0)
                    )?;
                    Self::conn_trash(
                        conn,
                        master_schema_oid,
                        master_schema_row_oid,
                        completed_table_oid,
                    )?;
                }
                Ok(None::<()>)
            }
        )?;

        // Trash deeper in the inheritance tree
        if let Some(deepest_trashed_inheritor) = sql_map_then_iter(
            conn,
            "
SELECT 
    INHERITOR_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE MASTER_TABLE_OID = ?1
            ",
            params![table_oid],
            |row| row.get(0),
            |inheritor_schema_oid| {
                if !completed_table_oid.contains(&inheritor_schema_oid) {
                    completed_table_oid.insert(inheritor_schema_oid);
                    if let Some(inheritor_schema_row_oid) = sql_zero_or_one(
                        conn,
                        format!(
                            "
SELECT 
    OID 
FROM TABLE{inheritor_schema_oid} 
WHERE MASTER{table_oid}_OID = ?1
                            "
                        ), 
                        params![row_oid], 
                        |row| row.get(0)
                    )? {
                        // Stop iteration at the first inheritor table found to have been previously untrashed
                        if let Some(deepest_level_trashed_table_and_row) = Self::conn_trash(
                            conn,
                            inheritor_schema_oid,
                            inheritor_schema_row_oid,
                            completed_table_oid,
                        )? {
                            return Ok(Some(deepest_level_trashed_table_and_row));
                        }
                    }
                }
                Ok(None)
            }
        )? {
            Ok(Some(deepest_trashed_inheritor))
        } else {
            // If no inheritor schema was trashed, this is the deepest level that was trashed, so return (table_oid, row_oid)
            Ok(Some((table_oid, row_oid)))
        }
    }

    /// Unsets the flag labelling a row for garbage collection.
    pub fn untrash(table_oid: i64, row_oid: i64) -> Result<(), Error> {
        // Start a transaction
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;

        // Unset the TRASH flag for the row + every master row
        let mut completed_table_oid: HashSet<i64> = HashSet::new();
        Self::conn_untrash(&trans, table_oid, row_oid, &mut completed_table_oid)?;

        // Commit the transaction
        trans.commit()?;
        Ok(())
    }

    /// Untrashes a row and all of its master rows.
    fn conn_untrash(conn: &Connection, table_oid: i64, row_oid: i64, completed_table_oid: &mut HashSet<i64>) -> Result<(), Error> {
        // Untrash the row
        sql_execute(
            conn, 
            format!(
                "
UPDATE __TABLE{table_oid} SET 
    TRASH = FALSE 
WHERE OID = ?1
                "
            ), 
            params![row_oid]
        )?;

        // Untrash upwards in the inheritance tree
        sql_iter(
            conn,
            "
SELECT 
    MASTER_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE INHERITOR_TABLE_OID = ?1
            ",
            params![table_oid],
            |row| {
                let master_table_oid: i64 = row.get::<_, i64>(0)?;
                if !completed_table_oid.contains(&master_table_oid)
                {
                    completed_table_oid.insert(master_table_oid);
                    let master_table_row_oid: i64 = sql_one(
                        conn,
                        format!(
                            "
SELECT 
    MASTER{master_table_oid}_OID 
FROM TABLE{table_oid} 
WHERE OID = ?1
                            "
                        ), 
                        params![row_oid], 
                        |row| row.get(0)
                    )?;
                    Self::conn_untrash(
                        conn,
                        master_table_oid,
                        master_table_row_oid,
                        completed_table_oid,
                    )?;
                }
                Ok(None::<()>)
            }
        )?;
        Ok(())
    }

    /// Constructs a mapping of all associated rows in inheritor tables.
    fn map_all_inheritor_tables(
        conn: &Connection,
        table_oid: i64,
        row_oid: Option<i64>,
        mapped_table_oid: &mut HashMap<i64, Option<i64>>,
    ) -> Result<(usize, Option<i64>), Error> {
        if !mapped_table_oid.contains_key(&table_oid) {
            mapped_table_oid.insert(table_oid, row_oid);

            let mut deepest_level: usize = 0;
            let mut deepest_table_oid: Option<i64> = None;

            sql_map_then_iter(
                conn,
                "
SELECT 
    INHERITOR_TABLE_OID 
FROM METADATA_TABLE_INHERITANCE
WHERE MASTER_TABLE_OID = ?1
                ",
                params![table_oid],
                |row| row.get::<_, i64>(0),
                |inheritor_table_oid| {
                    if let Some(row_oid) = row_oid {
                        match sql_zero_or_one(
                            conn,
                            format!(
                                "
SELECT 
    OID, 
    TRASH 
FROM __TABLE{inheritor_table_oid} 
WHERE MASTER{table_oid}_OID = ?1
                                "
                            ), 
                            params![row_oid], 
                            |row| Ok((row.get::<_, i64>("OID")?, row.get::<_, bool>("TRASH")?))
                        )? {
                            Some((inheritor_row_oid, inheritor_row_is_trashed)) => {
                                // Map all inheritor tables of the inheritor table
                                let (deepest_mapped_level, deepest_mapped_table_oid) = Self::map_all_inheritor_tables(conn, inheritor_table_oid, Some(inheritor_row_oid), mapped_table_oid)?;
                                if !inheritor_row_is_trashed && deepest_mapped_level > deepest_level {
                                    deepest_level = deepest_mapped_level;
                                    deepest_table_oid = deepest_mapped_table_oid;
                                }
                            }
                            None => {
                                Self::map_all_inheritor_tables(conn, inheritor_table_oid, None, mapped_table_oid)?;        
                            }
                        }
                    } else {
                        Self::map_all_inheritor_tables(conn, inheritor_table_oid, None, mapped_table_oid)?;
                    }
                    Ok(None::<()>)
                }
            )?;
            return Ok((deepest_level + 1, deepest_table_oid));
        }
        Ok((0, None))
    }

    /// Change the object type of a row in a table.
    pub fn change_object_type(
        table_oid: i64,
        row_oid: i64,
        inheritor_table_oid: i64,
    ) -> Result<i64, Error> {
        // Start a transaction
        let mut conn = db::open()?;

        // Trash the row + all related rows up and down the inheritance tree
        let trans: Transaction = conn.transaction()?;
        let prior_object = {
            let mut completed_trash_table_oid: HashSet<i64> = HashSet::new();
            Self::conn_trash(
                &trans, 
                table_oid, 
                row_oid, 
                &mut completed_trash_table_oid
            )?
        };

        // Check whether a row already exists in the table for the new type
        if let Some(inheritor_row_oid) = sql_zero_or_one(
            &trans, 
            format!(
                "
SELECT 
    OID
FROM __TABLE{inheritor_table_oid}
WHERE MASTER{table_oid}_OID = ?1
                "
            ), 
            params![row_oid], 
            |row| row.get::<_, i64>("OID")
        )? {
            // If a row does already exist, untrash it
            let mut completed_untrash_table_oid: HashSet<i64> = HashSet::new();
            Self::conn_untrash(
                &trans,
                inheritor_table_oid,
                inheritor_row_oid.clone(),
                &mut completed_untrash_table_oid,
            )?;
        } else {
            // If a row does not already exist, create a new row associated with the known rows
            let mut master_rows: HashMap<i64, i64> = {
                let mut master_rows: HashMap<i64, Option<i64>> = HashMap::new();
                Self::map_all_inheritor_tables(&trans, table_oid, Some(row_oid), &mut master_rows)?;
                HashMap::from_iter(master_rows.into_iter().filter_map(|(table_oid, row_oid)| {
                    if let Some(row_oid) = row_oid {
                        Some((table_oid, row_oid))
                    } else {
                        None
                    }
                }))
            };
            Self::conn_insert(&trans, inheritor_table_oid, None, &mut master_rows)?;
        }

        // Commit the transaction
        trans.commit()?;
        Ok(prior_object.unwrap_or((table_oid, row_oid)).0)
    }

    /// Reorders a row in a table.
    pub fn reorder(table_oid: i64, row_oid: i64, new_row_oid: Option<i64>) -> Result<i64, Error> {
        // Start a transaction
        let mut conn = db::open()?;
        let trans: Transaction = conn.transaction()?;

        let new_row_oid: i64 = match new_row_oid {
            Some(new_row_oid) => {
                // Make room for the row OID
                sql_execute(
                    &trans, 
                    format!(
                        "
UPDATE __TABLE{table_oid} SET 
    OID = -OID 
WHERE OID >= ?1 
    AND OID != ?2
                        "
                    ), 
                    params![new_row_oid, row_oid]
                )?;

                // Change the row OID
                sql_execute(
                    &trans, 
                    format!(
                        "
UPDATE __TABLE{table_oid} SET 
    OID = ?1 
WHERE OID = ?2
                        "
                    ), 
                    params![new_row_oid, row_oid]
                )?;

                // Move back the other row OIDs
                sql_execute(
                    &trans, 
                    format!(
                        "
UPDATE __TABLE{table_oid} SET 
    OID = 1 - OID 
WHERE OID < 0
                        "), 
                        []
                )?;

                new_row_oid
            }
            None => {
                // Query for the next OID
                let new_row_oid: i64 = sql_zero_or_one(
                    &trans,
                    format!(
                        "
SELECT 
    COALESCE(MAX(OID), 0) + 1 
FROM __TABLE{table_oid}
                        "
                    ), 
                    [], 
                    |row| row.get::<_, i64>(0)
                )?.unwrap_or(1);

                // Change the row OID
                sql_execute(
                    &trans, 
                    format!(
                        "
UPDATE __TABLE{table_oid} SET 
    OID = ?1 
WHERE OID = ?2
                        "
                    ), 
                    params![new_row_oid, row_oid]
                )?;

                new_row_oid
            }
        };

        // Commit the transaction
        trans.commit()?;
        Ok(new_row_oid)
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