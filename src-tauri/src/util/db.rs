use crate::backend::table_data;
use crate::util::error;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{
    params, Connection, DropBehavior, Params, Result, Row, Transaction, TransactionBehavior,
};
use std::any::Any;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

static DATABASE_PATH: Mutex<Option<String>> = Mutex::new(None);

/// Data structure locking access to the database while a function performs an action.
pub struct DbAction<'a> {
    conn: Connection,
    pub trans: Transaction<'a>,
}

impl DbAction<'_> {
    /// Convenience method to execute a query that returns multiple rows, then execute a function for each row.
    pub fn query_iterate<P: Params, F: FnMut(&Row<'_>) -> Result<(), error::Error>>(
        &self,
        sql: &str,
        p: P,
        f: &mut F,
    ) -> Result<(), error::Error> {
        // Prepare a statement
        let mut stmt = match self.trans.prepare(sql) {
            Ok(s) => s,
            Err(e) => {
                return Err(error::Error::RusqliteError(e));
            }
        };

        // Execute the statement to query rows
        let mut rows = stmt.query(p)?;
        loop {
            let row = match rows.next()? {
                Some(r) => r,
                None => {
                    break;
                }
            };
            f(row);
        }
        return Ok(());
    }
}

/// Applies the metadata schema to the database at the given path.
fn setup_db_at_path<P: AsRef<Path>>(path: P) -> Result<(), error::Error> {
    if path.as_ref().exists() {
        return Ok(());
    }

    let conn = Connection::open(path)?;
    conn.execute_batch("
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;

    BEGIN;

    -- METADATA_DATASOURCE stores all possible data sources for a report.
    CREATE TABLE IF NOT EXISTS METADATA_DATASOURCE (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE
    );

    

    -- METADATA_SCHEMA is associated with all column definitions.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        NAME TEXT NOT NULL
    );
    
    -- METADATA_TABLE stores all user-defined schemas that store data.
    -- A table can additionally be associated with storage types.
    CREATE TABLE IF NOT EXISTS METADATA_TABLE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_REPORT stores all user-defined schemas that do not store data, but rather pull data from other schemas.
    -- A report can only be associated with virtual columns.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_REPORT_DATASOURCE stores the datasources associated with a report.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT_DATASOURCE (
        REPORT_OID INTEGER REFERENCES METADATA_REPORT (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        DATASOURCE_OID INTEGER REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
    );



    -- METADATA_COLUMN_TYPE stores all column types.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- METADATA_COLUMN_TYPE__FORMULA stores all Formula column types.
    -- Formulas are a virtual column that can be part of any schema.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__FORMULA (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FORMULA TEXT NOT NULL
    );

    -- METADATA_COLUMN_TYPE__SUBREPORT stores all user-defined sub-report types.
    -- Subreports are a virtual column that can be part of any schema.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__SUBREPORT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        REPORT_OID INTEGER NOT NULL REFERENCES METADATA_REPORT (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_COLUMN_TYPE__PRIMITIVE stores all Primitive column types.
    -- Primitives are a storage column with a primitive type (e.g. number, text, file).
    -- Primitive types are predefined with negative OIDs to prevent conflict with user-defined types.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__PRIMITIVE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        MODE TEXT NOT NULL
    );

    -- METADATA_COLUMN_TYPE__OBJECT stores all Object column types.
    -- Objects are a storage column that represent an injective relationship to at most one row in another table.
    -- This is represented in UI by a clickable link with the primary key of the linked row.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__OBJECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );

    -- METADATA_COLUMN_TYPE__SELECT stores all Select column types.
    -- Selects are a storage column that selects a single row from another table.
    -- This is represented in UI by a dropdown.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__SELECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );

    -- METADATA_COLUMN_TYPE__MULTISELECT stores all Multiselect column types.
    -- Multiselects are a storage column that selects multiple rows from another table.
    -- This is represented in UI by a checkbox dropdown.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__MULTISELECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );



    -- METADATA_PARAMETER stores all parameters to a user-defined report
    CREATE TABLE IF NOT EXISTS METADATA_PARAMETER (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_COLUMN stores all columns of user-defined data types
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        HIDDEN BOOLEAN NOT NULL DEFAULT FALSE,
        SCHEMA_OID INTEGER NOT NULL REFERENCES METADATA_SCHEMA (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        NAME TEXT NOT NULL,
        TYPE_OID INTEGER NOT NULL DEFAULT -1 REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE SET DEFAULT,
        COLUMN_CSS_STYLE TEXT NOT NULL,
            -- Column CSS style
        COLUMN_ORDERING INTEGER NOT NULL,
            -- The ordering of columns as displayed in the table
        IS_NULLABLE BOOLEAN NOT NULL DEFAULT TRUE,
        IS_UNIQUE BOOLEAN NOT NULL DEFAULT FALSE,
        IS_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE,
        DEFAULT_VALUE ANY
    );

    -- METADATA_TABLE_INHERITANCE records the inheritance of columns between tables.
    CREATE TABLE IF NOT EXISTS METADATA_TABLE_INHERITANCE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        INHERITOR_TABLE_OID INTEGER REFERENCES METADATA_TABLE (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
        MASTER_TABLE_OID INTEGER REFERENCES METADATA_TABLE (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE
    );

    -- METADATA_PARAMETER_CHAIN stores adhoc parameters that link a row of a base table to [a column in] another table through some form of reference
    -- [Select] column: N-to-1
    -- [Multiselect] column: N-to-M
    -- [Object] column: 1-to-1
    -- Inheritance: 1-to-1
    CREATE TABLE IF NOT EXISTS METADATA_PARAMETER_CHAIN (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        REF_PARAMETER_OID INTEGER NOT NULL REFERENCES METADATA_PARAMETER (OID) 
            ON UPDATE CASCADE,
        DEF_PARAMETER_OID INTEGER REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
    );

    

    -- METADATA_SCHEMA_ORDERBY stores what parameters (if any) the schema is sorted by, in what order, and in what direction.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA_ORDERBY (
        COLUMN_OID INTEGER NOT NULL REFERENCES METADATA_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        SORT_ORDERING INTEGER NOT NULL DEFAULT 0,
        SORT_ASCENDING BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- METADATA_REPORT_GROUPBY stores what parameters (if any) the report is aggregated over.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT_GROUPBY (
        COLUMN_OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- METADATA_REPORT_FILTER stores what filters are applied to the report.
    -- A filter takes the form of a boolean formula that is evaluated for each row in the report.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT_FILTER (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        FORMULA TEXT NOT NULL
    );
    


    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-1);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-1, 'text');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-2);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-2, 'int');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-3);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-3, 'number');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-4);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-4, 'checkbox');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-5);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-5, 'date');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-6);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-6, 'datetime');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-7);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-7, 'file');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-8);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-8, 'image');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-9);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-9, 'JSON');

    COMMIT;
    ")?;
    return Ok(());
}

/// Closes any previous database connection, and opens a new one.
pub fn init(path: String) -> Result<(), error::Error> {
    // Initialize the database if it did not already exist
    setup_db_at_path(&path)?;

    // Record the path to static variable
    let mut database_path = DATABASE_PATH.lock().unwrap();
    *database_path = Some(path);
    return Ok(());
}

/// Opens a connection to the database.
pub fn open() -> Result<Connection, error::Error> {
    let database_path = DATABASE_PATH.lock().unwrap();
    match *database_path {
        Some(ref path) => {
            let conn = Connection::open(path)?;
            conn.execute_batch(
                "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            ",
            )?;
            return Ok(conn);
        }
        None => {
            return Err(error::Error::AdhocError("No file is open!"));
        }
    }
}
