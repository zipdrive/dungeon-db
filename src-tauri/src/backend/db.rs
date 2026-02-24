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

/// Initializes a new database at the given path.
fn initialize_new_db_at_path<P: AsRef<Path>>(path: P) -> Result<(), error::Error> {
    if path.as_ref().exists() {
        return Ok(());
    }

    let conn = Connection::open(path)?;
    conn.execute_batch("
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;

    BEGIN;

    -- METADATA_DATASOURCE stores all possible data sources for a report.
    CREATE TABLE METADATA_DATASOURCE (
        OID INTEGER PRIMARY KEY
    );

    -- METADATA_TYPE stores all pre-defined and user-defined data types
    CREATE TABLE METADATA_TYPE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        MODE INTEGER NOT NULL DEFAULT 0 
            -- Modes are:
            -- 0 = primitive
            -- 1 = adhoc single-select dropdown
            -- 2 = adhoc multi-select dropdown
            -- 3 = reference to independent table
            -- 4 = child object
            -- 5 = child table
    );

    -- Any primitive type? Always null? Shouldn't be used, regardless
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (0);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (0, 0);

    -- Boolean primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-1);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-1, 0);

    -- Integer primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-2);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-2, 0);

    -- Number primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-3);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-3, 0);

    -- Date primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-4);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-4, 0);

    -- Timestamp primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-5);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-5, 0);

    -- Text primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-6);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-6, 0);

    -- Text primitive (forced to be JSON)
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-7);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-7, 0);

    -- BLOB primitive
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-8);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-8, 0);

    -- BLOB primitive (but this one is displayed as an image)
    INSERT INTO METADATA_DATASOURCE (OID) VALUES (-9);
    INSERT INTO METADATA_TYPE (OID, MODE) VALUES (-9, 0);
    

    -- METADATA_PARAMETER stores all parameters to a user-defined report
    CREATE TABLE METADATA_PARAMETER (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_TABLE stores all user-defined tables and object types
    CREATE TABLE METADATA_TABLE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_TYPE (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        NAME TEXT NOT NULL
    );
    ALTER TABLE METADATA_TABLE ADD COLUMN PARENT_TABLE_OID INTEGER 
        REFERENCES METADATA_TABLE (OID) 
            ON UPDATE CASCADE;

    -- METADATA_TABLE_INHERITANCE stores inheritance of columns from another table
    CREATE TABLE METADATA_TABLE_INHERITANCE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        INHERITOR_TABLE_OID INTEGER REFERENCES METADATA_TABLE (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
        MASTER_TABLE_OID INTEGER REFERENCES METADATA_TABLE (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0
    );

    -- METADATA_TABLE_COLUMN stores all columns of user-defined tables and data types
    CREATE TABLE METADATA_TABLE_COLUMN (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        NAME TEXT NOT NULL DEFAULT 'Column',
        TYPE_OID INTEGER NOT NULL DEFAULT 8 REFERENCES METADATA_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE SET DEFAULT,
        COLUMN_CSS_STYLE TEXT DEFAULT 'width: 100;',
            -- Column CSS style, applied via colgroup
        COLUMN_ORDERING INTEGER NOT NULL DEFAULT 0,
            -- The ordering of columns as displayed in the table
        IS_NULLABLE TINYINT NOT NULL DEFAULT 1,
        IS_UNIQUE TINYINT NOT NULL DEFAULT 0,
        IS_PRIMARY_KEY TINYINT NOT NULL DEFAULT 0,
        DEFAULT_VALUE ANY
    );

    -- METADATA_PARAMETER__CHAIN stores adhoc parameters that link a row of a base table to [a column in] another table through some form of reference
    -- [Reference] column: N-to-1
    -- [Object] column: 1-to-1
    -- [Table] column: 1-to-N
    -- Inheritance: 1-to-1
    CREATE TABLE METADATA_PARAMETER__CHAIN (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        REF_PARAMETER_OID INTEGER NOT NULL REFERENCES METADATA_PARAMETER (OID) 
            ON UPDATE CASCADE,
        DEF_PARAMETER_OID INTEGER REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
    );

    -- METADATA_RPT stores all user-defined reports
    CREATE TABLE METADATA_RPT (
        OID INTEGER PRIMARY KEY
    );

    -- METADATA_RPT_DATASOURCE stores all datasources of a report
    CREATE TABLE METADATA_RPT_DATASOURCE (
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        RPT_OID INTEGER REFERENCES METADATA_RPT (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        DATASOURCE_OID INTEGER REFERENCES METADATA_DATASOURCE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        PRIMARY KEY (RPT_OID, DATASOURCE_OID)
    );

    -- METADATA_RPT__REPORT stores all user-defined reports
    CREATE TABLE METADATA_RPT__REPORT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_RPT (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        NAME TEXT NOT NULL
    );

    -- METADATA_RPT_COLUMN stores all columns of user-defined reports
    CREATE TABLE METADATA_RPT_COLUMN (
        OID INTEGER PRIMARY KEY,
        RPT_OID INTEGER NOT NULL REFERENCES METADATA_RPT (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT 0,
        NAME TEXT NOT NULL,
        CSS_COLUMN_STYLE TEXT DEFAULT 'width: 100;',
        COLUMN_ORDERING INTEGER NOT NULL DEFAULT 0
    );
    -- METADATA_RPT_COLUMN__FORMULA
    CREATE TABLE METADATA_RPT_COLUMN__FORMULA (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_RPT_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FORMULA TEXT NOT NULL
    );
    -- METADATA_RPT_COLUMN__SUBREPORT
    CREATE TABLE METADATA_RPT_COLUMN__SUBREPORT (
        RPT_COLUMN_OID INTEGER PRIMARY KEY REFERENCES METADATA_RPT_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        RPT_OID INTEGER NOT NULL REFERENCES METADATA_RPT (OID)
            ON UPDATE CASCADE
    );
    
    -- METADATA_RPT_GROUPBY stores what parameters (if any) the report is aggregated over
    CREATE TABLE METADATA_RPT_GROUPBY (
        OID INTEGER PRIMARY KEY,
        RPT_OID INTEGER NOT NULL REFERENCES METADATA_RPT (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        PARAMETER_OID INTEGER NOT NULL REFERENCES METADATA_PARAMETER (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );
    
    -- METADATA_RPT_ORDERBY stores what columns the report is sorted by, in what order and what direction
    CREATE TABLE METADATA_RPT_ORDERBY (
        RPT_COLUMN_OID INTEGER NOT NULL REFERENCES METADATA_RPT_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        SORT_ORDERING INTEGER NOT NULL DEFAULT 0,
        SORT_ASCENDING BOOLEAN NOT NULL DEFAULT 0
    );

    COMMIT;
    ")?;
    return Ok(());
}

/// Closes any previous database connection, and opens a new one.
pub fn init(path: String) -> Result<(), error::Error> {
    // Initialize the database if it did not already exist
    initialize_new_db_at_path(&path)?;

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

/// Convenience method to execute a query that returns multiple rows, then execute a function for each row.
pub fn query_iterate<P: Params, F: FnMut(&Row<'_>) -> Result<(), error::Error>>(
    trans: &Transaction,
    sql: &str,
    p: P,
    f: &mut F,
) -> Result<(), error::Error> {
    // Prepare a statement
    let mut stmt = match trans.prepare(sql) {
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
