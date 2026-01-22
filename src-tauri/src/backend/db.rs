use std::path::{Path};
use std::sync::{Mutex,MutexGuard};
use rusqlite::{Connection, DropBehavior, Result, Transaction, TransactionBehavior, params};
use crate::util::error;

static SAVEPOINT_ID: Mutex<i64> = Mutex::new(0);
static mut GLOBAL_CONNECTION: Option<Connection> = None;
static mut GLOBAL_TRANSACTION: Option<Transaction<'static>> = None;

/// Data structure locking access to the database while a function performs an action.
pub struct DbAction<'a> {
    trans: &'a mut Transaction<'a>,
    savepoint_id: MutexGuard<'a, i64>
}

/// Initializes a new database at the given path.
fn initialize_new_db_at_path<P: AsRef<Path>>(path: P) -> Result<(), error::Error> {
    let conn_result = Connection::open(path);
    let conn: Connection = match conn_result {
        Ok(c) => c,
        Err(e) => {
            return Err(error::Error::RusqliteError(e));
        }
    };
    let init_script_result = conn.execute_batch("
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;

    BEGIN;

    -- __METADATA_TYPE stores all pre-defined and user-defined data types
    CREATE TABLE METADATA_TABLE_COLUMN_TYPE (
        OID INTEGER PRIMARY KEY,
        MODE INTEGER NOT NULL DEFAULT 0 
            -- Modes are:
            -- 0 = primitive
            -- 1 = adhoc single-select dropdown
            -- 2 = adhoc multi-select dropdown
            -- 3 = reference to independent table
            -- 4 = child object
            -- 5 = child table
    );
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (0, 0); -- Always null
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (1, 0); -- Boolean
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (2, 0); -- Integer
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (3, 0); -- Number
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (4, 0); -- Date
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (5, 0); -- Timestamp
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (6, 0); -- Text
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (7, 0); -- Text (JSON)
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (8, 0); -- BLOB
    INSERT INTO METADATA_TABLE_COLUMN_TYPE (OID, MODE) VALUES (9, 0); -- BLOB (displayed as image thumbnail)

    -- METADATA_TABLE stores all user-defined tables and data types
    CREATE TABLE METADATA_TABLE (
        OID INTEGER PRIMARY KEY,
        PARENT_OID INTEGER,
        NAME TEXT NOT NULL DEFAULT 'UnnamedTable',
        FOREIGN KEY (OID) REFERENCES METADATA_TABLE_COLUMN_TYPE (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FOREIGN KEY (PARENT_OID) REFERENCES METADATA_TABLE(OID) 
            ON UPDATE CASCADE
            ON DELETE SET NULL
    );

    -- METADATA_TABLE_COLUMN stores all columns of user-defined tables and data types
    CREATE TABLE METADATA_TABLE_COLUMN (
        OID INTEGER PRIMARY KEY,
        TABLE_OID INTEGER NOT NULL,
        NAME TEXT NOT NULL DEFAULT 'Column',
        TYPE_OID INTEGER NOT NULL DEFAULT 8,
        COLUMN_WIDTH INTEGER NOT NULL DEFAULT 100,
            -- Column width, as measured in pixels
        COLUMN_ORDERING INTEGER NOT NULL DEFAULT 0,
            -- The ordering of columns as displayed in the table
        IS_NULLABLE TINYINT NOT NULL DEFAULT 1,
        IS_UNIQUE TINYINT NOT NULL DEFAULT 0,
        IS_PRIMARY_KEY TINYINT NOT NULL DEFAULT 0,
        DEFAULT_VALUE ANY,
        FOREIGN KEY (TABLE_OID) REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FOREIGN KEY (TYPE_OID) REFERENCES METADATA_TABLE_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE SET DEFAULT
    );

    -- Surrogate key is displayed by references
    -- Each table has at most one surrogate key
    ALTER TABLE METADATA_TABLE ADD COLUMN SURROGATE_KEY_COLUMN_OID INTEGER REFERENCES METADATA_TABLE_COLUMN (OID);

    COMMIT;
    ");
    match init_script_result {
        Ok(_) => {
            return Ok(());
        },
        Err(e) => {
            return Err(error::Error::RusqliteError(e));
        }
    }
}

/// Closes any previous database connection, and opens a new one.
pub fn init<P: AsRef<Path>>(path: P) -> Result<(), error::Error> {
    // Initialize the database if it did not already exist
    if !path.as_ref().exists() {
        initialize_new_db_at_path(&path)?;
    }

    unsafe {
        // Obtain lock
        let mut savepoint_id = SAVEPOINT_ID.lock().unwrap();

        // Open a connection to the database
        GLOBAL_CONNECTION = Some(match Connection::open(&path) {
            Ok(conn) => conn,
            Err(e) => {
                return Err(error::Error::RusqliteError(e));
            }
        });
        match &mut GLOBAL_CONNECTION {
            Some(conn) => {
                // Do commands to set up the necessary pragmas for the entire connection
                match conn.execute_batch("PRAGMA foreign_keys = ON;PRAGMA journal_mode = WAL;") {
                    Ok(_) => {},
                    Err(e) => { return Err(error::Error::RusqliteError(e)); }
                };

                // Start the transaction that will serve as the undo stack
                GLOBAL_TRANSACTION = Some(match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
                    Ok(trans) => trans,
                    Err(e) => {
                        return Err(error::Error::RusqliteError(e));
                    }
                });
            },
            None => {
                return Err(error::Error::AdhocError("GLOBAL_CONNECTION found to be None immediately following initialization."));
            }
        }

        match &mut GLOBAL_TRANSACTION {
            Some(trans) => {
                // Set the behavior of the transaction to commit if the transaction is dropped
                trans.set_drop_behavior(DropBehavior::Commit);
            },
            None => {
                return Err(error::Error::AdhocError("GLOBAL_TRANSACTION found to be None immediately following initialziation."));
            }
        }

        *savepoint_id = 0;
    }

    return Ok(());
}

/// Starts a new action.
pub fn begin_db_action() -> Result<DbAction<'static>, error::Error> {
    unsafe {
        // Obtain lock
        let mut savepoint_id = SAVEPOINT_ID.lock().unwrap();

        match &mut GLOBAL_TRANSACTION {
            Some(trans) => {
                // Create a savepoint
                match trans.execute(
                    "SAVEPOINT ?1;",
                    params![format!("save{}", *savepoint_id + 1)]
                ) {
                    Ok(_) => {
                        *savepoint_id += 1;
                        return Ok(DbAction {
                            trans,
                            savepoint_id: savepoint_id
                        });
                    },
                    Err(e) => {
                        return Err(error::Error::RusqliteError(e));
                    }
                }
            },
            None => {
                return Err(error::Error::AdhocError("Database connection has not been opened."));
            }
        }
    }
}

/// Undoes the last action performed.
pub fn undo_db_action() -> Result<(), error::Error> {
    unsafe {
        // Obtain lock
        let mut savepoint_id = SAVEPOINT_ID.lock().unwrap();
        // Check if there exists an action to undo
        if *savepoint_id > 0 {
            match &mut GLOBAL_TRANSACTION {
                Some(trans) => {
                    // Create a savepoint
                    match trans.execute(
                        "ROLLBACK TO SAVEPOINT ?1;",
                        params![format!("save{}", *savepoint_id)]
                    ) {
                        Ok(_) => {
                            *savepoint_id -= 1;
                        },
                        Err(e) => {
                            return Err(error::Error::RusqliteError(e));
                        }
                    }
                },
                None => {
                    return Err(error::Error::AdhocError("Database connection has not been opened."))
                }
            }
        }
    }
    return Ok(());
}