use std::{Option, None};
use std::sync::{Mutex};
use rusqlite::{params, Connection, Transaction, TransactionBehavior, DropBehavior, Result};

/// Initializes a new database at the given path.
fn initialize_new_db<P: AsRef<Path>>(path: P) -> Result<()> {
    let conn: Connection = Connection::open(path);
    conn.execute("PRAGMA foreign_keys = ON;");
    conn.execute("PRAGMA journal_mode = WAL;");
    conn.execute_batch("
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
        NAME TEXT NOT NULL DEFAULT ('Table' || TO_STRING(OID)),
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
        NAME TEXT NOT NULL DEFAULT ('COLUMN' || TO_STRING(OID)),
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
    ")?;

    Ok(());
}


static current_db_connection: Mutex<Connection> = Mutex::new();
static current_db_transaction: Mutex<Transaction> = Mutex::new();
static current_db_transaction_last_savepoint_id: Mutex<u32> = Mutex::new(0);

/// Closes any previous database connection, and opens a new one.
pub fn init<P: AsRef<Path>>(path: P) -> Result<()> {
    // Initialize the database if it did not already exist
    if !path.exists() {
        initialize_new_db(path);
    }

    // Open a connection to the database
    let mut conn = current_db_connection.lock().unwrap();
    *conn = Connection::open(path);
    *conn.execute("PRAGMA foreign_keys = ON;")?;
    *conn.execute("PRAGMA journal_mode = WAL;")?;

    // Start the transaction, configure it to update database immediately and to commit if the connection is dropped
    let mut tx = current_db_transaction.lock().unwrap();
    *tx = *conn.transaction_with_behavior(TransactionBehavior::IMMEDIATE)?;
    *tx.set_drop_behavior(DropBehavior::COMMIT)?;

    Ok(());
}

fn create_savepoint() -> Result<()> {
    // Create a savepoint
    let mut tx = current_db_transaction.lock().unwrap();
    let mut savepoint_id = current_db_transaction_last_savepoint_id.lock().unwrap();
    *savepoint_id = *savepoint_id + 1;
    *tx.execute(
        "SAVEPOINT ?1;",
        params![String::from("save") + *savepoint_id.to_string()]
    );
}

/// Undoes the last action performed.
pub fn undo() -> Result<()> {
    let mut savepoint_id = current_db_transaction_last_savepoint_id.lock().unwrap();
    if *savepoint_id > 0 {
        // Rollback to the last savepoint
        let mut tx = current_db_transaction.lock().unwrap();
        *tx.execute(
            "ROLLBACK TO SAVEPOINT ?1;",
            params![String::from("save") + *savepoint_id.to_string()]
        );
        *savepoint_id = *savepoint_id - 1;
    }
    // If savepoint_id = 0, do nothing because the edit stack is empty
    Ok(());
}

/// Creates a new table.
pub fn create_table(name: &str) -> Result<i64> {
    create_savepoint()?;
    
    let mut tx = current_db_transaction.lock().unwrap();
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (3);");
    let table_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, NAME) VALUES (?1, ?2);",
        params![table_id, String::from(name)]
    );
    let create_cmd: String = String::from("CREATE TABLE TABLE") + table_id.to_string() + String::from(" (OID INTEGER PRIMARY KEY);");
    *tx.execute(&create_cmd);
    Ok(table_id);
}

/// Inserts a column into a table, such that the column initially has a globally-accessible type.
/// A type is considered globally-accessible if it can be reused across other columns.
/// Examples include primitives, references to another table, and child objects.
pub fn insert_table_column_globally_accessible_type(table_id: i64, column_name: &str, column_type_id: i64, column_ordering: i64, is_nullable: bool, is_unique: bool) -> Result<i64> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Shift every column to the right of this one over by 1
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE TABLE_OID = ?1 AND COLUMN_ORDERING >= ?2;",
        params![table_id, column_ordering]
    )?;
    // Insert the new column into the metadata
    *tx.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME, TYPE_OID, COLUMN_ORDERING, IS_NULLABLE, IS_UNIQUE) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        params![table_id, column_name, column_type_id, column_ordering, if is_nullable { 1 } else { 0 }, if is_unique { 1 } else { 0 }]
    )?;
    // Insert the new column into the data table
    let column_id: i64 = *tx.last_insert_rowid();
    let alter_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
        + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
        + String::from(" ") + match column_type_id {
            1 => String::from("TINYINT"),
            2 => String::from("INTEGER"),
            3 => String::from("FLOAT"),
            4 => String::from("DATE"),
            5 => String::from("DATETIME"),
            6 => String::from("TEXT"),
            7 => String::from("TEXT"),
            8 => String::from("BLOB"),
            9 => String::from("BLOB"),
            _ => String::from("INTEGER REFERENCES TABLE") + column_type_id.to_string() + String::from(" (OID) ON UPDATE CASCADE ON DELETE SET NULL")
        } + String::from(";");
    *tx.execute(&alter_cmd)?;
    Ok(column_id);
}

/// Inserts a "column" into the table, such that the "column" is a child table.
/// I am putting "column" in scare-quotes because the "column" is only present in the metadata and is not in the actual table itself.
pub fn insert_table_column_child_table_type(table_id: i64, column_name: &str, column_ordering: i64, is_nullable: bool, is_unique: bool) -> Result<(i64, i64)> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Shift every column to the right of this one over by 1
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE TABLE_OID = ?1 AND COLUMN_ORDERING >= ?2;",
        params![table_id, column_ordering]
    )?;
    // Create new table to serve as child table
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (5);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (OID INTEGER PRIMARY KEY, _PARENTID_ INTEGER, FOREIGN KEY (_PARENTID_) REFERENCES TABLE") + table_id.to_string() + String::from(" (OID));");
    *tx.execute(&create_cmd)?;
    // Insert the new column into the metadata
    *tx.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME, TYPE_OID, COLUMN_ORDERING, IS_NULLABLE, IS_UNIQUE) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        params![table_id, column_name, column_type_id, column_ordering, if is_nullable { 1 } else { 0 }, if is_unique { 1 } else { 0 }]
    )?;
    // Get the ID of the new column
    let column_id: i64 = *tx.last_insert_rowid();
    Ok((column_id, column_type_id));
}

/// Inserts a column into the table, such that the column references a hidden table containing text values.
/// The column will be displayed as a single-select dropdown.
pub fn insert_table_column_singleselect_type(table_id: i64, column_name: &str, column_ordering: i64, is_nullable: bool) -> Result<(i64, i64)> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Shift every column to the right of this one over by 1
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE TABLE_OID = ?1 AND COLUMN_ORDERING >= ?2;",
        params![table_id, column_ordering]
    )?;
    // Create new table to serve as table of values for the single-select dropdown
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (1);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (VALUE TEXT UNIQUE ON CONFLICT IGNORE NOT NULL);");
    *tx.execute(&create_cmd)?;
    // Insert the new column into the metadata
    *tx.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME, TYPE_OID, COLUMN_ORDERING, IS_NULLABLE, IS_UNIQUE) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        params![table_id, column_name, column_type_id, column_ordering, if is_nullable { 1 } else { 0 }, 0]
    )?;
    // Insert the new column into the data table
    let column_id: i64 = *tx.last_insert_rowid();
    let alter_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
        + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
        + String::from(" TEXT REFERENCES TABLE") + column_type_id.to_string() 
        + String::from(" (VALUE) ON UPDATE CASCADE ON DELETE SET NULL;");
    *tx.execute(&alter_cmd)?;
    Ok((column_id, column_type_id));
}

/// Inserts a "column" into the table, such that the "column" references any number of text values in a hidden table.
/// The "column" will be displayed as a multi-select dropdown.
/// I am putting "column" in scare-quotes because the "column" is only present in the metadata and is not in the actual table itself.
pub fn insert_table_column_multiselect_type(table_id: i64, column_name: &str, column_ordering: i64, is_nullable: bool, is_unique: bool) -> Result<(i64, i64)> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Shift every column to the right of this one over by 1
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET COLUMN_ORDERING = COLUMN_ORDERING + 1 WHERE TABLE_OID = ?1 AND COLUMN_ORDERING >= ?2;",
        params![table_id, column_ordering]
    )?;
    // Create new table to serve as table of values for the multi-select dropdown
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (2);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (VALUE TEXT UNIQUE ON CONFLICT IGNORE NOT NULL);");
    *tx.execute(&create_cmd)?;
    // Create new table to serve as many-to-many relationship between multi-select dropdown values and rows of the data table
    let create2_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() 
        + String::from("_SELECTIONS (
            OID INTEGER, 
            VALUE TEXT, 
            PRIMARY KEY (OID, VALUE), 
            FOREIGN KEY (OID) REFERENCES TABLE") + table_id.to_string() 
                + String::from(" (OID) ON UPDATE CASCADE ON DELETE CASCADE, 
            FOREIGN KEY TABLE") + column_type_id.to_string()
                + String::from(" (VALUE) REFERENCES TABLE (VALUE) ON UPDATE CASCADE ON DELETE CASCADE);");
    *tx.execute(&create2_cmd)?;
    // Insert the new column into the metadata
    *tx.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME, TYPE_OID, COLUMN_ORDERING, IS_NULLABLE, IS_UNIQUE) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        params![table_id, column_name, column_type_id, column_ordering, if is_nullable { 1 } else { 0 }, 0]
    )?;
    // Get the ID of the new column
    let column_id: i64 = *tx.last_insert_rowid();
    Ok((column_id, column_type_id));
}

/// Modifies a column in a table, such that the new type of the column is globally-accessible.
/// A type is considered globally-accessible if it can be reused across other columns.
/// Examples include primitives, references to another table, and child objects.
pub fn modify_table_column_globally_accessible_type(table_id: i64, column_id: i64, column_type_id: i64) -> Result<()> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Load the old type ID and mode
    let (column_old_type_id, column_old_type_mode) = *tx.query_one(
        "SELECT OID, MODE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = (SELECT TYPE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1);",
        params![column_id],
        |row| (row.get(0), row.get(1))
    )?;
    // Create a temporary table to store the column data
    *tx.execute("
    CREATE TEMPORARY TABLE DATA_HOLDER STRICT (
        OID INTEGER PRIMARY KEY, 
        CONTENT ANY
    );")?;
    // Copy the column data into the temporary table
    let copy_cmd: String = String::from("INSERT INTO temp.DATA_HOLDER (OID, CONTENT) SELECT OID, COLUMN") + column_id.to_string() + String::from(" FROM main.TABLE") + table_id.to_string() + String::from(";");
    *tx.execute(&copy_cmd);
    // Drop the old column
    let drop_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() + String::from(" DROP COLUMN COLUMN") + column_id.to_string() + String::from(";");
    *tx.execute(&drop_cmd);
    // Add the column back in, this time with the new type
    let add_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
        + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
        + String::from(" ") + match column_type_id {
            1 => String::from("TINYINT"),
            2 => String::from("INTEGER"),
            3 => String::from("FLOAT"),
            4 => String::from("DATE"),
            5 => String::from("DATETIME"),
            6 => String::from("TEXT"),
            7 => String::from("TEXT"),
            8 => String::from("BLOB"),
            9 => String::from("BLOB"),
            _ => String::from("INTEGER REFERENCES TABLE") + column_type_id.to_string() + String::from(" (OID) ON UPDATE CASCADE ON DELETE SET NULL")
        } + String::from(";");
    *tx.execute(&add_cmd)?;
    // Copy the data back over to the original table
    let update_cmd: String = String::from("UPDATE OR IGNORE main.TABLE") + table_id.to_string() + String::from(" AS u SET u.COLUMN") + column_id.to_string() + String::from(" = (SELECT t.CONTENT FROM temp.DATA_HOLDER AS t WHERE t.OID = u.OID);");
    *tx.execute(&update_cmd)?;
    // Drop the temporary table
    *tx.execute("DROP TABLE temp.DATA_HOLDER;")?;

    // Drop the old type, if necessary
    match column_old_type_mode {
        1 => {
            // Delete the old single-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd);
        },
        2 => {
            // Delete the old multi-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd);
            // Delete the old *-to-* relationship table
            let drop3_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from("_SELECTIONS;");
            *tx.execute(&drop3_cmd);
        },
        5 => {
            // Delete the old child table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd);
        }
    }

    Ok(());
}


/// Modifies a column in a table, such that the new type of the column is globally-accessible.
/// A type is considered globally-accessible if it can be reused across other columns.
/// Examples include primitives, references to another table, and child objects.
pub fn modify_table_column_globally_accessible_type(table_id: i64, column_id: i64, column_type_id: i64) -> Result<()> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    // Load the old type ID and mode
    let (column_old_type_id, column_old_type_mode) = *tx.query_one(
        "SELECT OID, MODE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = (SELECT TYPE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1);",
        params![column_id],
        |row| (row.get(0), row.get(1))
    )?;

    // Update the type of the column in the metadata
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET TYPE_OID = ?1 WHERE OID = ?2;",
        params![column_type_id, column_id]
    )?;

    if column_old_type_mode == 0 || column_old_type_mode == 1 || column_old_type_mode == 3 {
        // Try to copy over the old values

        // Create a temporary table to store the column data
        *tx.execute("
        CREATE TEMPORARY TABLE DATA_HOLDER STRICT (
            OID INTEGER PRIMARY KEY, 
            CONTENT ANY
        );")?;

        // Check if the old column references a table with a surrogate key
        let old_surrogate_key_column_id_nullable: Option<i64> = if column_old_type_mode == 3 {
            *tx.query_one(
                "SELECT SURROGATE_KEY_COLUMN_OID FROM METADATA_TABLE WHERE OID = ?1",
                params![column_old_type_id],
                |row| row.get(0)
            ).optional()?;
        } else { None };
        // Copy the column data into the temporary table
        match old_surrogate_key_column_id_nullable {
            Some(old_surrogate_key_column_id) {
                // Copy data from the referenced table's surrogate key into the temporary table
                let copy_cmd: String = String::from("INSERT INTO temp.DATA_HOLDER (OID, CONTENT) SELECT t1.OID, t2.COLUMN") + old_surrogate_key_column_id.to_string() + String::from(" FROM main.TABLE") + table_id.to_string() + String::from(" t1 INNER JOIN main.TABLE") + column_old_type_id.to_string() + String::from(" t2 ON t2.OID = t1.COLUMN") + column_id.to_string() + String::from(";");
                *tx.execute(&copy_cmd)?;
            },
            None {
                // Copy the raw data from the old column into the temporary table
                let copy_cmd: String = String::from("INSERT INTO temp.DATA_HOLDER (OID, CONTENT) SELECT OID, COLUMN") + column_id.to_string() + String::from(" FROM main.TABLE") + table_id.to_string() + String::from(";");
                *tx.execute(&copy_cmd)?;
            }
        }
        
        // Drop the old column
        let drop_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() + String::from(" DROP COLUMN COLUMN") + column_id.to_string() + String::from(";");
        *tx.execute(&drop_cmd)?;
        // Add the column back in, this time with the new type
        let add_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
            + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
            + String::from(" ") + match column_type_id {
                1 => String::from("TINYINT"),
                2 => String::from("INTEGER"),
                3 => String::from("FLOAT"),
                4 => String::from("DATE"),
                5 => String::from("DATETIME"),
                6 => String::from("TEXT"),
                7 => String::from("TEXT"),
                8 => String::from("BLOB"),
                9 => String::from("BLOB"),
                _ => String::from("INTEGER REFERENCES TABLE") + column_type_id.to_string() + String::from(" (OID) ON UPDATE CASCADE ON DELETE SET NULL")
            } + String::from(";");
        *tx.execute(&add_cmd)?;

        // Check if the new column references a table with a surrogate key
        let new_surrogate_key_column_id_nullable: Option<i64> = *tx.query_one(
            "SELECT SURROGATE_KEY_COLUMN_OID FROM METADATA_TABLE WHERE OID = ?1",
            params![column_type_id],
            |row| row.get(0)
        ).optional()?;
        // Copy the values back over from the temporary table
        match new_surrogate_key_column_id_nullable {
            Some(new_surrogate_key_column_id) {
                // Copy the values back over, attempting to match the value in the table with the referenced table's surrogate key
                let update_cmd: String = String::from("UPDATE OR IGNORE main.TABLE") + table_id.to_string() + String::from(" AS u SET u.COLUMN") + column_id.to_string() + String::from(" = (SELECT v.OID FROM temp.DATA_HOLDER AS t INNER JOIN main.TABLE") + column_type_id.to_string() + String::from(" v ON v.COLUMN") + new_surrogate_key_column_id.to_string() + String::from(" = t.CONTENT WHERE t.OID = u.OID);");
                *tx.execute(&update_cmd)?;
            },
            None {
                // Copy the values back over, raw
                let update_cmd: String = String::from("UPDATE OR IGNORE main.TABLE") + table_id.to_string() + String::from(" AS u SET u.COLUMN") + column_id.to_string() + String::from(" = (SELECT t.CONTENT FROM temp.DATA_HOLDER AS t WHERE t.OID = u.OID);");
                *tx.execute(&update_cmd)?;
            }
        }
        // Drop the temporary table
        *tx.execute("DROP TABLE temp.DATA_HOLDER;")?;
    } else {
        // Completely discard the old values

        // Drop the old column
        let drop_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() + String::from(" DROP COLUMN COLUMN") + column_id.to_string() + String::from(";");
        *tx.execute(&drop_cmd)?;
        // Add the column back in, this time with the new type
        let add_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
            + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
            + String::from(" ") + match column_type_id {
                1 => String::from("TINYINT"),
                2 => String::from("INTEGER"),
                3 => String::from("FLOAT"),
                4 => String::from("DATE"),
                5 => String::from("DATETIME"),
                6 => String::from("TEXT"),
                7 => String::from("TEXT"),
                8 => String::from("BLOB"),
                9 => String::from("BLOB"),
                _ => String::from("INTEGER REFERENCES TABLE") + column_type_id.to_string() + String::from(" (OID) ON UPDATE CASCADE ON DELETE SET NULL")
            } + String::from(";");
        *tx.execute(&add_cmd)?;
    }

    // Drop the old type, if necessary
    match column_old_type_mode {
        1 => {
            // Delete the old single-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        },
        2 => {
            // Delete the old multi-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
            // Delete the old *-to-* relationship table
            let drop3_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from("_SELECTIONS;");
            *tx.execute(&drop3_cmd)?;
        },
        5 => {
            // Delete the old child table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        }
    }

    Ok(());
}

/// Modifies a "column" in the table, such that the "column" is a child table.
/// I am putting "column" in scare-quotes because the "column" is only present in the metadata and is not in the actual table itself.
pub fn modify_table_column_child_table_type(table_id: i64, column_id: i64) -> Result<i64> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    
    // Load the old type ID and mode
    let (column_old_type_id, column_old_type_mode) = *tx.query_one(
        "SELECT OID, MODE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = (SELECT TYPE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1);",
        params![column_id],
        |row| (row.get(0), row.get(1))
    )?;

    // Create new table to serve as child table
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (5);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (OID INTEGER PRIMARY KEY, _PARENTID_ INTEGER, FOREIGN KEY (_PARENTID_) REFERENCES TABLE") + table_id.to_string() + String::from(" (OID));");
    *tx.execute(&create_cmd)?;
    // Update the type of the column in the metadata
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET TYPE_OID = ?1 WHERE OID = ?2;",
        params![column_type_id, column_id]
    )?;

    // Drop the old type, if necessary
    match column_old_type_mode {
        1 => {
            // Delete the old single-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        },
        2 => {
            // Delete the old multi-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
            // Delete the old *-to-* relationship table
            let drop3_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from("_SELECTIONS;");
            *tx.execute(&drop3_cmd)?;
        },
        5 => {
            // Delete the old child table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        }
    }

    Ok(column_type_id);
}

/// Modifies a column into the table, such that the column references a hidden table containing text values.
/// The column will be displayed as a single-select dropdown.
pub fn modify_table_column_singleselect_type(table_id: i64, column_id: i64) -> Result<i64> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    
    // Load the old type ID and mode
    let (column_old_type_id, column_old_type_mode) = *tx.query_one(
        "SELECT OID, MODE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = (SELECT TYPE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1);",
        params![column_id],
        |row| (row.get(0), row.get(1))
    )?;

    // Create new table to serve as table of values for the single-select dropdown
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (1);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (VALUE TEXT UNIQUE ON CONFLICT IGNORE NOT NULL);");
    *tx.execute(&create_cmd)?;
    // Insert the new column into the metadata
    *tx.execute(
        "INSERT INTO METADATA_TABLE_COLUMN (TABLE_OID, NAME, TYPE_OID, COLUMN_ORDERING, IS_NULLABLE, IS_UNIQUE) VALUES (?1, ?2, ?3, ?4, ?5, ?6);",
        params![table_id, column_name, column_type_id, column_ordering, if is_nullable { 1 } else { 0 }, 0]
    )?;
    // Insert the new column into the data table
    let alter_cmd: String = String::from("ALTER TABLE TABLE") + table_id.to_string() 
        + String::from(" ADD COLUMN COLUMN") + column_id.to_string() 
        + String::from(" TEXT REFERENCES TABLE") + column_type_id.to_string() 
        + String::from(" (VALUE) ON UPDATE CASCADE ON DELETE SET NULL;");
    *tx.execute(&alter_cmd)?;

    // Update the type of the column in the metadata
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET TYPE_OID = ?1 WHERE OID = ?2;",
        params![column_type_id, column_id]
    )?;

    // Drop the old type, if necessary
    match column_old_type_mode {
        1 => {
            // Delete the old single-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        },
        2 => {
            // Delete the old multi-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
            // Delete the old *-to-* relationship table
            let drop3_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from("_SELECTIONS;");
            *tx.execute(&drop3_cmd)?;
        },
        5 => {
            // Delete the old child table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        }
    }

    Ok(column_type_id);
}

/// Modifies a "column" into the table, such that the "column" references any number of text values in a hidden table.
/// The "column" will be displayed as a multi-select dropdown.
/// I am putting "column" in scare-quotes because the "column" is only present in the metadata and is not in the actual table itself.
pub fn modify_table_column_multiselect_type(table_id: i64, column_id: i64) -> Result<i64> {
    create_savepoint()?;

    let mut tx = current_db_transaction.lock().unwrap();
    
    // Load the old type ID and mode
    let (column_old_type_id, column_old_type_mode) = *tx.query_one(
        "SELECT OID, MODE FROM METADATA_TABLE_COLUMN_TYPE WHERE OID = (SELECT TYPE_OID FROM METADATA_TABLE_COLUMN WHERE OID = ?1);",
        params![column_id],
        |row| (row.get(0), row.get(1))
    )?;

    // Create new table to serve as table of values for the multi-select dropdown
    *tx.execute("INSERT INTO METADATA_TABLE_COLUMN_TYPE (MODE) VALUES (2);")?;
    let column_type_id: i64 = *tx.last_insert_rowid();
    *tx.execute(
        "INSERT INTO METADATA_TABLE (OID, PARENT_OID) VALUES (?1, ?2);",
        params![column_type_id, table_id]
    )?;
    let create_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() + String::from(" (VALUE TEXT UNIQUE ON CONFLICT IGNORE NOT NULL);");
    *tx.execute(&create_cmd)?;
    // Create new table to serve as many-to-many relationship between multi-select dropdown values and rows of the data table
    let create2_cmd: String = String::from("CREATE TABLE TABLE") + column_type_id.to_string() 
        + String::from("_SELECTIONS (
            OID INTEGER, 
            VALUE TEXT, 
            PRIMARY KEY (OID, VALUE), 
            FOREIGN KEY (OID) REFERENCES TABLE") + table_id.to_string() 
                + String::from(" (OID) ON UPDATE CASCADE ON DELETE CASCADE, 
            FOREIGN KEY TABLE") + column_type_id.to_string()
                + String::from(" (VALUE) REFERENCES TABLE (VALUE) ON UPDATE CASCADE ON DELETE CASCADE);");
    *tx.execute(&create2_cmd)?;
    // Update the type of the column in the metadata
    *tx.execute(
        "UPDATE METADATA_TABLE_COLUMN SET TYPE_OID = ?1 WHERE OID = ?2;",
        params![column_type_id, column_id]
    )?;

    // Drop the old type, if necessary
    match column_old_type_mode {
        1 => {
            // Delete the old single-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        },
        2 => {
            // Delete the old multi-select value table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
            // Delete the old *-to-* relationship table
            let drop3_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from("_SELECTIONS;");
            *tx.execute(&drop3_cmd)?;
        },
        5 => {
            // Delete the old child table
            let drop2_cmd: String = String::from("DROP TABLE TABLE") + column_old_type_id.to_string() + String::from(";");
            *tx.execute(&drop2_cmd)?;
        }
    }

    Ok(column_type_id);
}