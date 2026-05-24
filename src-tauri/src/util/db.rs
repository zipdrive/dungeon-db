use crate::util::error;
use rusqlite::fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{
    params, Connection, DropBehavior, Params, Result, Row, Transaction, TransactionBehavior,
};
use tauri::AppHandle;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use tempfile::NamedTempFile;

static DATABASE_PATH: Mutex<Option<String>> = Mutex::new(None);
static DATABASE_AUTOSAVE_PATH: Mutex<Option<NamedTempFile>> = Mutex::new(None);

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
    let conn = Connection::open(path)?;
    conn.execute_batch("
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;

    BEGIN;

    -- METADATA_SCHEMA is associated with all column definitions.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        NAME TEXT NOT NULL
    );

    -- METADATA_SCHEMA_INHERITANCE records the inheritance of columns between tables.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA_INHERITANCE (
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        INHERITOR_SCHEMA_OID INTEGER REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
        MASTER_SCHEMA_OID INTEGER REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE 
            ON DELETE CASCADE,
        PRIMARY KEY (MASTER_SCHEMA_OID, INHERITOR_SCHEMA_OID)
    );
    CREATE INDEX IF NOT EXISTS METADATA_SCHEMA_INHERITANCE_INDEX_BY_INHERITOR_SCHEMA_OID ON METADATA_SCHEMA_INHERITANCE (INHERITOR_SCHEMA_OID);

    -- METADATA_SCHEMA_INHERITANCE_VIEW is a view that filters out trashed schema inheritance relationships.
    CREATE VIEW IF NOT EXISTS METADATA_SCHEMA_INHERITANCE_VIEW AS 
        SELECT
            inh.INHERITOR_SCHEMA_OID,
            inh.MASTER_SCHEMA_OID
        FROM METADATA_SCHEMA_INHERITANCE inh 
        INNER JOIN METADATA_SCHEMA m ON m.OID = inh.MASTER_SCHEMA_OID 
        INNER JOIN METADATA_SCHEMA i ON i.OID = inh.INHERITOR_SCHEMA_OID
        WHERE NOT inh.TRASH 
            AND NOT m.TRASH 
            AND NOT i.TRASH
    ;

    -- METADATA_SCHEMA_INHERITANCE_PATH_VIEW is a view that flattens the inheritance hierarchy.
    CREATE VIEW IF NOT EXISTS METADATA_SCHEMA_INHERITANCE_PATH_VIEW AS 
        WITH RECURSIVE FLATTENING (INHERITOR_SCHEMA_OID, MASTER_SCHEMA_OID, INHERITOR_DATASOURCE_PATH, MASTER_DATASOURCE_PATH, DEPTH) AS (
            SELECT
                u.INHERITOR_SCHEMA_OID,
                u.MASTER_SCHEMA_OID,
                '_INHERITOR' || u.INHERITOR_SCHEMA_OID INHERITOR_DATASOURCE_PATH,
                '_MASTER' || u.MASTER_SCHEMA_OID MASTER_DATASOURCE_PATH,
                1 DEPTH
            FROM METADATA_SCHEMA_INHERITANCE_VIEW u

            UNION

            SELECT
                u.INHERITOR_SCHEMA_OID,
                s.MASTER_SCHEMA_OID,
                s.INHERITOR_DATASOURCE_PATH || '_INHERITOR' || u.INHERITOR_SCHEMA_OID INHERITOR_DATASOURCE_PATH,
                '_MASTER' || u.MASTER_SCHEMA_OID || s.MASTER_DATASOURCE_PATH MASTER_DATASOURCE_PATH,
                s.DEPTH + 1 DEPTH
            FROM FLATTENING s
            INNER JOIN METADATA_SCHEMA_INHERITANCE_VIEW u ON u.MASTER_SCHEMA_OID = s.INHERITOR_SCHEMA_OID
        )
        SELECT 
            INHERITOR_SCHEMA_OID,
            MASTER_SCHEMA_OID,
            MIN(INHERITOR_DATASOURCE_PATH) INHERITOR_DATASOURCE_PATH,
            MIN(MASTER_DATASOURCE_PATH) MASTER_DATASOURCE_PATH,
            MAX(DEPTH) MAX_DEPTH  
        FROM FLATTENING
        GROUP BY INHERITOR_SCHEMA_OID, MASTER_SCHEMA_OID
    ;

    -- METADATA_SCHEMA_VALIDATION represents a validation performed on a schema.
    -- A validation takes the form of a boolean validation formula that is evaluated for each row in the schema,
    -- and a text message formula which is the error message displayed if the validation formula returns FALSE.
    -- The error message will be displayed on the row's index.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA_VALIDATION (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        SCHEMA_OID INTEGER NOT NULL REFERENCES METADATA_SCHEMA (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        VALIDATION_FORMULA TEXT NOT NULL,
        MESSAGE_FORMULA TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS METADATA_SCHEMA_VALIDATION_INDEX_BY_SCHEMA_OID ON METADATA_SCHEMA_VALIDATION (SCHEMA_OID);
    
    -- METADATA_TABLE stores all user-defined schemas that store data.
    -- A table can additionally be associated with storage types.
    CREATE TABLE IF NOT EXISTS METADATA_TABLE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );

    -- METADATA_REPORT stores all user-defined schemas that do not store data, but rather pull data from one or more tables (and/or array literals?).
    -- A report can only be associated with virtual columns.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_SCHEMA (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FILTER_FORMULA TEXT
    );



    -- METADATA_FILE stores all files.
    CREATE TABLE IF NOT EXISTS METADATA_FILE (
        OID INTEGER PRIMARY KEY
    );

    -- METADATA_FILE__PATH stores all files that are a reference to a file on the local filesystem.
    CREATE TABLE IF NOT EXISTS METADATA_FILE__PATH (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_FILE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FILEPATH TEXT NOT NULL
    );

    -- METADATA_FILE__BLOB stores all files stored inside the database as BLOBs.
    CREATE TABLE IF NOT EXISTS METADATA_FILE__BLOB (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_FILE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FILENAME TEXT NOT NULL,
        CONTENT BLOB NOT NULL
    );
    
    -- METADATA_FILE_VIEW constructs a label for each file.
    CREATE VIEW IF NOT EXISTS METADATA_FILE_VIEW AS 
        SELECT
            OID,
            FILENAME || ' (' || CASE 
                WHEN CONTENT IS NULL THEN NULL 
                WHEN LENGTH(CONTENT) > 1000000000 THEN FORMAT('%.1f GB', LENGTH(CONTENT) * 0.000000001)
                WHEN LENGTH(CONTENT) > 1000000 THEN FORMAT('%.1f MB', LENGTH(CONTENT) * 0.000001)
                ELSE FORMAT('%.1f KB', LENGTH(CONTENT) * 0.001)
            END || ')' AS LABEL
        FROM METADATA_FILE__BLOB
        
        UNION ALL
        
        SELECT
            OID,
            FILEPATH AS LABEL
        FROM METADATA_FILE__PATH
    ;



    -- METADATA_COLUMN_TYPE stores all column types.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- METADATA_COLUMN_TYPE__FORMULA stores all Formula column types.
    -- Formulas are a virtual column that can be part of any schema.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__FORMULA (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        FORMULA TEXT NOT NULL
    );

    -- METADATA_COLUMN_TYPE__SUBREPORT stores all user-defined sub-report types.
    -- Subreports are a virtual column that can be part of any schema.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__SUBREPORT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        REPORT_OID INTEGER NOT NULL REFERENCES METADATA_REPORT (OID) 
            ON UPDATE CASCADE
            ON DELETE CASCADE
    );
    CREATE UNIQUE INDEX IF NOT EXISTS METADATA_COLUMN_TYPE__SUBREPORT_INDEX_BY_REPORT_OID ON METADATA_COLUMN_TYPE__SUBREPORT (REPORT_OID);

    -- METADATA_COLUMN_TYPE__PRIMITIVE stores all Primitive column types.
    -- Primitives are a storage column with a primitive type (e.g. number, text, file).
    -- Primitive types are predefined with negative OIDs to prevent conflict with user-defined types.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__PRIMITIVE (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        MODE TEXT NOT NULL
    );

    -- METADATA_COLUMN_TYPE__OBJECT stores all Object column types.
    -- Objects are a storage column that represent an injective relationship to at most one row in another table.
    -- This is represented in UI by a clickable link with the primary key of the linked row.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__OBJECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );
    CREATE UNIQUE INDEX IF NOT EXISTS METADATA_COLUMN_TYPE__OBJECT_INDEX_BY_TABLE_OID ON METADATA_COLUMN_TYPE__OBJECT (TABLE_OID);

    -- METADATA_COLUMN_TYPE__SELECT stores all Select column types.
    -- Selects are a storage column that selects a single row from another table.
    -- This is represented in UI by a dropdown.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__SELECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );
    CREATE UNIQUE INDEX IF NOT EXISTS METADATA_COLUMN_TYPE__SELECT_INDEX_BY_TABLE_OID ON METADATA_COLUMN_TYPE__SELECT (TABLE_OID);

    -- METADATA_COLUMN_TYPE__MULTISELECT stores all Multiselect column types.
    -- Multiselects are a storage column that selects multiple rows from another table.
    -- This is represented in UI by a checkbox dropdown.
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN_TYPE__MULTISELECT (
        OID INTEGER PRIMARY KEY REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
    );

    -- METADATA_COLUMN_TYPE_VIEW filters out all trashed column types.
    CREATE VIEW IF NOT EXISTS METADATA_COLUMN_TYPE_VIEW AS 
        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__FORMULA ctf ON ctf.OID = ct.OID 
        WHERE NOT ct.TRASH 

        UNION ALL 

        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__SUBREPORT cts ON cts.OID = ct.OID 
        INNER JOIN METADATA_SCHEMA s ON s.OID = cts.REPORT_OID 
        WHERE NOT ct.TRASH AND NOT s.TRASH

        UNION ALL 

        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__PRIMITIVE ctp ON ctp.OID = ct.OID 
        WHERE NOT ct.TRASH 

        UNION ALL 

        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__OBJECT cto ON cto.OID = ct.OID 
        INNER JOIN METADATA_SCHEMA s ON s.OID = cto.TABLE_OID 
        WHERE NOT ct.TRASH AND NOT s.TRASH
        
        UNION ALL 

        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__SELECT cto ON cto.OID = ct.OID 
        INNER JOIN METADATA_SCHEMA s ON s.OID = cto.TABLE_OID 
        WHERE NOT ct.TRASH AND NOT s.TRASH
        
        UNION ALL 

        SELECT ct.OID 
        FROM METADATA_COLUMN_TYPE ct 
        INNER JOIN METADATA_COLUMN_TYPE__MULTISELECT cto ON cto.OID = ct.OID 
        INNER JOIN METADATA_SCHEMA s ON s.OID = cto.TABLE_OID 
        WHERE NOT ct.TRASH AND NOT s.TRASH
    ;


    -- METADATA_COLUMN stores all columns of user-defined data types
    CREATE TABLE IF NOT EXISTS METADATA_COLUMN (
        OID INTEGER PRIMARY KEY,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        HIDDEN BOOLEAN NOT NULL DEFAULT FALSE,
        SCHEMA_OID INTEGER NOT NULL REFERENCES METADATA_SCHEMA (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        NAME TEXT NOT NULL,
        TYPE_OID INTEGER NOT NULL DEFAULT -1 REFERENCES METADATA_COLUMN_TYPE (OID)
            ON UPDATE CASCADE
            ON DELETE SET DEFAULT,
        STYLE TEXT NOT NULL,
            -- Column CSS style
        ORDERING INTEGER NOT NULL,
            -- The ordering of columns as displayed in the table
        IS_NULLABLE BOOLEAN NOT NULL DEFAULT TRUE,
        IS_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE,
        DEFAULT_VALUE TEXT
    );
    CREATE INDEX IF NOT EXISTS METADATA_COLUMN_INDEX_BY_SCHEMA_OID ON METADATA_COLUMN (SCHEMA_OID);

    -- METADATA_COLUMN_VIEW filters out all trashed columns.
    CREATE VIEW IF NOT EXISTS METADATA_COLUMN_VIEW AS 
        SELECT 
            c.OID,
            c.HIDDEN,
            c.SCHEMA_OID,
            c.NAME,
            c.TYPE_OID,
            c.STYLE,
            c.ORDERING,
            c.IS_NULLABLE,
            c.IS_PRIMARY_KEY,
            c.DEFAULT_VALUE 
        FROM METADATA_COLUMN c 
        INNER JOIN METADATA_SCHEMA s ON s.OID = c.SCHEMA_OID 
        INNER JOIN METADATA_COLUMN_TYPE_VIEW ct ON ct.OID = c.TYPE_OID 
        WHERE NOT c.TRASH AND NOT s.TRASH
    ;

    -- METADATA_SCHEMA_COLUMN_VIEW is a view that lists the columns of a schema.
    CREATE VIEW IF NOT EXISTS METADATA_SCHEMA_COLUMN_VIEW AS 
        SELECT
            c.SCHEMA_OID,
            '' DATASOURCE_PATH,
            c.OID COLUMN_OID,
            TRUE IS_REQUIRED
        FROM METADATA_COLUMN_VIEW c
        
        UNION ALL
        
        SELECT
            inh.INHERITOR_SCHEMA_OID SCHEMA_OID,
            inh.MASTER_DATASOURCE_PATH DATASOURCE_PATH,
            c.OID COLUMN_OID,
            TRUE IS_REQUIRED
        FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW inh
        INNER JOIN METADATA_COLUMN_VIEW c ON c.SCHEMA_OID = inh.MASTER_SCHEMA_OID

        UNION ALL

        SELECT
            inh.MASTER_SCHEMA_OID SCHEMA_OID,
            inh.INHERITOR_DATASOURCE_PATH DATASOURCE_PATH,
            c.OID COLUMN_OID,
            FALSE IS_REQUIRED
        FROM METADATA_SCHEMA_INHERITANCE_PATH_VIEW inh
        INNER JOIN METADATA_COLUMN_VIEW c ON c.SCHEMA_OID = inh.INHERITOR_SCHEMA_OID
    ;


    -- METADATA_DATASOURCE stores root datasources for a schema.
    CREATE TABLE IF NOT EXISTS METADATA_DATASOURCE (
        OID INTEGER PRIMARY KEY,
        TABLE_OID INTEGER NOT NULL REFERENCES METADATA_TABLE (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        LABEL TEXT
    );
    CREATE INDEX IF NOT EXISTS METADATA_DATASOURCE_INDEX_BY_TABLE_OID ON METADATA_DATASOURCE (TABLE_OID);
    


    -- METADATA_SCHEMA_ORDERBY stores what columns (if any) the schema is sorted by, in what order, and in what direction.
    CREATE TABLE IF NOT EXISTS METADATA_SCHEMA_ORDERBY (
        SCHEMA_OID INTEGER REFERENCES METADATA_SCHEMA (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        COLUMN_OID INTEGER REFERENCES METADATA_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        ORDERING INTEGER NOT NULL DEFAULT 0,
        SORT_ASCENDING BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (SCHEMA_OID, COLUMN_OID)
    );

    -- METADATA_SCHEMA_ORDERBY_VIEW is a view that filters out any trashed METADATA_SCHEMA_ORDERBY rows.
    CREATE VIEW IF NOT EXISTS METADATA_SCHEMA_ORDERBY_VIEW AS
        SELECT 
            so.SCHEMA_OID,
            sc.DATASOURCE_PATH,
            so.COLUMN_OID,
            so.ORDERING,
            so.SORT_ASCENDING
        FROM METADATA_SCHEMA_ORDERBY so 
        INNER JOIN METADATA_SCHEMA_COLUMN_VIEW sc 
            ON sc.SCHEMA_OID = so.SCHEMA_OID
                AND sc.COLUMN_OID = so.COLUMN_OID
        WHERE NOT so.TRASH
    ;



    -- METADATA_REPORT_GROUPBY stores what columns (if any) the report is aggregated over.
    CREATE TABLE IF NOT EXISTS METADATA_REPORT_GROUPBY (
        REPORT_OID INTEGER REFERENCES METADATA_REPORT (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        COLUMN_OID INTEGER REFERENCES METADATA_COLUMN (OID)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
        TRASH BOOLEAN NOT NULL DEFAULT FALSE,
        PRIMARY KEY (REPORT_OID, COLUMN_OID)
    );

    -- METADATA_REPORT_GROUPBY_VIEW is a view that filters out any trashed METADATA_REPORT_GROUPBY rows.
    CREATE VIEW IF NOT EXISTS METADATA_REPORT_GROUPBY_VIEW AS
        SELECT 
            rg.REPORT_OID,
            rg.COLUMN_OID
        FROM METADATA_REPORT_GROUPBY rg
        INNER JOIN METADATA_SCHEMA_COLUMN_VIEW sc
            ON sc.SCHEMA_OID = rg.REPORT_OID
                AND sc.COLUMN_OID = rg.COLUMN_OID
        WHERE NOT rg.TRASH
    ;
    


    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-1);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-1, 'text');
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE (OID) VALUES (-2);
    INSERT OR IGNORE INTO METADATA_COLUMN_TYPE__PRIMITIVE (OID, MODE) VALUES (-2, 'integer');
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

/// Closes any previous database connection, and opens 
pub fn init_new() -> Result<(), error::Error> {
    // Reset static variables
    let mut database_path = DATABASE_PATH.lock().unwrap();
    let mut database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    *database_path = None;
    *database_autosave_tempfile = None;

    // Create new autosave file
    let Ok(tempfile) = NamedTempFile::new() else {
        return Err(error::Error::AdhocError("Unable to make an autosave file."));
    };

    // Initialize the database at the path
    setup_db_at_path(tempfile.path())?;

    // Transfer ownership of the tempfile to the static variable
    *database_autosave_tempfile = Some(tempfile);

    Ok(())
}

/// Closes any previous database connection, and opens a new one.
pub fn init_existing(path: String) -> Result<(), error::Error> {
    // Reset static variables
    let mut database_path = DATABASE_PATH.lock().unwrap();
    let mut database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    *database_path = None;
    *database_autosave_tempfile = None;

    // Make a new autosave file
    let Ok(tempfile) = NamedTempFile::new() else {
        return Err(error::Error::AdhocError("Unable to make an autosave file."));
    };

    // Copy the data over from the main file to the autosave
    let Ok(_) = fs::copy(&path, tempfile.path()) else {
        return Err(error::Error::AdhocError("Unable to transfer contents of autosave to main file."));
    };
    setup_db_at_path(tempfile.path())?;

    // Record the path to static variable
    *database_path = Some(path);
    // Transfer ownership of the tempfile to the static variable
    *database_autosave_tempfile = Some(tempfile);

    Ok(())
}

/// Opens a connection to the database.
pub fn open() -> Result<Connection, error::Error> {
    let database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    match *database_autosave_tempfile {
        Some(ref tempfile) => {
            let conn = Connection::open(tempfile.path())?;
            conn.execute_batch(
                "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            ",
            )?;
            rusqlite::vtab::array::load_module(&conn)?;
            return Ok(conn);
        }
        None => {
            return Err(error::Error::AdhocError("No file is open!"));
        }
    }
}

/// Copies the data from the autosave file to the main file, then open a connection to the main file for cleaning purposes.
pub fn save(app: &AppHandle) -> Result<(), error::Error> {
    let mut database_path = DATABASE_PATH.lock().unwrap();
    let database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    match *database_autosave_tempfile {
        Some(ref tempfile) => {
            let save_path: &String = match *database_path {
                Some(ref path) => path,
                None => {
                    use tauri_plugin_dialog::DialogExt;

                    if let Some(file_path) = app.dialog().file().add_filter("DungeonDB File", &["db"]).blocking_save_file() {
                        *database_path = Some(file_path.to_string());
                        (database_path.as_ref()).unwrap()
                    } else {
                        return Ok(());
                    }
                }
            };

            // Copy the data from the autosave back to the main file
            let Ok(_) = fs::copy(tempfile.path(), save_path) else {
                return Err(error::Error::AdhocError("Unable to transfer contents of autosave to main file."));
            };

            // Open connection to the main file
            let mut conn = Connection::open(save_path)?;
            conn.execute_batch(
                "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            ",
            )?;
            rusqlite::vtab::array::load_module(&conn)?;

            // Start transaction to clean database
            let trans = conn.transaction()?;

            // Delete data columns
            for row_result in trans.prepare("SELECT c.SCHEMA_OID, c.OID AS COLUMN_OID FROM METADATA_COLUMN c WHERE NOT EXISTS (SELECT OID FROM METADATA_COLUMN_VIEW WHERE OID = c.OID)")?.query_map([], |row| Ok((row.get::<_, i64>("SCHEMA_OID")?, row.get::<_, i64>("COLUMN_OID")?)))? {
                let (schema_oid, column_oid) = row_result?;
                let table_name: String = format!("TABLE{schema_oid}");

                // Drop the multiselect *-to-* mapping table
                let drop_multiselect_sql: String = format!("DROP TABLE IF EXISTS MULTISELECT{column_oid}");
                trans.execute(&drop_multiselect_sql, [])?;

                // Drop the column from its host table
                if trans.table_exists(Some("main"), &table_name)? {
                    let drop_sql: String = format!("ALTER TABLE {table_name} DROP COLUMN COLUMN{column_oid}");
                    trans.execute(&drop_sql, [])?;
                }
            }

            // Delete data inheritance columns
            for row_result in trans.prepare("SELECT inh.MASTER_SCHEMA_OID, inh.INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE inh INNER JOIN METADATA_SCHEMA m ON m.OID = inh.MASTER_SCHEMA_OID WHERE inh.TRASH OR m.TRASH")?.query_map([], |row| Ok((row.get::<_, i64>("MASTER_SCHEMA_OID")?, row.get::<_, i64>("INHERITOR_SCHEMA_OID")?)))? {
                let (master_schema_oid, inheritor_schema_oid) = row_result?;
                let inheritor_table_name: String = format!("TABLE{inheritor_schema_oid}");

                // Drop the inheritance definition column from the inheriting table
                if trans.table_exists(Some("main"), &inheritor_table_name)? {
                    let drop_sql: String = format!("ALTER TABLE {inheritor_table_name} DROP COLUMN MASTER{master_schema_oid}_OID");
                    trans.execute(&drop_sql, [])?;
                }
            }

            // Delete data tables
            for row_result in trans.prepare("SELECT s.OID FROM METADATA_SCHEMA s INNER JOIN METADATA_TABLE t ON s.OID = t.OID WHERE s.TRASH")?.query_map([], |row| row.get("OID"))? {
                let table_oid: i64 = row_result?;

                // Drop the table and views related to the table
                let drop_sql: String = format!("
                    DROP VIEW IF EXISTS TABLE{table_oid}_LABEL_VIEW;
                    DROP VIEW IF EXISTS TABLE{table_oid}_POLYMORPHISM_VIEW;
                    DROP TABLE IF EXISTS TABLE{table_oid};
                ");
                trans.execute_batch(&drop_sql)?;
            }

            // Finish cleaning metadata
            trans.execute_batch("
            DELETE FROM METADATA_COLUMN_TYPE AS ct WHERE ct.TRASH OR NOT EXISTS(
                SELECT OID FROM METADATA_COLUMN_TYPE__FORMULA WHERE OID = ct.OID
                UNION ALL
                SELECT OID FROM METADATA_COLUMN_TYPE__SUBREPORT WHERE OID = ct.OID 
                UNION ALL 
                SELECT OID FROM METADATA_COLUMN_TYPE__PRIMITIVE WHERE OID = ct.OID
                UNION ALL 
                SELECT OID FROM METADATA_COLUMN_TYPE__OBJECT WHERE OID = ct.OID 
                UNION ALL 
                SELECT OID FROM METADATA_COLUMN_TYPE__SELECT WHERE OID = ct.OID
                UNION ALL 
                SELECT OID FROM METADATA_COLUMN_TYPE__MULTISELECT WHERE OID = ct.OID 
            );
            DELETE FROM METADATA_COLUMN WHERE TRASH;
            DELETE FROM METADATA_SCHEMA WHERE TRASH;
            DELETE FROM METADATA_SCHEMA_INHERITANCE WHERE TRASH;
            DELETE FROM METADATA_SCHEMA_VALIDATION WHERE TRASH;
            DELETE FROM METADATA_SCHEMA_ORDERBY WHERE TRASH;
            DELETE FROM METADATA_REPORT_GROUPBY WHERE TRASH;
            ")?;

            return Ok(());
        }
        None => {
            return Err(error::Error::AdhocError("No file is open!"));
        }
    }
}
