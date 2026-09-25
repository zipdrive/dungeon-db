use crate::util::error::Error;
use rusqlite::{Connection, Error as RusqliteError, OptionalExtension, Params, Result, Row, RowIndex};
use rusqlite::types::FromSql;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use tauri::AppHandle;
use tempfile::NamedTempFile;
use backtrace::Backtrace;

static DATABASE_PATH: Mutex<Option<String>> = Mutex::new(None);
static DATABASE_AUTOSAVE_PATH: Mutex<Option<NamedTempFile>> = Mutex::new(None);

/// Applies the metadata schema to the database at the given path.
fn setup_db_at_path<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    let conn = Connection::open(path)?;
    conn.execute_batch("
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

BEGIN;

DROP VIEW IF EXISTS METADATA_REPORT_ORDERBY;
DROP VIEW IF EXISTS METADATA_REPORT_GROUPBY;
DROP VIEW IF EXISTS METADATA_REPORT_COLUMN;
DROP VIEW IF EXISTS METADATA_REPORT_COLUMNTYPE;
DROP VIEW IF EXISTS METADATA_REPORT;

DROP VIEW IF EXISTS METADATA_DATASOURCE;
DROP VIEW IF EXISTS METADATA_TABLE_COLUMN_VALIDATION;
DROP VIEW IF EXISTS METADATA_TABLE_COLUMN_PATH;
DROP VIEW IF EXISTS METADATA_TABLE_COLUMN;
DROP VIEW IF EXISTS METADATA_TABLE_COLUMNTYPE;
DROP VIEW IF EXISTS METADATA_TABLE_INHERITANCE_PATH;
DROP VIEW IF EXISTS METADATA_TABLE_INHERITANCE;
DROP VIEW IF EXISTS METADATA_TABLE;

DROP VIEW IF EXISTS METADATA_FILE;


-------------
--  FILES  --
-------------

-- __METADATA_FILE stores all files.
CREATE TABLE IF NOT EXISTS __METADATA_FILE (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    NAME TEXT NOT NULL
);

-- __METADATA_FILE_PATH stores all files that are a reference to a file on the local filesystem.
CREATE TABLE IF NOT EXISTS __METADATA_FILE_PATH (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_FILE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    PATH TEXT NOT NULL
);

-- __METADATA_FILE_BLOB stores all files stored inside the database as BLOBs.
CREATE TABLE IF NOT EXISTS __METADATA_FILE_BLOB (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_FILE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONTENT BLOB NOT NULL
);

-- METADATA_FILE provides access only to files that are not deleted.
CREATE VIEW METADATA_FILE AS 
    SELECT 
        f.OID,
        f.NAME,
        'path' AS FILE_TYPE,
        p.PATH AS FILE_PATH,
        NULL AS FILE_CONTENT_SIZE
    FROM __METADATA_FILE f 
    INNER JOIN __METADATA_FILE_PATH p ON p.OID = f.OID 
    WHERE NOT f.TRASH

    UNION ALL

    SELECT
        f.OID,
        f.NAME AS FILE_NAME,
        'blob' AS FILE_TYPE,
        NULL AS FILE_PATH,
        LENGTH(b.CONTENT) AS FILE_CONTENT_SIZE
    FROM __METADATA_FILE f 
    INNER JOIN __METADATA_FILE_BLOB b ON b.OID = f.OID 
    WHERE NOT f.TRASH
;




---------------
--  REPORTS  --
---------------

-- __METADATA_REPORT is associated with all report column definitions.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    NAME TEXT NOT NULL,

    FILTER TEXT NOT NULL
        -- A formula applied to filter the rows of the report
);

-- METADATA_REPORT provides access only to reports that are not deleted.
CREATE VIEW METADATA_REPORT AS
    SELECT 
        OID,
        NAME,
        FILTER
    FROM __METADATA_REPORT 
    WHERE NOT TRASH
;



---------------------------
--  REPORT COLUMN TYPES  --
---------------------------

-- __METADATA_REPORT_COLUMNTYPE stores all column types for reports.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_COLUMNTYPE (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE
);

-- __METADATA_REPORT_COLUMNTYPE_FORMULA stores all Formula column types.
-- Formulas are a virtual column that can be part of any schema.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_COLUMNTYPE_FORMULA (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_REPORT_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FORMULA TEXT NOT NULL
);

-- __METADATA_REPORT_COLUMNTYPE_SUBREPORT stores all user-defined drill-down report types.
-- Drill-down reports are a link to a report filtered by the GROUPBY columns of the row in the parent report, or else by the OIDs of the row in the parent report.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_COLUMNTYPE_SUBREPORT (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_REPORT_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    REPORT_OID INTEGER NOT NULL REFERENCES __METADATA_REPORT (OID) 
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_REPORT_COLUMNTYPE_SUBREPORT_INDEX_BY_REPORT_OID ON __METADATA_REPORT_COLUMNTYPE_SUBREPORT (REPORT_OID);

-- METADATA_REPORT_COLUMNTYPE provides access only to column types that have not been deleted.
CREATE VIEW IF NOT EXISTS METADATA_REPORT_COLUMNTYPE AS 
    SELECT 
        ct.OID,
        'Formula' AS TYPE,
        ctf.FORMULA AS FORMULA,
        NULL AS REPORT_OID
    FROM __METADATA_REPORT_COLUMNTYPE ct 
    INNER JOIN __METADATA_REPORT_COLUMNTYPE_FORMULA ctf ON ctf.OID = ct.OID 
    WHERE NOT ct.TRASH 

    UNION ALL 

    SELECT 
        ct.OID,
        'Subreport' AS TYPE,
        NULL AS FORMULA,
        cts.REPORT_OID
    FROM __METADATA_REPORT_COLUMNTYPE ct 
    INNER JOIN __METADATA_REPORT_COLUMNTYPE_SUBREPORT cts ON cts.OID = ct.OID 
    INNER JOIN METADATA_REPORT s ON s.OID = cts.REPORT_OID 
    WHERE NOT ct.TRASH
;



----------------------
--  REPORT COLUMNS  --
----------------------

-- __METADATA_REPORT_COLUMN stores all columns of user-defined tables
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_COLUMN (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    REPORT_OID INTEGER NOT NULL REFERENCES __METADATA_REPORT (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    NAME TEXT NOT NULL,
    COLUMNTYPE_OID INTEGER NOT NULL DEFAULT -1 REFERENCES __METADATA_REPORT_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE SET DEFAULT,

    SIZE INTEGER NOT NULL,
        -- Column pixel width
    
    STYLE TEXT NOT NULL,
        -- Column CSS style
    
    IS_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE
        -- True if the column is displayed in the label for a row of the table
);
CREATE INDEX IF NOT EXISTS __METADATA_REPORT_COLUMN_INDEX_BY_REPORT_OID ON __METADATA_REPORT_COLUMN (REPORT_OID);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_REPORT_COLUMN_INDEX_BY_ORDERING ON __METADATA_REPORT_COLUMN (ORDERING);

-- METADATA_REPORT_COLUMN restricts access to only columns that have not been deleted.
CREATE VIEW METADATA_REPORT_COLUMN AS 
    SELECT
        c.OID,
        c.REPORT_OID,
        c.NAME,
        ct.OID AS COLUMNTYPE_OID,
        ct.TYPE AS COLUMNTYPE,
        ct.FORMULA AS COLUMNTYPE_FORMULA,
        ct.REPORT_OID AS COLUMNTYPE_REPORT_OID,
        c.SIZE,
        c.STYLE,
        c.IS_PRIMARY_KEY 
    FROM __METADATA_REPORT_COLUMN c 
    INNER JOIN METADATA_REPORT_COLUMNTYPE ct ON ct.OID = c.COLUMNTYPE_OID
    WHERE NOT TRASH
;



--------------------------------
--  REPORT GROUPING/ORDERING  --
--------------------------------

-- __METADATA_REPORT_GROUPBY lists the columns by which the report is grouped.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_GROUPBY (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    COLUMN_OID INTEGER NOT NULL REFERENCES __METADATA_REPORT_COLUMN (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- METADATA_REPORT_GROUPBY restricts access only to columns that have not been deleted
CREATE VIEW METADATA_REPORT_GROUPBY AS 
    SELECT 
        o.OID,
        c.REPORT_OID,
        o.COLUMN_OID
    FROM __METADATA_REPORT_GROUPBY o 
    INNER JOIN METADATA_REPORT_COLUMN c ON c.OID = o.COLUMN_OID
    WHERE NOT o.TRASH
;

-- __METADATA_REPORT_ORDERBY lists the columns by which the report is sorted.
CREATE TABLE IF NOT EXISTS __METADATA_REPORT_ORDERBY (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    COLUMN_OID INTEGER NOT NULL REFERENCES __METADATA_REPORT_COLUMN (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

-- METADATA_REPORT_ORDERBY restricts access only to columns that have not been deleted
CREATE VIEW METADATA_REPORT_ORDERBY AS 
    SELECT 
        o.OID,
        c.REPORT_OID,
        o.COLUMN_OID
    FROM __METADATA_REPORT_ORDERBY o 
    INNER JOIN METADATA_REPORT_COLUMN c ON c.OID = o.COLUMN_OID
    WHERE NOT o.TRASH
;





--------------
--  TABLES  --
--------------

-- __METADATA_TABLE is associated with all table column definitions.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    NAME TEXT NOT NULL
);

-- METADATA_TABLE provides access only to tables that are not deleted.
CREATE VIEW METADATA_TABLE AS
    SELECT 
        OID,
        NAME
    FROM __METADATA_TABLE 
    WHERE NOT TRASH
;



-------------------------
--  TABLE INHERITANCE  --
-------------------------

-- __METADATA_TABLE_INHERITANCE defines the inheritance relationships between tables.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_INHERITANCE (
    INHERITOR_TABLE_OID INTEGER REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    MASTER_TABLE_OID INTEGER REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (INHERITOR_TABLE_OID, MASTER_TABLE_OID)
);
CREATE INDEX IF NOT EXISTS METADATA_TABLE_INHERITANCE_INDEX_BY_MASTER_TABLE_OID ON METADATA_TABLE_INHERITANCE (MASTER_TABLE_OID);

-- METADATA_TABLE_INHERITANCE provides access only to inheritance relationships that have not been deleted.
CREATE VIEW METADATA_TABLE_INHERITANCE AS 
    SELECT 
        INHERITOR_TABLE_OID,
        MASTER_TABLE_OID 
    FROM __METADATA_TABLE_INHERITANCE u 
    INNER JOIN __METADATA_TABLE t1 ON t1.OID = u.INHERITOR_TABLE_OID
    INNER JOIN __METADATA_TABLE t2 ON t2.OID = u.MASTER_TABLE_OID
    WHERE NOT u.TRASH AND NOT t1.TRASH AND NOT t2.TRASH
;

-- METADATA_TABLE_INHERITANCE_PATH describes the path of inheritance from one table to another.
CREATE VIEW METADATA_TABLE_INHERITANCE_PATH AS 
    WITH RECURSIVE FLATTENING (INHERITOR_TABLE_OID, MASTER_TABLE_OID, INHERITOR_DATASOURCE_PATH, MASTER_DATASOURCE_PATH, DEPTH) AS (
        SELECT
            u.INHERITOR_TABLE_OID,
            u.MASTER_TABLE_OID,
            '_INHERITOR' || u.INHERITOR_TABLE_OID INHERITOR_DATASOURCE_PATH,
            '_MASTER' || u.MASTER_TABLE_OID MASTER_DATASOURCE_PATH,
            1 DEPTH
        FROM METADATA_TABLE_INHERITANCE u

        UNION

        SELECT
            u.INHERITOR_TABLE_OID,
            s.MASTER_TABLE_OID,
            s.INHERITOR_DATASOURCE_PATH || '_INHERITOR' || u.INHERITOR_TABLE_OID INHERITOR_DATASOURCE_PATH,
            '_MASTER' || u.MASTER_TABLE_OID || s.MASTER_DATASOURCE_PATH MASTER_DATASOURCE_PATH,
            s.DEPTH + 1 DEPTH
        FROM FLATTENING s
        INNER JOIN METADATA_TABLE_INHERITANCE u ON u.MASTER_TABLE_OID = s.INHERITOR_TABLE_OID
    )
    SELECT 
        INHERITOR_TABLE_OID,
        MASTER_TABLE_OID,
        MIN(INHERITOR_DATASOURCE_PATH) INHERITOR_DATASOURCE_PATH,
        MIN(MASTER_DATASOURCE_PATH) MASTER_DATASOURCE_PATH,
        MAX(DEPTH) MAX_DEPTH  
    FROM FLATTENING
    GROUP BY INHERITOR_TABLE_OID, MASTER_TABLE_OID
;


--------------------------
--  TABLE COLUMN TYPES  --
--------------------------

-- __METADATA_TABLE_COLUMNTYPE stores all column types for tables.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE
);

-- METADATA_TABLE_COLUMNTYPE_PRIMITIVE stores all Primitive column types.
-- Primitives are a storage column with a primitive type (e.g. number, text, file).
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_PRIMITIVE (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    MODE TEXT NOT NULL,
        -- The type of primitive. Possible values are:
        -- 'Text'
        -- 'Text/Json'
        -- 'Text/Xml'
        -- 'Text/Markdown'
        -- 'Text/BBCode'
        -- 'Boolean'
        -- 'Integer'
        -- 'Number'
        -- 'Date'
        -- 'Datetime'
    
    DEFAULT_VALUE TEXT
        -- The default value for columns of this type 
);
INSERT OR IGNORE INTO __METADATA_TABLE_COLUMNTYPE (OID) VALUES (-1);
INSERT OR IGNORE INTO __METADATA_TABLE_COLUMNTYPE_PRIMITIVE (OID, MODE, DEFAULT_VALUE) VALUES (-1, 'Text', NULL);

-- __METADATA_TABLE_COLUMNTYPE_FILE stores all File column types.
-- Files are a storage column that select a single row from the __METADATA_FILE table.
-- This is represented on the UI by upload/download buttons.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_FILE (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
);

-- __METADATA_TABLE_COLUMNTYPE_OBJECT stores all Object column types.
-- Objects are a storage column that represent an injective relationship to at most one row in another table.
-- This is represented in UI by a clickable link with the primary key of the linked row.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_OBJECT (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    TABLE_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_OBJECT_INDEX_BY_TABLE_OID ON __METADATA_TABLE_COLUMNTYPE_OBJECT (TABLE_OID);

-- __METADATA_TABLE_COLUMNTYPE_SINGLESELECT stores all Select column types.
-- Selects are a storage column that selects a single row from another table.
-- This is represented in UI by a dropdown.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_SINGLESELECT (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    TABLE_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_SINGLESELECT_INDEX_BY_TABLE_OID ON __METADATA_TABLE_COLUMNTYPE_SINGLESELECT (TABLE_OID);

-- __METADATA_TABLE_COLUMNTYPE_MULTISELECT stores all Multiselect column types.
-- Multiselects are a storage column that selects multiple rows from another table.
-- This is represented in UI by a checkbox dropdown.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_MULTISELECT (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    TABLE_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
);

-- __METADATA_TABLE_COLUMNTYPE_SUBREPORT stores all user-defined drill-down report types.
-- Drill-down reports are a link to a report that is filtered by the OID of the row in the parent table.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_SUBREPORT (
    OID INTEGER PRIMARY KEY REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    REPORT_OID INTEGER NOT NULL REFERENCES __METADATA_REPORT (OID) 
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_TABLE_COLUMNTYPE_SUBREPORT_INDEX_BY_REPORT_OID ON __METADATA_TABLE_COLUMNTYPE_SUBREPORT (REPORT_OID);

-- METADATA_TABLE_COLUMNTYPE provides access only to column types that have not been deleted.
CREATE VIEW METADATA_TABLE_COLUMNTYPE AS 
    SELECT 
        ct.OID,
        ctp.MODE AS TYPE,
        ctp.DEFAULT_VALUE,
        NULL AS TABLE_OID,
        NULL AS REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_PRIMITIVE ctp ON ctp.OID = ct.OID 
    WHERE NOT ct.TRASH 

    UNION ALL 

    SELECT 
        ct.OID,
        'File' AS TYPE,
        NULL AS DEFAULT_VALUE,
        NULL AS TABLE_OID,
        NULL AS REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_FILE ctf ON ctf.OID = ct.OID 
    WHERE NOT ct.TRASH

    UNION ALL

    SELECT 
        ct.OID,
        'Object' AS TYPE,
        NULL AS DEFAULT_VALUE,
        cto.TABLE_OID,
        NULL AS REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_OBJECT cto ON cto.OID = ct.OID 
    INNER JOIN METADATA_TABLE t ON t.OID = cto.TABLE_OID 
    WHERE NOT ct.TRASH
    
    UNION ALL 

    SELECT 
        ct.OID,
        'SingleSelect' AS TYPE,
        NULL AS DEFAULT_VALUE,
        cto.TABLE_OID,
        NULL AS REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_SINGLESELECT cto ON cto.OID = ct.OID 
    INNER JOIN METADATA_TABLE t ON t.OID = cto.TABLE_OID 
    WHERE NOT ct.TRASH
    
    UNION ALL 

    SELECT 
        ct.OID,
        'MultiSelect' AS TYPE,
        NULL AS DEFAULT_VALUE,
        cto.TABLE_OID,
        NULL AS REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_MULTISELECT cto ON cto.OID = ct.OID 
    INNER JOIN METADATA_TABLE t ON t.OID = cto.TABLE_OID 
    WHERE NOT ct.TRASH
    
    UNION ALL 

    SELECT 
        ct.OID,
        'Subreport' AS TYPE,
        NULL AS DEFAULT_VALUE,
        NULL AS TABLE_OID,
        ctr.REPORT_OID
    FROM __METADATA_TABLE_COLUMNTYPE ct 
    INNER JOIN __METADATA_TABLE_COLUMNTYPE_SUBREPORT ctr ON ctr.OID = ct.OID 
    INNER JOIN METADATA_REPORT r ON r.OID = ctr.REPORT_OID 
    WHERE NOT ct.TRASH
;


---------------------
--  TABLE COLUMNS  --
---------------------

-- __METADATA_TABLE_COLUMN stores all columns of user-defined tables
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMN (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    TABLE_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    NAME TEXT NOT NULL,
    COLUMNTYPE_OID INTEGER NOT NULL DEFAULT -1 REFERENCES __METADATA_TABLE_COLUMNTYPE (OID)
        ON UPDATE CASCADE
        ON DELETE SET DEFAULT,

    ORDERING INTEGER NOT NULL,
        -- The ordering of the columns, from left to right

    SIZE INTEGER NOT NULL,
        -- Column pixel width
    
    STYLE TEXT NOT NULL,
        -- Column CSS style
    
    IS_PRIMARY_KEY BOOLEAN NOT NULL DEFAULT FALSE
        -- True if the column is displayed in the label for a row of the table
);
CREATE INDEX IF NOT EXISTS __METADATA_TABLE_COLUMN_INDEX_BY_TABLE_OID ON __METADATA_TABLE_COLUMN (TABLE_OID);
CREATE UNIQUE INDEX IF NOT EXISTS __METADATA_TABLE_COLUMN_INDEX_BY_ORDERING ON __METADATA_TABLE_COLUMN (ORDERING);

-- METADATA_TABLE_COLUMN restricts access to only columns that have not been deleted.
CREATE VIEW METADATA_TABLE_COLUMN AS 
    SELECT
        c.OID,
        c.TABLE_OID,
        c.NAME,
        ct.OID AS COLUMNTYPE_OID,
        ct.TYPE AS COLUMNTYPE,
        ct.DEFAULT_VALUE AS COLUMNTYPE_DEFAULT_VALUE,
        ct.TABLE_OID AS COLUMNTYPE_TABLE_OID,
        c.ORDERING,
        c.SIZE,
        c.STYLE,
        c.IS_PRIMARY_KEY 
    FROM __METADATA_TABLE_COLUMN c 
    INNER JOIN METADATA_TABLE_COLUMNTYPE ct ON ct.OID = c.COLUMNTYPE_OID
    WHERE NOT TRASH
;

-- METADATA_TABLE_COLUMN_PATH lists all columns (base or inherited) for a particular table.
CREATE VIEW METADATA_TABLE_COLUMN_PATH AS 
    SELECT 
        c.OID,
        c.TABLE_OID,
        c.TABLE_OID AS BASE_TABLE_OID,
        '' AS DATASOURCE_PATH,
        c.NAME,
        c.COLUMNTYPE_OID,
        c.COLUMNTYPE,
        c.COLUMNTYPE_DEFAULT_VALUE,
        c.COLUMNTYPE_TABLE_OID,
        c.ORDERING,
        c.SIZE,
        c.STYLE,
        c.IS_PRIMARY_KEY
    FROM METADATA_TABLE_COLUMN c 

    UNION ALL

    SELECT 
        c.OID,
        u.INHERITOR_TABLE_OID AS TABLE_OID,
        c.TABLE_OID AS BASE_TABLE_OID,
        u.MASTER_DATASOURCE_PATH AS DATASOURCE_PATH,
        c.NAME,
        c.COLUMNTYPE_OID,
        c.COLUMNTYPE,
        c.COLUMNTYPE_DEFAULT_VALUE,
        c.COLUMNTYPE_TABLE_OID,
        c.ORDERING,
        c.SIZE,
        c.STYLE,
        c.IS_PRIMARY_KEY
    FROM METADATA_TABLE_INHERITANCE_PATH u 
    INNER JOIN METADATA_TABLE_COLUMN c ON c.TABLE_OID = u.MASTER_TABLE_OID
;



------------------------
--  TABLE VALIDATION  --
------------------------

-- __METADATA_TABLE_COLUMN_VALIDATION stores formulas used to validate the contents of a column.
CREATE TABLE IF NOT EXISTS __METADATA_TABLE_COLUMN_VALIDATION (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    COLUMN_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE_COLUMN (OID)
        ON UPDATE CASCADE 
        ON DELETE CASCADE,

    FORMULA TEXT NOT NULL,
        -- A formula returning a scalar boolean value. If the value is false, validation fails.
    
    MESSAGE TEXT NOT NULL 
        -- The message to display if validation fails.
);

-- METADATA_TABLE_COLUMN_VALIDATION restricts access only to validation that has not been deleted.
CREATE VIEW METADATA_TABLE_COLUMN_VALIDATION AS 
    SELECT 
        OID,
        COLUMN_OID,
        FORMULA,
        MESSAGE
    FROM __METADATA_TABLE_COLUMN_VALIDATION
    WHERE NOT TRASH
;



-------------------
--  DATASOURCES  --
-------------------

-- __METADATA_DATASOURCE stores root datasources for a formula parameter.
CREATE TABLE IF NOT EXISTS __METADATA_DATASOURCE (
    OID INTEGER PRIMARY KEY,
    TRASH BOOLEAN NOT NULL DEFAULT FALSE,
    TABLE_OID INTEGER NOT NULL REFERENCES __METADATA_TABLE (OID)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    LABEL TEXT
);
CREATE INDEX IF NOT EXISTS __METADATA_DATASOURCE_INDEX_BY_TABLE_OID ON __METADATA_DATASOURCE (TABLE_OID);

-- METADATA_DATASOURCE restricts access only to valid, non-deleted datasources.
CREATE VIEW METADATA_DATASOURCE AS 
    SELECT 
        d.OID,
        d.TABLE_OID,
        COALESCE(d.LABEL, t.NAME) AS LABEL
    FROM __METADATA_DATASOURCE d 
    INNER JOIN METADATA_TABLE t ON t.OID = d.TABLE_OID 
    WHERE NOT TRASH 
;

COMMIT;
    ")?;
    return Ok(());
}

/// Closes any previous database connection, and opens a new temporary file.
pub fn init_new() -> Result<(), Error> {
    // Reset static variables
    let mut database_path = DATABASE_PATH.lock().unwrap();
    let mut database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    *database_path = None;
    *database_autosave_tempfile = None;

    // Create new autosave file
    let Ok(tempfile) = NamedTempFile::new() else {
        return Err(Error::adhoc("Unable to make an autosave file."));
    };

    // Initialize the database at the path
    setup_db_at_path(tempfile.path())?;

    // Transfer ownership of the tempfile to the static variable
    *database_autosave_tempfile = Some(tempfile);

    Ok(())
}

/// Closes any previous database connection, and opens a new one.
pub fn init_existing(path: String) -> Result<(), Error> {
    // Reset static variables
    let mut database_path = DATABASE_PATH.lock().unwrap();
    let mut database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    *database_path = None;
    *database_autosave_tempfile = None;

    // Make a new autosave file
    let Ok(tempfile) = NamedTempFile::new() else {
        return Err(Error::adhoc("Unable to make an autosave file."));
    };

    // Copy the data over from the main file to the autosave
    let Ok(_) = fs::copy(&path, tempfile.path()) else {
        return Err(Error::adhoc(
            "Unable to transfer contents of autosave to main file.",
        ));
    };
    setup_db_at_path(tempfile.path())?;

    // Record the path to static variable
    *database_path = Some(path);
    // Transfer ownership of the tempfile to the static variable
    *database_autosave_tempfile = Some(tempfile);

    Ok(())
}

/// Opens a connection to the database.
pub fn open() -> Result<Connection, Error> {
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
            return Err(Error::adhoc("Cannot open file: No file is open!"));
        }
    }
}

/// Copies the data from the autosave file to the main file, then open a connection to the main file for cleaning purposes.
/// Returns false if the file was not saved due to the user cancelling the save prompt, and returns true otherwise.
pub fn save_to_current_file(app: &AppHandle) -> Result<bool, Error> {
    // First, check if there is a main file
    {
        let database_path = DATABASE_PATH.lock().unwrap();
        if let Some(ref save_path) = *database_path {
            // If there is a main file, save to it
            save(app, save_path)?;
            return Ok(true);
        }
    }

    // If there is not a main file, prompt which file to save to
    save_to_prompted_file(app)
}

/// Copies the data from the autosave file to a prompted main file, then open a connection to the main file for cleaning purposes.
/// Returns false if the file was not saved due to the user cancelling the save prompt, and returns true otherwise.
pub fn save_to_prompted_file(app: &AppHandle) -> Result<bool, Error> {
    use tauri_plugin_dialog::DialogExt;

    let mut database_path = DATABASE_PATH.lock().unwrap();
    if let Some(file_path) = app
        .dialog()
        .file()
        .add_filter("DungeonDB File (*.dndb)", &["dndb"])
        .blocking_save_file()
    {
        *database_path = Some(file_path.to_string());
        save(app, (database_path.as_ref()).unwrap())?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Copies the data from the autosave file to the specified main file, then open a connection to the main file for cleaning purposes.
fn save(app: &AppHandle, save_path: &String) -> Result<(), Error> {
    let database_autosave_tempfile = DATABASE_AUTOSAVE_PATH.lock().unwrap();
    match *database_autosave_tempfile {
        Some(ref tempfile) => {
            // Copy the data from the autosave back to the main file
            let Ok(_) = fs::copy(tempfile.path(), save_path) else {
                return Err(Error::adhoc(
                    "Unable to transfer contents of autosave to main file.",
                ));
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
                sql_execute(&trans, &drop_multiselect_sql, [])?;

                // Drop the column from its host table
                if trans.table_exists(Some("main"), &table_name)? {
                    let drop_sql: String = format!("ALTER TABLE {table_name} DROP COLUMN COLUMN{column_oid}");
                    sql_execute(&trans, &drop_sql, [])?;
                }
            }

            // Delete data inheritance columns
            for row_result in trans.prepare("SELECT inh.MASTER_SCHEMA_OID, inh.INHERITOR_SCHEMA_OID FROM METADATA_SCHEMA_INHERITANCE inh INNER JOIN METADATA_SCHEMA m ON m.OID = inh.MASTER_SCHEMA_OID WHERE inh.TRASH OR m.TRASH")?.query_map([], |row| Ok((row.get::<_, i64>("MASTER_SCHEMA_OID")?, row.get::<_, i64>("INHERITOR_SCHEMA_OID")?)))? {
                let (master_schema_oid, inheritor_schema_oid) = row_result?;
                let inheritor_table_name: String = format!("TABLE{inheritor_schema_oid}");

                // Drop the inheritance definition column from the inheriting table
                if trans.table_exists(Some("main"), &inheritor_table_name)? {
                    let drop_sql: String = format!("ALTER TABLE {inheritor_table_name} DROP COLUMN MASTER{master_schema_oid}_OID");
                    sql_execute(&trans, &drop_sql, [])?;
                }
            }

            // Delete data tables
            for row_result in trans
                .prepare("SELECT s.OID FROM METADATA_SCHEMA s WHERE s.TRASH")?
                .query_map([], |row| row.get("OID"))?
            {
                let schema_oid: i64 = row_result?;

                // Drop the table and views related to the table
                let drop_sql: String = format!(
                    "
                    DROP VIEW IF EXISTS REPORT{schema_oid}_VIEW;
                    DROP VIEW IF EXISTS TABLE{schema_oid}_VIEW;
                    DROP VIEW IF EXISTS TABLE{schema_oid}_LABEL_VIEW;
                    DROP VIEW IF EXISTS TABLE{schema_oid}_POLYMORPHISM_VIEW;
                    DROP TABLE IF EXISTS TABLE{schema_oid};
                "
                );
                trans.execute_batch(&drop_sql)?;
            }

            // Finish cleaning metadata
            trans.execute_batch(
                "
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
            ",
            )?;

            return Ok(());
        }
        None => {
            return Err(Error::adhoc("Cannot save to opened file: No file is open!"));
        }
    }
}



/// Maps all rows of a query, then iterates over the mapped values.
pub fn sql_map_then_iter<T, U, S, P, F1, F2>(conn: &Connection, sql: S, params: P, row_parser: F1, mut row_fn: F2) -> Result<Option<U>, Error> where S : AsRef<str>, P : Params, F1 : Fn(&Row) -> Result<T, RusqliteError>, F2 : FnMut(T) -> Result<Option<U>, Error> {
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query_map(params, row_parser) {
                Ok(mapped_rows) => {
                    for mapped_row_result in mapped_rows {
                        match mapped_row_result {
                            Ok(mapped_row) => {
                                if let Some(u) = row_fn(mapped_row)? {
                                    return Ok(Some(u));
                                }
                            }
                            Err(err) => {
                                return Err(Error::SqlError {
                                    sql: String::from(sql.as_ref()),
                                    backtrace: Backtrace::new_unresolved(),
                                    err
                                });
                            }
                        }
                    }
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
    Ok(None)
}

/// Maps and collects all rows of a query into a Vec object.
pub fn sql_map<T, S, P, F1, F2>(conn: &Connection, sql: S, params: P, row_parser: F1) -> Result<Vec<T>, Error> where S: AsRef<str>, P : Params, F1 : Fn(&Row) -> Result<T, RusqliteError> {
    let mut result: Vec<T> = Vec::new();
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query(params) {
                Ok(mut rows) => {
                    loop {
                        let Some(row) = (match rows.next() {
                            Ok(row) => row,
                            Err(err) => {
                                return Err(Error::SqlError {
                                    sql: String::from(sql.as_ref()),
                                    backtrace: Backtrace::new_unresolved(),
                                    err
                                });
                            }
                        }) else {
                            break;
                        };

                        result.push(match row_parser(&row) {
                            Ok(values) => values,
                            Err(err) => {
                                return Err(Error::SqlError {
                                    sql: String::from(sql.as_ref()),
                                    backtrace: Backtrace::new_unresolved(),
                                    err
                                });
                            } 
                        });
                    }
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
    Ok(result)
}


pub struct RowWrapper<'a> {
    sql: String,
    row: &'a Row<'a> 
}

impl<'a> RowWrapper<'a> {
    /// Creates a new row wrapper.
    fn new<S>(sql: &S, row: &'a Row<'a>) -> Self where S : AsRef<str> {
        Self {
            sql: String::from(sql.as_ref()),
            row 
        }
    }

    /// Gets an index from the row.
    pub fn get<I, T>(&self, idx: I) -> Result<T, Error> where I : RowIndex, T : FromSql {
        match self.row.get::<I, T>(idx) {
            Ok(value) => Ok(value),
            Err(err) => Err(Error::SqlError { 
                sql: self.sql.clone(), 
                backtrace: Backtrace::new_unresolved(), 
                err 
            })
        }
    }
}

/// Iterates progressively over all rows of a query.
pub fn sql_iter<U, S, P, F>(conn: &Connection, sql: S, params: P, mut row_fn: F) -> Result<Option<U>, Error> where S: AsRef<str>, P : Params, F : FnMut(&RowWrapper) -> Result<Option<U>, Error> {
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query(params) {
                Ok(mut rows) => {
                    loop {
                        let Some(row) = (match rows.next() {
                            Ok(row) => row,
                            Err(err) => {
                                return Err(Error::SqlError {
                                    sql: String::from(sql.as_ref()),
                                    backtrace: Backtrace::new_unresolved(),
                                    err
                                });
                            }
                        }) else {
                            break;
                        };

                        let row_wrapped: RowWrapper = RowWrapper::new(&sql, row);
                        if let Some(u) = row_fn(&row_wrapped)? {
                            return Ok(Some(u));
                        }
                    }
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
    Ok(None)
}

/// Iterates progressively over all rows of a query, collecting the mapped value from each row into a Vec.
pub fn sql_collect<U, S, P, F>(conn: &Connection, sql: S, params: P, mut row_fn: F) -> Result<Vec<U>, Error> where S: AsRef<str>, P : Params, F : FnMut(&RowWrapper) -> Result<U, Error> {
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query(params) {
                Ok(mut rows) => {
                    let mut output: Vec<U> = Vec::new();
                    loop {
                        let Some(row) = (match rows.next() {
                            Ok(row) => row,
                            Err(err) => {
                                return Err(Error::SqlError {
                                    sql: String::from(sql.as_ref()),
                                    backtrace: Backtrace::new_unresolved(),
                                    err
                                });
                            }
                        }) else {
                            break;
                        };

                        let row_wrapped: RowWrapper = RowWrapper::new(&sql, row);
                        output.push(row_fn(&row_wrapped)?);
                    }
                    return Ok(output);
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
}


/// Queries for exactly one row.
pub fn sql_one<S, T, P, F>(conn: &Connection, sql: S, params: P, row_fn: F) -> Result<T, Error> where S : AsRef<str>, P : Params, F : FnOnce(&RowWrapper) -> Result<T, Error> {
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query(params) {
                Ok(mut rows) => {
                    let Some(row) = (match rows.next() {
                        Ok(row) => row,
                        Err(err) => {
                            return Err(Error::SqlError {
                                sql: String::from(sql.as_ref()),
                                backtrace: Backtrace::new_unresolved(),
                                err
                            });
                        }
                    }) else {
                        return Err(Error::SqlError {
                            sql: String::from(sql.as_ref()),
                            backtrace: Backtrace::new_unresolved(),
                            err: rusqlite::Error::QueryReturnedNoRows
                        });
                    };

                    let row_wrapped: RowWrapper = RowWrapper::new(&sql, row);
                    return row_fn(&row_wrapped);
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
}

/// Queries for up to one row.
pub fn sql_zero_or_one<S, T, P, F>(conn: &Connection, sql: S, params: P, row_fn: F) -> Result<Option<T>, Error> where S : AsRef<str>, P : Params, F : FnOnce(&RowWrapper) -> Result<T, Error> {
    match conn.prepare(sql.as_ref()) {
        Ok(mut stmt) => {
            match stmt.query(params) {
                Ok(mut rows) => {
                    let Some(row) = (match rows.next() {
                        Ok(row) => row,
                        Err(err) => {
                            return Err(Error::SqlError {
                                sql: String::from(sql.as_ref()),
                                backtrace: Backtrace::new_unresolved(),
                                err
                            });
                        }
                    }) else {
                        return Ok(None);
                    };

                    let row_wrapped: RowWrapper = RowWrapper::new(&sql, row);
                    return Ok(Some(row_fn(&row_wrapped)?));
                }
                Err(err) => {
                    return Err(Error::SqlError {
                        sql: String::from(sql.as_ref()),
                        backtrace: Backtrace::new_unresolved(),
                        err
                    });
                }
            }
        }
        Err(err) => {
            return Err(Error::SqlError {
                sql: String::from(sql.as_ref()),
                backtrace: Backtrace::new_unresolved(),
                err
            });
        }
    }
}


/// Executes a single SQL statement.
pub fn sql_execute<S, P>(conn: &Connection, sql: S, params: P) -> Result<usize, Error> where S : AsRef<str>, P : Params {
    match conn.execute(sql.as_ref(), params) {
        Ok(value) => Ok(value),
        Err(err) => {
            Err(Error::SqlError { 
                sql: String::from(sql.as_ref()), 
                backtrace: Backtrace::new_unresolved(), 
                err 
            })
        }
    }
}

/// Executes a batch of SQL statements.
pub fn sql_execute_batch<S>(conn: &Connection, sql: S) -> Result<(), Error> where S : AsRef<str> {
    match conn.execute_batch(sql.as_ref()) {
        Ok(value) => Ok(value),
        Err(err) => {
            Err(Error::SqlError { 
                sql: String::from(sql.as_ref()), 
                backtrace: Backtrace::new_unresolved(), 
                err 
            })
        }
    }
}