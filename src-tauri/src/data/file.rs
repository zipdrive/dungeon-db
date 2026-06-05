use base64::{Engine, prelude::{BASE64_STANDARD as base64standard}};
use std::{collections::btree_map::Entry::Occupied, path::Path};
use std::fs::{File as FilesystemFile};
use std::io::{BufReader, Read, Write};
use serde::{Deserialize, Serialize};
use rusqlite::{Connection, params};
use crate::util::db;
use crate::util::error::Error;

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all="camelCase", rename_all_fields="camelCase")]
pub enum File {
    Path {
        oid: i64,
        path: String
    },
    Blob {
        oid: i64
    }
}

impl File {
    /// Retrieve the file with the given OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::get_transact(&conn, oid) 
    }

    /// Retrieve the file with the given OID.
    pub fn get_transact(conn: &Connection, oid: i64) -> Result<Self, Error> {
        let (oid, path) = conn.query_one(
            "
            SELECT
                OID,
                NULL AS FILEPATH
            FROM METADATA_FILE__BLOB
            WHERE OID = ?1

            UNION

            SELECT
                OID,
                FILEPATH
            FROM METADATA_FILE__PATH
            WHERE OID = ?1
            ", 
            params![oid], 
            |row| Ok::<(i64, Option<String>), rusqlite::Error>((row.get("OID")?, row.get("FILEPATH")?))
        )?;
        Ok(match path {
            Some(path) => Self::Path { oid, path },
            None => Self::Blob { oid }
        })
    }

    /// Loads the file as a URI (e.g. for an img tag).
    pub fn get_image_src(self) -> Result<String, Error> {
        let conn = db::open()?;
        self.get_image_src_transact(&conn)
    }

    /// Loads the file as a URI (e.g. for an img tag).
    pub fn get_image_src_transact(self, conn: &Connection) -> Result<String, Error> {
        // Load file content into buffer
        match self {
            Self::Path { path, .. } => {
                return Ok(path.clone());
            }
            Self::Blob { oid } => {
                let blob = conn.blob_open("main", "METADATA_FILE__BLOB", "CONTENT", oid, true)?;
                
                // Read the BLOB into a buffer
                let mut buf: Vec<u8> = Vec::new();
                let mut buf_reader = BufReader::new(blob);
                match buf_reader.read_to_end(&mut buf) {
                    Ok(_) => {},
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to read stored file."));
                    }
                }
                
                // Read the MIME type to ensure that the file is an image 
                let mime_type = mimetype_detector::detect(&buf);
                if mimetype_detector::MimeKind::IMAGE == mime_type.kind() {
                    return Ok(format!("data:{};base64,{}", mime_type.name(), base64standard.encode(&buf)));
                } else {
                    return Err(Error::AdhocError("File is not an image!"));
                }
            }
        }
    }

    /// Loads the file as a base64 string.
    pub fn into_base64(self) -> Result<String, Error> {
        let conn = db::open()?;
        
        // Load file content into buffer
        let buf: Vec<u8> = match self {
            Self::Path { path, .. } => {
                match std::fs::read(path) {
                    Ok(read_buf) => read_buf,
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to open file."));
                    }
                }
            }
            Self::Blob { oid } => {
                let blob = conn.blob_open("main", "METADATA_FILE__BLOB", "CONTENT", oid, true)?;
                
                // Read the BLOB into a buffer
                let mut buf: Vec<u8> = Vec::new();
                let mut buf_reader = BufReader::new(blob);
                match buf_reader.read_to_end(&mut buf) {
                    Ok(_) => {},
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to read stored file."));
                    }
                }
                buf
            }
        };

        // Encode buffer into base64
        return Ok(base64standard.encode(&buf));
    }

    /// Download a file to a location in the local filesystem.
    pub fn download(self, download_to_path: String) -> Result<(), Error> {
        // Load the file content to a buffer
        let buf: Vec<u8> = match self {
            Self::Path { path, .. } => {
                // Read the file into a buffer
                match std::fs::read(path) {
                    Ok(read_buf) => read_buf,
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to open file."));
                    }
                }
            }
            Self::Blob { oid } => {
                // Create the BLOB
                let conn = db::open()?;
                let blob = conn.blob_open("main", "METADATA_FILE__BLOB", "CONTENT", oid, true)?;

                // Read the BLOB into a buffer
                let mut buf_reader = BufReader::new(blob);
                let mut buf: Vec<u8> = Vec::new();
                match buf_reader.read_to_end(&mut buf) {
                    Ok(_) => {},
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to read stored file."));
                    }
                }
                buf
            }
        };

        // Load the file from the filesystem
        let mut file = match FilesystemFile::create(download_to_path) {
            Ok(f) => f,
            Err(_) => {
                return Err(Error::AdhocError("Unable to open file."));
            }
        };

        // Write the contents of the buffer into the file
        match file.write_all(&buf) {
            Ok(_) => {},
            Err(_) => {
                return Err(Error::AdhocError("Unable to write to file."));
            }
        }
        return Ok(());
    }

    /// Upload a file from the local filesystem.
    pub fn upload(&mut self, upload_from_path: String) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        // Create a file
        trans.execute("INSERT INTO METADATA_FILE DEFAULT VALUES", [])?;

        match self {
            Self::Path { oid, path } => {
                // Update the file OID and path
                *oid = trans.last_insert_rowid();
                *path = upload_from_path;

                // Insert a new path
                trans.execute("INSERT INTO METADATA_FILE__PATH (OID, FILEPATH) VALUES (?1, ?2)", params![*oid, *path])?;
            }
            Self::Blob { oid } => {
                // Update the file OID
                *oid = trans.last_insert_rowid();

                // Crop the filepath down to the file name
                let name: String = {
                    let path = Path::new(&upload_from_path);
                    match path.file_name() {
                        Some(n) => String::from(n.to_str().unwrap_or("")),
                        None => String::from("")
                    }
                };

                // Load the file from the filesystem
                let buf = match std::fs::read(upload_from_path) {
                    Ok(read_buf) => read_buf,
                    Err(_) => {
                        return Err(Error::AdhocError("Unable to open file."));
                    }
                };
                let cropped_file_len: i64 = match i64::try_from(buf.len()) {
                    Ok(len) => len,
                    Err(_) => {
                        return Err(Error::AdhocError("File size is greater than 9,223,372,036,854,775,807 bytes."));
                    }
                };

                // Update the value with an empty blob
                trans.execute(
                    "INSERT INTO METADATA_FILE__BLOB (OID, FILENAME, CONTENT) VALUES (?1, ?2, ZEROBLOB(?3))", 
                    params![*oid, name, cropped_file_len]
                )?;

                // Fill the empty blob with the data from the file
                {
                    let mut blob = trans.blob_open("main", "METADATA_FILE__BLOB", "CONTENT", *oid, false)?;
                    match blob.write_all(&buf) {
                        Ok(_) => {},
                        Err(_) => {
                            return Err(Error::AdhocError("Unable to upload file contents to database."));
                        }
                    }
                }
            }
        }

        // Commit the transaction
        trans.commit()?;
        return Ok(());
    }
}