use crate::util::db;
use crate::util::db::{sql_one, sql_execute};
use crate::util::error::Error;
use base64::{prelude::BASE64_STANDARD as base64standard, Engine};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::fs::File as FilesystemFile;
use std::io::{BufReader, Read, Write};
use std::os::windows::fs::MetadataExt;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum File {
    Path { 
        oid: i64, 
        name: String, 
        path: String,
    },
    Blob { 
        oid: i64,
        name: String,
        size: i64,
    }
}

impl File {
    /// The OID of the file.
    pub fn oid(&self) -> &i64 {
        match self {
            Self::Path { oid, .. }
            | Self::Blob { oid, .. } => oid 
        }
    }

    /// The name of the file.
    pub fn name(&self) -> &String {
        match self {
            Self::Path { name, .. }
            | Self::Blob { name, .. } => name
        }
    }

    /// Retrieve the file with the given OID.
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        Self::conn_get(&conn, oid)
    }

    /// Retrieve the file with the given OID.
    pub fn conn_get(conn: &Connection, oid: i64) -> Result<Self, Error> {
        sql_one(
            conn,
            "SELECT * FROM METADATA_FILE WHERE OID = ?1",
            params![oid],
            |row| {
                let oid: i64 = row.get::<_, i64>("OID")?;
                let name: String = row.get::<_, String>("FILE_NAME")?;
                let file_type: String = row.get::<_, String>("FILE_TYPE")?;
                Ok(if file_type == "path" {
                    Self::Path {
                        oid,
                        name,
                        path: row.get("FILE_PATH")?
                    }
                } else if file_type == "blob" {
                    Self::Blob { 
                        oid, 
                        name,
                        size: row.get("FILE_CONTENT_SIZE")?
                    }
                } else {
                    return Err(Error::adhoc(format!("Unknown file storage method \"{file_type}\"")));
                })
            }
        )
    }

    /// Loads the file as a URI (e.g. for an img tag).
    pub fn get_src(self) -> Result<String, Error> {
        let conn = db::open()?;
        self.conn_get_src(&conn)
    }

    /// Loads the file as a URI (e.g. for an img tag).
    pub fn conn_get_src(self, conn: &Connection) -> Result<String, Error> {
        // Load file content into buffer
        let buf: Vec<u8> = match self {
            Self::Path { path, .. } => match std::fs::read(path.clone()) {
                Ok(read_buf) => read_buf,
                Err(_) => {
                    return Err(Error::adhoc(format!("Unable to open file at \"{path}\"")));
                }
            },
            Self::Blob { oid, name, .. } => {
                let blob = conn.blob_open("main", "__METADATA_FILE_BLOB", "CONTENT", oid, true)?;

                // Read the BLOB into a buffer
                let mut buf: Vec<u8> = Vec::new();
                let mut buf_reader = BufReader::new(blob);
                match buf_reader.read_to_end(&mut buf) {
                    Ok(_) => {}
                    Err(_) => {
                        return Err(Error::adhoc(format!("Unable to read stored file \"{name}\"")));
                    }
                }
                buf
            }
        };

        // Read the MIME type to ensure that the file is an image
        let mime_type = mimetype_detector::detect(&buf);
        if mimetype_detector::MimeKind::IMAGE == mime_type.kind() {
            return Ok(format!(
                "data:{};base64,{}",
                mime_type.name(),
                base64standard.encode(&buf)
            ));
        } else {
            return Err(Error::adhoc("File is not an image!"));
        }
    }

    /// Determines the size of the file.
    pub fn get_size(&self) -> Result<i64, Error> {
        match self {
            Self::Path { path, .. } => {
                match std::fs::metadata(path) {
                    Ok(metadata) => {
                        let true_file_size = metadata.file_size();
                        if true_file_size > i64::MAX as u64 {
                            Err(Error::adhoc("File size is greater than 9,223,372,036,854,775,807 bytes."))
                        } else {
                            Ok(true_file_size as i64)
                        }
                    }
                    Err(_) => Err(Error::adhoc(""))
                }
            }
            Self::Blob { size, .. } => Ok(size.clone())
        }
    }

    /// Download a file to a location in the local filesystem.
    pub fn download(self, download_to_path: String) -> Result<(), Error> {
        // Load the file content to a buffer
        let buf: Vec<u8> = match self {
            Self::Path { path, .. } => {
                // Read the file into a buffer
                match std::fs::read(path.clone()) {
                    Ok(read_buf) => read_buf,
                    Err(_) => {
                        return Err(Error::adhoc(format!("Unable to open file at \"{path}\"")));
                    }
                }
            }
            Self::Blob { oid, name, .. } => {
                // Create the BLOB
                let conn = db::open()?;
                let blob = conn.blob_open("main", "__METADATA_FILE_BLOB", "CONTENT", oid, true)?;

                // Read the BLOB into a buffer
                let mut buf_reader = BufReader::new(blob);
                let mut buf: Vec<u8> = Vec::new();
                match buf_reader.read_to_end(&mut buf) {
                    Ok(_) => {}
                    Err(_) => {
                        return Err(Error::adhoc(format!("Unable to read stored file \"{name}\"")));
                    }
                }
                buf
            }
        };

        // Load the file from the filesystem
        let mut file = match FilesystemFile::create(download_to_path) {
            Ok(f) => f,
            Err(_) => {
                return Err(Error::adhoc("Unable to open file."));
            }
        };

        // Write the contents of the buffer into the file
        match file.write_all(&buf) {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::adhoc("Unable to write to file."));
            }
        }
        return Ok(());
    }

    /// Upload a file from the local filesystem.
    pub fn upload(&mut self, upload_from_path: String) -> Result<(), Error> {
        let mut conn = db::open()?;
        let trans = conn.transaction()?;

        match self {
            Self::Path { oid, name, path } => {
                // Create a file
                *name = String::from(Path::new(&upload_from_path).file_name().unwrap().to_str().unwrap());
                sql_execute(&trans, "INSERT INTO __METADATA_FILE (NAME) VALUES (?1)", params![*name])?;

                // Update the file OID and path
                *oid = trans.last_insert_rowid();
                *path = upload_from_path;

                // Insert a new path
                sql_execute(
                    &trans,
                    "
                    INSERT INTO __METADATA_FILE_PATH 
                        (OID, PATH) 
                        VALUES 
                        (?1, ?2)
                    ",
                    params![*oid, *path],
                )?;
            }
            Self::Blob { oid, name, .. } => {
                // Create a file
                *name = String::from(Path::new(&upload_from_path).file_name().unwrap().to_str().unwrap());
                sql_execute(&trans, "INSERT INTO __METADATA_FILE (NAME) VALUES (?1)", params![*name])?;

                // Update the file OID
                *oid = trans.last_insert_rowid();

                // Load the file from the filesystem
                let buf = match std::fs::read(upload_from_path.clone()) {
                    Ok(read_buf) => read_buf,
                    Err(_) => {
                        return Err(Error::adhoc(format!("Unable to open file at \"{upload_from_path}\"")));
                    }
                };
                let cropped_file_len: i64 = match i64::try_from(buf.len()) {
                    Ok(len) => len,
                    Err(_) => {
                        return Err(Error::adhoc(
                            "File size is greater than 9,223,372,036,854,775,807 bytes.",
                        ));
                    }
                };

                // Update the value with an empty blob
                sql_execute(
                    &trans,
                    "
                    INSERT INTO __METADATA_FILE_BLOB 
                        (OID, CONTENT) 
                        VALUES 
                        (?1, ZEROBLOB(?2))
                    ", 
                    params![*oid, cropped_file_len]
                )?;

                // Fill the empty blob with the data from the file
                {
                    let mut blob =
                        trans.blob_open("main", "__METADATA_FILE_BLOB", "CONTENT", *oid, false)?;
                    match blob.write_all(&buf) {
                        Ok(_) => {}
                        Err(_) => {
                            return Err(Error::adhoc(
                                "Unable to upload file contents to database.",
                            ));
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
