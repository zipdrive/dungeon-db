pub enum Primitive {
    Text,
    Integer,
    Number,
    Checkbox,
    Date,
    Datetime,
    File,
    Image,
    JSON
}

impl Primitive {
    /// Gets the OID of the primitive type.
    fn get_oid(&self) -> i64 {
        match self {
            Self::Text => -1,
            Self::Integer => -2,
            Self::Number => -3,
            Self::Checkbox => -4,
            Self::Date => -5,
            Self::Datetime => -6,
            Self::File => -7,
            Self::Image => -8,
            Self::JSON => -9
        }
    }
}

pub enum ColumnType {
    Formula {
        oid: i64,
        formula: String
    },
    Subreport {
        oid: i64,
        report_oid: i64
    },
    Primitive(Primitive),
    Object {
        oid: i64,
        table_oid: i64
    },
    Select {
        oid: i64,
        table_oid: i64 
    },
    Multiselect {
        oid: i64,
        table_oid: i64
    }
}

impl ColumnType {
    pub fn get(oid: i64) -> Result<Self, Error> {
        let conn = db::open()?;
        let metadata: Self = conn.query_one(

        )?;
        return Ok()
    }

    /// Find the column type matching the metadata.
    pub fn find(self) -> Result<Self, Error> {
        let mut conn = db::open()?;

        match self {
            Self::Formula { formula, .. } => {
                let trans = conn.transaction()?;

                // Create the column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                let oid: i64 = trans.last_insert_rowid();

                // Create the formula column type metadata
                trans.execute("INSERT INTO METADATA_COLUMN_TYPE__FORMULA (OID, FORMULA) VALUES (?1, ?2)", params![oid, formula])?;

                // Commit the transaction
                trans.commit()?;
                return Ok(Self::Formula {
                    oid,
                    formula
                });
            }
            Self::Subreport { report_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__SUBREPORT WHERE REPORT_OID = ?1",
                    params![report_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Subreport {
                            oid,
                            report_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the subreport column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__SUBREPORT (OID, REPORT_OID) VALUES (?1, ?2)", params![oid, report_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Subreport {
                            oid,
                            report_oid
                        });
                    }
                }
            }
            Self::Primitive => {
                return Ok(self);
            }
            Self::Object { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__OBJECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Object {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the object column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__OBJECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Object {
                            oid,
                            table_oid
                        });
                    }
                }
            }
            Self::Select { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__SELECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the select column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__SELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                }
            }
            Self::Multiselect { table_oid, .. } => {
                match conn.query_one(
                    "SELECT OID FROM METADATA_COLUMN_TYPE__MULTISELECT WHERE TABLE_OID = ?1",
                    params![table_oid],
                    |row| row.get(0)
                )? {
                    Some(oid) => {
                        return Ok(Self::Multiselect {
                            oid,
                            table_oid
                        });
                    }
                    None => {
                        let trans = conn.transaction()?;

                        // Create the column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE DEFAULT VALUES", [])?;
                        let oid: i64 = trans.last_insert_rowid();

                        // Create the multiselect column type metadata
                        trans.execute("INSERT INTO METADATA_COLUMN_TYPE__MULTISELECT (OID, TABLE_OID) VALUES (?1, ?2)", params![oid, table_oid])?;

                        // Commit the transaction
                        trans.commit()?;
                        return Ok(Self::Select {
                            oid,
                            table_oid
                        });
                    }
                }
            }
        }
    }
}