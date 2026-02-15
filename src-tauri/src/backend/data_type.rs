use crate::backend::db;
use crate::util::error;
use rusqlite::{params, Transaction};
use serde::{Deserialize, Serialize};

#[derive(PartialEq)]
pub struct DropdownValue {
    pub oid: Option<i64>,
    pub value: String
}


#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum Primitive {
    Any,       // Mode = 0 && OID = 0
    Boolean,   // Mode = 0 && OID = 1
    Integer,   // Mode = 0 && OID = 2
    Number,    // Mode = 0 && OID = 3
    Date,      // Mode = 0 && OID = 4
    Timestamp, // Mode = 0 && OID = 5
    Text,      // Mode = 0 && OID = 6
    JSON,      // Mode = 0 && OID = 7
    File,      // Mode = 0 && OID = 8
    Image,     // Mode = 0 && OID = 9
}

impl Primitive {
    /// Gets the corresponding SQLite column type for a given primitive type.
    pub fn get_sqlite_type(&self) -> &'static str {
        return match self {
            Self::Any => "ANY",
            Self::Boolean => "INTEGER",
            Self::Integer => "INTEGER",
            Self::Number => "REAL",
            Self::Date => "REAL",
            Self::Timestamp => "REAL",
            Self::Text | Self::JSON => "TEXT",
            Self::File | Self::Image => "BLOB",
        };
    }

    /// Gets the corresponding type OID of a given primitive type.
    pub fn get_type_oid(&self) -> i64 {
        match self {
            Self::Any => 0,
            Self::Boolean => 1,
            Self::Integer => 2,
            Self::Number => 3,
            Self::Date => 4,
            Self::Timestamp => 5,
            Self::Text => 6,
            Self::JSON => 7,
            Self::File => 8,
            Self::Image => 9,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum MetadataColumnType {
    Primitive(Primitive),      // Mode = 0
    SingleSelectDropdown(i64), // Mode = 1
    MultiSelectDropdown(i64),  // Mode = 2
    Reference(i64),            // Mode = 3
    ChildObject(i64),          // Mode = 4
    ChildTable(i64),           // Mode = 5
}

impl MetadataColumnType {
    /// Converts a type from the database OID and mode.
    pub fn from_database(type_oid: i64, type_mode: i64) -> MetadataColumnType {
        match type_mode {
            0 => match type_oid {
                1 => {
                    return Self::Primitive(Primitive::Boolean);
                }
                2 => {
                    return Self::Primitive(Primitive::Integer);
                }
                3 => {
                    return Self::Primitive(Primitive::Number);
                }
                4 => {
                    return Self::Primitive(Primitive::Date);
                }
                5 => {
                    return Self::Primitive(Primitive::Timestamp);
                }
                6 => {
                    return Self::Primitive(Primitive::Text);
                }
                7 => {
                    return Self::Primitive(Primitive::JSON);
                }
                8 => {
                    return Self::Primitive(Primitive::File);
                }
                9 => {
                    return Self::Primitive(Primitive::Image);
                }
                _ => {
                    return Self::Primitive(Primitive::Any);
                }
            },
            1 => {
                return Self::SingleSelectDropdown(type_oid);
            }
            2 => {
                return Self::MultiSelectDropdown(type_oid);
            }
            3 => {
                return Self::Reference(type_oid);
            }
            4 => {
                return Self::ChildObject(type_oid);
            }
            5 => {
                return Self::ChildTable(type_oid);
            }
            _ => {
                return Self::Primitive(Primitive::Any);
            }
        }
    }

    /// Gets the corresponding type OID of a column type.
    pub fn get_type_oid(&self) -> i64 {
        return match self {
            Self::Primitive(prim) => prim.get_type_oid(),
            Self::SingleSelectDropdown(type_oid)
            | Self::MultiSelectDropdown(type_oid)
            | Self::Reference(type_oid)
            | Self::ChildObject(type_oid)
            | Self::ChildTable(type_oid) => type_oid.clone(),
        };
    }

    /// Gets the corresponding type mode of a column type.
    pub fn get_type_mode(&self) -> i64 {
        return match self {
            Self::Primitive(_) => 0,
            Self::SingleSelectDropdown(_) => 1,
            Self::MultiSelectDropdown(_) => 2,
            Self::Reference(_) => 3,
            Self::ChildObject(_) => 4,
            Self::ChildTable(_) => 5,
        };
    }

    /// If the type is unique to a specific table (i.e. single-select dropdown, multi-select dropdown, child table), creates rows/tables for the specified type.
    /// Returns the OID for the type.
    pub fn create_for_table(
        self,
        trans: &Transaction,
        table_oid: &i64,
        dropdown_values: Option<Vec<DropdownValue>>
    ) -> Result<Self, error::Error> {
        match self {
            Self::Primitive(_) | Self::Reference(_) | Self::ChildObject(_) => {
                return Ok(self);
            }
            Self::SingleSelectDropdown(_) => {
                // Create the column type, use that as the OID for the type
                trans.execute(
                    "INSERT INTO METADATA_TYPE (MODE) VALUES (?1);",
                    params![self.get_type_mode()],
                )?;
                let column_type_oid = trans.last_insert_rowid();

                // Create table to store dropdown values
                let create_table_cmd = format!("CREATE TABLE TABLE{column_type_oid} (OID INTEGER PRIMARY KEY, TRASH TINYINT NOT NULL DEFAULT 0, VALUE TEXT NOT NULL);");
                trans.execute(&create_table_cmd, [])?;

                // Insert options into table of possible dropdown values
                if dropdown_values == None {
                    return Err(error::Error::AdhocError("Dropdown values must be given for a single-select dropdown column."));
                }
                for dropdown_value in dropdown_values.unwrap().iter() {
                    match dropdown_value.oid {
                        Some(o) => {
                            let insert_cmd = format!("INSERT INTO TABLE{column_type_oid} (OID, VALUE) VALUES (?1, ?2)");
                            trans.execute(&insert_cmd, params![o, dropdown_value.value]);
                        },
                        None => {
                            let insert_cmd = format!("INSERT INTO TABLE{column_type_oid} (VALUE) VALUES (?1)");
                            trans.execute(&insert_cmd, params![dropdown_value.value]);
                        }
                    };
                }

                // Return the OID of the created type
                return Ok(Self::SingleSelectDropdown(column_type_oid));
            }
            Self::MultiSelectDropdown(_) => {
                // Create the column type, use that as the OID for the type
                trans.execute(
                    "INSERT INTO METADATA_TYPE (MODE) VALUES (?1);",
                    params![self.get_type_mode()],
                )?;
                let column_type_oid = trans.last_insert_rowid();

                // Create table to store dropdown values
                let create_table_cmd = format!("CREATE TABLE TABLE{column_type_oid} (OID INTEGER PRIMARY KEY, TRASH TINYINT NOT NULL DEFAULT 0, VALUE TEXT NOT NULL);");
                trans.execute(&create_table_cmd, [])?;

                // Create table to store relationship with base table
                let create_relationship_cmd = format!(
                    "
                CREATE TABLE TABLE{column_type_oid}_MULTISELECT (
                    ROW_OID INTEGER REFERENCES TABLE{table_oid} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE CASCADE, 
                    VALUE_OID INTEGER REFERENCES TABLE{column_type_oid} (OID) 
                        ON UPDATE CASCADE 
                        ON DELETE CASCADE, 
                    PRIMARY KEY (ROW_OID, VALUE_OID)
                );"
                );
                trans.execute(&create_relationship_cmd, [])?;

                // Insert options into table of possible dropdown values
                if dropdown_values == None {
                    return Err(error::Error::AdhocError("Dropdown values must be given for a multi-select dropdown column."));
                }
                for dropdown_value in dropdown_values.unwrap().iter() {
                    match dropdown_value.oid {
                        Some(o) => {
                            let insert_cmd = format!("INSERT INTO TABLE{column_type_oid} (OID, VALUE) VALUES (?1, ?2)");
                            trans.execute(&insert_cmd, params![o, dropdown_value.value]);
                        },
                        None => {
                            let insert_cmd = format!("INSERT INTO TABLE{column_type_oid} (VALUE) VALUES (?1)");
                            trans.execute(&insert_cmd, params![dropdown_value.value]);
                        }
                    };
                }

                // Return the OID of the created type
                return Ok(Self::MultiSelectDropdown(column_type_oid));
            }
            Self::ChildTable(_) => {
                // Create the column type, use that as the OID for the type
                trans.execute(
                    "INSERT INTO METADATA_TYPE (MODE) VALUES (?1);",
                    params![self.get_type_mode()],
                )?;
                let column_type_oid = trans.last_insert_rowid();

                // Add metadata for the child table
                let child_table_name: String = format!("TABLE{column_type_oid}");
                trans.execute(
                    "INSERT INTO METADATA_TABLE (OID, PARENT_TABLE_OID, NAME) VALUES (?1, ?2, ?3);",
                    params![column_type_oid, table_oid, &child_table_name],
                )?;

                // Create the child table
                let create_table_cmd = format!(
                    "
                CREATE TABLE TABLE{column_type_oid} (
                    OID INTEGER PRIMARY KEY, 
                    TRASH BOOLEAN NOT NULL DEFAULT 0,
                    PARENT_OID INTEGER NOT NULL REFERENCES TABLE{table_oid} (OID)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE
                );"
                );
                trans.execute(&create_table_cmd, [])?;

                // Create a surrogate view for the child table
                let create_view_cmd = format!("
                CREATE VIEW TABLE{column_type_oid}_SURROGATE (OID, DISPLAY_VALUE) 
                AS 
                SELECT 
                    OID, 
                    CASE WHEN TRASH = 0 THEN '— NO PRIMARY KEY —' ELSE '— DELETED —' END AS DISPLAY_VALUE 
                FROM TABLE{column_type_oid};");
                trans.execute(&create_view_cmd, [])?;

                // Return the OID of the created type
                return Ok(Self::ChildTable(column_type_oid));
            }
        }
    }

    // Updates the dropdown values for a dropdown-typed column.
    pub fn edit_dropdown_values(&self, trans: &Transaction, dropdown_values: Option<Vec<DropdownValue>>) -> Result<(), error::Error> {
        match &self {
            Self::SingleSelectDropdown(type_oid) | Self::MultiSelectDropdown(type_oid) => {
                // Make sure dropdown values were actually provided
                if dropdown_values == None {
                    return Err(error::Error::AdhocError("Dropdown values must be given for a dropdown-typed column."));
                }

                // Mark all former possible dropdown values as trash
                let update_cmd = format!("UPDATE TABLE{type_oid} SET TRASH = 1");
                trans.execute(&update_cmd, [])?;

                // Insert options into table of possible dropdown values
                for dropdown_value in dropdown_values.unwrap().iter() {
                    match dropdown_value.oid {
                        Some(o) => {
                            let insert_cmd = format!("INSERT INTO TABLE{type_oid} (OID, VALUE) VALUES (?1, ?2) ON CONFLICT DO UPDATE SET TRASH = 0, VALUE = excluded.VALUE");
                            trans.execute(&insert_cmd, params![o, dropdown_value.value]);
                        },
                        None => {
                            let insert_cmd = format!("INSERT INTO TABLE{type_oid} (VALUE) VALUES (?1)");
                            trans.execute(&insert_cmd, params![dropdown_value.value]);
                        }
                    };
                }
            },
            _ => {}
        }

        return Ok(());
    }

    /// If the type is unique to a specific table (i.e. single-select dropdown, multi-select dropdown, child table), deletes the type and any associated rows/tables.
    pub fn delete_for_table(self, trans: &Transaction) -> Result<(), error::Error> {
        match self {
            Self::Primitive(_) | Self::Reference(_) | Self::ChildObject(_) => {
                return Ok(());
            }
            Self::SingleSelectDropdown(column_type_oid) => {
                // Drop the dropdown values table
                let drop_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                trans.execute(&drop_cmd, [])?;

                // Delete the dropdown type from the metadata
                trans.execute(
                    "DELETE FROM METADATA_TYPE WHERE OID = ?1",
                    params![column_type_oid],
                )?;

                return Ok(());
            }
            Self::MultiSelectDropdown(column_type_oid) => {
                // Drop the relationship table
                let drop_relationship_cmd =
                    format!("DROP TABLE TABLE{column_type_oid}_MULTISELECT;");
                trans.execute(&drop_relationship_cmd, [])?;

                // Drop the dropdown values table
                let drop_values_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                trans.execute(&drop_values_cmd, [])?;

                // Delete the dropdown value table from the metadata
                trans.execute(
                    "DELETE FROM METADATA_TYPE WHERE OID = ?1",
                    params![column_type_oid],
                )?;

                return Ok(());
            }
            Self::ChildTable(column_type_oid) => {
                // Drop the surrogate view
                let drop_view_cmd = format!("DROP VIEW TABLE{column_type_oid}_SURROGATE;");
                trans.execute(&drop_view_cmd, [])?;

                // Drop the child table
                let drop_table_cmd = format!("DROP TABLE TABLE{column_type_oid};");
                trans.execute(&drop_table_cmd, [])?;

                // Delete the metadata for the child type, which will cascade to delete the metadata for the child table
                trans.execute(
                    "DELETE FROM METADATA_TYPE WHERE OID = ?1",
                    params![column_type_oid],
                )?;

                return Ok(());
            }
        }
    }
}
