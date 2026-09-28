use rusqlite::Connection;

use crate::data::file::File; 
use crate::data::table::TableMetadata;
use crate::data::table::row::{TableCell, TableCellContent, TableCellTextContentFormat, TableRow};
use crate::util::encode::json_encode_string;
use crate::util::error::Error;

pub enum ValueType {
    /// A union of multiple value types.
    Union(Vec<ValueType>),

    /// A boolean true/false value.
    Boolean,

    /// A date with no time information.
    Date,

    /// A date and time.
    Datetime,

    /// A floating-point number.
    Number,

    /// An integer.
    Integer,

    /// A plain text value.
    Text,

    /// A JSON text value.
    TextJson,

    /// An XML text value
    TextXml,

    /// A Markdown text value.
    TextMarkdown,

    /// A BBCode text value.
    TextBBCode,

    /// A file.
    File,

    /// A row in a particular table.
    Record {
        table_oid: i64 
    },

    /// A list of multiple values.
    List(Box<ValueType>)
}

impl ValueType {
    /// Returns true if the value can be coerced to a boolean true/false value.
    pub fn is_boolean(&self) -> bool {
        match self {
            Self::Boolean => true,
            _ => false
        }
    }

    /// Returns true if the value can be coerced to a floating-point number.
    pub fn is_number(&self) -> bool {
        match self {
            Self::Number 
            | Self::Integer => true,
            _ => false 
        }
    }

    /// Returns true if the value can be coerced to an integer.
    pub fn is_integer(&self) -> bool {
        match self {
            Self::Integer => true,
            _ => false 
        }
    }

    /// Returns true if the value can be coerced to text.
    pub fn is_text(&self) -> bool {
        match self {
            Self::Text 
            | Self::TextJson 
            | Self::TextXml
            | Self::TextMarkdown
            | Self::TextBBCode => true,
            _ => false
        }
    }

    /// Returns true if the value can be coerced to a file.
    pub fn is_file(&self) -> bool {
        match self {
            Self::File => true,
            _ => false 
        }
    }

    /// Returns true if the value can be coerced to a record.
    pub fn is_record(&self) -> bool {
        match self {
            Self::Record { .. } => true,
            _ => false 
        }
    }
}


#[derive(Clone)]
pub struct TableCellReference {
    pub table_oid: i64,
    pub column_oid: i64,
    pub row_oid: i64
}

pub struct TextValue {
    pub text: String,
    pub format: TableCellTextContentFormat
}

pub struct RecordValue {
    pub table_oid: i64,
    pub table_name: String,
    pub row: TableRow
}

pub enum Value {
    Null {
        reference: Option<TableCellReference>
    },
    Boolean {
        value: bool,
        reference: Option<TableCellReference>
    },
    Date {
        value: i64,
        reference: Option<TableCellReference>
    },
    Datetime {
        value: f64,
        reference: Option<TableCellReference>
    },
    Integer {
        value: i64,
        reference: Option<TableCellReference>
    },
    Number {
        value: f64,
        reference: Option<TableCellReference>
    },
    Text {
        value: TextValue,
        reference: Option<TableCellReference>
    },
    FileCellReference {
        value: File,
        reference: TableCellReference
    },
    ObjectCellReference {
        table_oid: i64,
        value: i64,
        reference: TableCellReference
    },
    SingleSelectCellReference {
        table_oid: i64,
        value: i64,
        reference: TableCellReference
    },
    MultiSelectCellReference {
        table_oid: i64,
        value: Vec<i64>,
        reference: TableCellReference
    },
    List(Vec<Value>)
}

impl Value {
    pub fn new_null() -> Self {
        Self::Null { reference: None }
    }

    /// 
    pub fn from_cell(cell: &TableCell) -> Result<Self, Error> {
        let reference: Option<TableCellReference> = Some(TableCellReference {
            table_oid: cell.table_oid,
            column_oid: cell.column_oid,
            row_oid: cell.row_oid
        });
        Ok(match &cell.content {
            TableCellContent::Boolean { value } => Self::Boolean { value: value.clone(), reference },
            TableCellContent::Date { value, .. } => match value {
                Some(value) => Self::Date { value: value.clone(), reference },
                None => Self::Null { reference }
            },
            TableCellContent::Datetime { value, .. } => match value {
                Some(value) => Self::Datetime { value: value.clone(), reference },
                None => Self::Null { reference }
            },
            TableCellContent::File { value } => match value {
                Some(value) => Self::FileCellReference { value: value.clone(), reference: reference.unwrap() },
                None => Self::Null { reference }
            },
            TableCellContent::Integer { value } => match value {
                Some(value) => Self::Integer { value: value.clone(), reference },
                None => Self::Null { reference }
            },
            TableCellContent::Number { value } => match value {
                Some(value) => Self::Number { value: value.clone(), reference },
                None => Self::Null { reference }
            },
            TableCellContent::Text { value, format } => match value {
                Some(value) => Self::Text { value: TextValue { text: value.clone(), format: format.clone() }, reference },
                None => Self::Null { reference }
            },
            TableCellContent::Object { table_oid, value } => match value {
                Some(value) => Self::ObjectCellReference { table_oid: table_oid.clone(), value: value.clone(), reference: reference.unwrap() },
                None => Self::Null { reference }
            }
            TableCellContent::SingleSelectDropdown { table_oid, value } => match value {
                Some(value) => Self::SingleSelectCellReference { table_oid: table_oid.clone(), value: value.clone(), reference: reference.unwrap() },
                None => Self::Null { reference }
            },
            TableCellContent::MultiSelectDropdown { table_oid, value } => Self::MultiSelectCellReference { 
                table_oid: table_oid.clone(), 
                value: value.clone(), 
                reference: reference.unwrap() 
            },
            TableCellContent::Subreport { .. } => {
                return Err(Error::adhoc("Drill-Down Reports cannot be used in formulas."));
            }
        })
    }

    /// Converts the value into a non-optional bool.
    pub fn into_bool<S>(self, fn_id: S) -> Result<bool, Error> where S : AsRef<str> {
        match self {
            Self::Boolean { value, .. } => Ok(value),
            _ => Err(Error::adhoc(format!("Function {} expected a Boolean value, but received a {} value.", fn_id.as_ref(), self.to_string())))
        }
    }

    /// Converts the value into a nullable integer.
    pub fn into_i64<S>(self, fn_id: S) -> Result<Option<i64>, Error> where S : AsRef<str> {
        match self {
            Self::Null { .. } => Ok(None),
            Self::Integer { value, .. } => Ok(Some(value)),
            _ => Err(Error::adhoc(format!("Function {} expected an Integer value, but received a {} value.", fn_id.as_ref(), self.to_string())))
        }
    }

    /// Converts the value into a nullable floating-point value.
    pub fn into_f64<S>(self, fn_id: S) -> Result<Option<f64>, Error> where S : AsRef<str> {
        match self {
            Self::Null { .. } => Ok(None),
            Self::Integer { value, .. } => Ok(Some(value as f64)),
            Self::Number { value, .. } => Ok(Some(value)),
            _ => Err(Error::adhoc(format!("Function {} expected a Number value, but received a {} value.", fn_id.as_ref(), self.to_string())))
        }
    }

    /// Converts the value into a nullable File.
    pub fn into_file<S>(self, fn_id: S) -> Result<Option<File>, Error> where S : AsRef<str> {
        match self {
            Self::Null { .. } => Ok(None),
            Self::FileCellReference { value, .. } => Ok(Some(value)),
            _ => Err(Error::adhoc(format!("Function {} expected a File value, but received a {} value.", fn_id.as_ref(), self.to_string())))
        }
    }

    /// Converts the value into a nullable TextValue.
    pub fn into_text<S>(self, fn_id: S) -> Result<Option<TextValue>, Error> where S : AsRef<str> {
        match self {
            Self::Null { .. } => Ok(None),
            Self::Text { value, .. } => Ok(Some(value)),
            _ => Err(Error::adhoc(format!("Function {} expected a Text value, but received a {} value.", fn_id.as_ref(), self.to_string())))
        }
    }


    /// Describes the type of the Value.
    fn to_string(&self) -> String {
        match self {
            Self::Null { reference, .. } => if let Some(_) = reference { String::from("NullRef") } else { String::from("Null") },
            Self::Boolean { reference, .. } => if let Some(_) = reference { String::from("Ref<Boolean>") } else { String::from("Boolean") },
            Self::Date { reference, .. } => if let Some(_) = reference { String::from("Ref<Date>") } else { String::from("Date") },
            Self::Datetime { reference, .. } => if let Some(_) = reference { String::from("Ref<Datetime>") } else { String::from("Datetime") },
            Self::Integer { reference, .. } => if let Some(_) = reference { String::from("Ref<Integer>") } else { String::from("Integer") },
            Self::Number { reference, .. } => if let Some(_) = reference { String::from("Ref<Number>") } else { String::from("Number") },
            Self::List(list) => {
                String::from("List") // TODO
            }
            Self::Text { reference, value } => {
                let base = match value.format {
                    TableCellTextContentFormat::Plain => String::from("Text"),
                    TableCellTextContentFormat::Json => String::from("Json"),
                    TableCellTextContentFormat::Xml => String::from("Xml"),
                    TableCellTextContentFormat::Markdown => String::from("Markdown"),
                    TableCellTextContentFormat::BBCode => String::from("BBCode")
                };
                if let Some(_) = reference { format!("Ref<{}>", base) } else { base }
            }
            Self::FileCellReference { .. } => String::from("Ref<File>"),
            Self::ObjectCellReference { .. } => String::from("Ref<Object>"),
            Self::SingleSelectCellReference { .. } => String::from("Ref<SingleSelectDropdown>"),
            Self::MultiSelectCellReference { .. } => String::from("Ref<MultiSelectDropdown>")
        }
    }
}