use crate::data::file::File; 
use crate::data::table::row::TableCellTextContentFormat;
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
    pub row_oid: i64
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
    File {
        value: File,
        reference: Option<TableCellReference>
    },
    Record {
        value: RecordValue,
        reference: Option<TableCellReference>
    },
    List(Vec<Value>)
}

impl Value {
    pub fn new_null() -> Self {
        Self::Null { reference: None }
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
            Self::File { value, .. } => Ok(Some(value)),
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
            Self::Null { .. } => String::from("Null"),
            Self::Boolean { .. } => String::from("Boolean"),
            Self::Date { .. } => String::from("Date"),
            Self::Datetime { .. } => String::from("Datetime"),
            Self::File { .. } => String::from("File"),
            Self::Integer { .. } => String::from("Integer"),
            Self::List(list) => {
                String::from("List") // TODO
            }
            Self::Number { .. } => String::from("Number"),
            Self::Record { value, .. } => format!("\"{}\"", json_encode_string(&value.table_name)),
            Self::Text { value, .. } => match value.format {
                TableCellTextContentFormat::Plain => String::from("Text"),
                TableCellTextContentFormat::Json => String::from("Json"),
                TableCellTextContentFormat::Xml => String::from("Xml"),
                TableCellTextContentFormat::Markdown => String::from("Markdown"),
                TableCellTextContentFormat::BBCode => String::from("BBCode")
            }
        }
    }
}