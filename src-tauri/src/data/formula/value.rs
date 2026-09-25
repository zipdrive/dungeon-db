use crate::data::{file::File, table::row::TableCellTextContentFormat};

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



pub enum Value {
    Null,
    Boolean(bool),
    Date(i64),
    Datetime(f64),
    Integer(i64),
    Number(f64),
    Text {
        value: String,
        format: TableCellTextContentFormat
    },
    File(File),
    Record {
        table_oid: i64,
        row_oid: i64
    },
    List(Vec<Value>)
}