/// Encodes a string to make it safe for inserting inside an SQL string.
pub fn sql_encode_string(str: &String) -> String {
    str.replace("'", "''")
}

/// Encodes a string to make it safe for inserting into a JSON double-quoted string inside an SQL string.
pub fn json_encode_string(str: &String) -> String {
    str.replace("'", "''")
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
}