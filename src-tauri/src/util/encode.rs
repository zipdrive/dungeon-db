/// Encodes a literal string to make it safe for inserting inside a literal SQL string.
pub fn sql_encode_string(str: &String) -> String {
    str.replace("'", "''")
}

/// Encodes a literal string to make it safe for inserting into a JSON double-quoted string inside a literal SQL string.
pub fn sql_json_encode_string(str: &String) -> String {
    str.replace("'", "''")
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
}

/// Encodes a literal string to make it safe for inserting into a JSON double-quoted string.
pub fn json_encode_string(str: &String) -> String {
    str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
}

/// Encodes an SQL expression returning TEXT to make it safe for inserting into a JSON double-quoted string.
pub fn sql_json_encode_expr(expr: &String) -> String {
    format!("REPLACE(REPLACE({expr}, '\\', '\\\\'), '\"', '\\\"')")
}