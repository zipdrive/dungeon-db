use std::collections::HashMap;

use regex::Regex;
use rusqlite::Connection;

use crate::data::table::TableMetadata;
use crate::data::table::column::TableColumnMetadata;
use crate::data::formula::func::{Func, RecordFunc};
use crate::data::table::column_type::TableColumnType;
use crate::util::db;
use crate::util::error::Error;

pub mod query;
pub mod context;
pub mod func;
pub mod value;


const OR_PRECEDENCE: usize = 0;
const AND_PRECEDENCE: usize = 1;
const NOT_PRECEDENCE: usize = 2;
const EQ_PRECEDENCE: usize = 3;
const IN_PRECEDENCE: usize = 3;
const LT_PRECEDENCE: usize = 4;
const LTEQ_PRECEDENCE: usize = 4;
const ADD_PRECEDENCE: usize = 7;
const SUBTRACT_PRECEDENCE: usize = 7;
const MULTIPLY_PRECEDENCE: usize = 8;
const DIVIDE_PRECEDENCE: usize = 8;
const MODULO_PRECEDENCE: usize = 8;
const CONCAT_PRECEDENCE: usize = 9;


enum Expression {
    Func(Func),
    Ambiguous {
        record: RecordFunc,
        table_oid: i64,
        column: TableColumnMetadata
    },
    RecordFunc(RecordFunc)
}

impl Expression {
    /// Converts expression into a Func.
    pub fn into_func(self) -> Result<Func, Error> {
        match self {
            Self::Func(func) => Ok(func),
            Self::Ambiguous { record, table_oid, column } => Ok(Func::Column { record, table_oid, column }),
            Self::RecordFunc(rfunc) => Err(Error::adhoc(format!("Could not convert ({}) into an expression.", rfunc.formula())))
        }
    }

    /// Converts the expression into a RecordFunc.
    fn into_record(self) -> Result<RecordFunc, Error> {
        match self {
            Self::RecordFunc(rfunc) => Ok(rfunc),
            Self::Ambiguous { record, column, .. } => Ok(RecordFunc::Reference { record: Box::new(record), column }),
            Self::Func(func) => Err(Error::adhoc(format!("Cannot treat ({}) as a record.", func.formula())))
        }
    }



    /// Rotates the order of evaluation of binary operations, according to the rules laid out in Formula::binary_operator_precedence().
    fn binary_operator_rotate<F: FnOnce(Func, Func) -> Func>(
        outer_precedence: usize,
        lhs: Func,
        rhs: Func,
        construct_operator: F,
    ) -> Self {
        if let Some(rhs_precedence) = match &rhs {
            Func::Or(_, _) => Some(OR_PRECEDENCE),
            Func::And(_, _) => Some(AND_PRECEDENCE),
            Func::Not(_) => Some(NOT_PRECEDENCE),
            //Self::Eq(_, _) => Some(EQ_PRECEDENCE),
            //Self::In { .. } => Some(IN_PRECEDENCE),
            //Self::LessThan(_, _) => Some(LT_PRECEDENCE),
            //Self::LessThanOrEq(_, _) => Some(LTEQ_PRECEDENCE),
            Func::Add(_, _) => Some(ADD_PRECEDENCE),
            Func::Sub(_, _) => Some(SUBTRACT_PRECEDENCE),
            Func::Mul(_, _) => Some(MULTIPLY_PRECEDENCE),
            Func::Div(_, _) => Some(DIVIDE_PRECEDENCE),
            Func::Mod(_, _) => Some(MODULO_PRECEDENCE),
            Func::Concat(_, _) => Some(CONCAT_PRECEDENCE),
            _ => None,
        } {
            if rhs_precedence < outer_precedence {
                // Do the rotation
                return Self::Func(match rhs {
                    Func::Or(mid, rhs) => 
                        Func::Or(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::And(mid, rhs) => 
                        Func::And(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Not(rhs) => 
                        Func::Not(Box::new(construct_operator(lhs, *rhs))),
                    /*
                    Func::Eq(mid, rhs) => {
                        return Func::Eq(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Func::In {
                        value: mid,
                        collection: rhs,
                    } => {
                        return Func::In {
                            value: Box::new(construct_operator(lhs, *mid)),
                            collection: rhs,
                        };
                    }
                    Func::LessThan(mid, rhs) => {
                        return Func::LessThan(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Func::LessThanOrEq(mid, rhs) => {
                        return Func::LessThanOrEq(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    */
                    Func::Add(mid, rhs) => 
                        Func::Add(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Sub(mid, rhs) => 
                        Func::Sub(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Mul(mid, rhs) => 
                        Func::Mul(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Div(mid, rhs) => 
                        Func::Div(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Mod(mid, rhs) => 
                        Func::Mod(Box::new(construct_operator(lhs, *mid)), rhs),
                    Func::Concat(mid, rhs) => 
                        Func::Concat(Box::new(construct_operator(lhs, *mid)), rhs),
                    _ => construct_operator(lhs, rhs)
                });
            }
        }
        Self::Func(construct_operator(lhs, rhs))
    }


    
    /// Parses a fixed-length list of arguments.
    fn conn_parse_fixed_args<const N: usize>(
        conn: &Connection,
        full_str: &String,
        remaining_str: &str,
        fn_name: String,
        arg_end_regex: &Regex,
    ) -> Result<([Func; N], String), Error> {
        let arg_divider_regex: Regex = Regex::new(r#"(?s)\s*,(.*)"#).unwrap();

        let mut func_args: [Func; N] = [const { Func::Null }; N];
        let mut following: String = String::from(remaining_str);

        for k in 0..(N - 1) {
            let (arg, tail) = Self::conn_parse_expr(conn, full_str, &following)?;
            func_args[k] = arg.into_func()?;

            // Test for divider between prev argument and next argument
            if let Some(arg_divider_cap) = arg_divider_regex.captures(&tail) {
                let (_, [following_str]) = arg_divider_cap.extract();
                following = following_str.into();
            // Test if end of arguments
            } else if arg_end_regex.is_match(&following) {
                return Err(Error::FormulaSyntaxError {
                    msg: format!("Too few arguments for function {fn_name}."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            } else {
                return Err(Error::FormulaSyntaxError {
                    msg: String::from("Unexpected character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Parse final argument
        if N > 0 {
            let arg: Self;
            (arg, following) = Self::conn_parse_expr(conn, full_str, &following)?;
            func_args[N - 1] = arg.into_func()?;
        }

        // Check to make sure final argument is capped off
        if let Some(arg_end_cap) = arg_end_regex.captures(&following) {
            let (_, [following_str]) = arg_end_cap.extract();
            return Ok((func_args, String::from(following_str)));
        } else if arg_divider_regex.is_match(&following) {
            return Err(Error::FormulaSyntaxError {
                msg: format!("Too many arguments for function {fn_name}."),
                full_formula: full_str.clone(),
                substring_with_error: String::from(remaining_str.trim_start()),
            });
        } else {
            // If argument is followed by neither end of argument nor transition to next argument, return error
            return Err(Error::FormulaSyntaxError {
                msg: format!("Unexpected character after all arguments."),
                full_formula: full_str.clone(),
                substring_with_error: String::from(remaining_str.trim_start()),
            });
        }
    }

    /// Parses a variable list of arguments.
    fn conn_parse_variable_args(
        conn: &Connection,
        full_str: &String,
        remaining_str: &str,
        fn_name: String,
        arg_end_regex: &Regex,
        min_arg_count: usize,
    ) -> Result<(Vec<Func>, String), Error> {
        // Test to see if no arguments provided
        if let Some(arg_end_cap) = arg_end_regex.captures(remaining_str) {
            if min_arg_count == 0 {
                // If no minimum # expected arguments, return success
                let (_, [following_str]) = arg_end_cap.extract();
                return Ok((Vec::new(), String::from(following_str)));
            } else {
                // If not fulfilled minimum # expected arguments, return error
                return Err(Error::FormulaSyntaxError {
                    msg: format!("Too few arguments for function {fn_name}."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        let arg_divider_regex: Regex = Regex::new(r#"(?s)\s*,(.*)"#).unwrap();

        let mut func_args: Vec<Func> = Vec::new();
        let mut following: String = String::from(remaining_str);

        loop {
            // Parse next argument
            let (arg, tail) = Self::conn_parse_expr(conn, full_str, &following)?;
            func_args.push(arg.into_func()?);

            // Test for divider between prev argument and next argument
            if let Some(arg_divider_cap) = arg_divider_regex.captures(&tail) {
                let (_, [following_str]) = arg_divider_cap.extract();
                following = following_str.into();
            // Test if end of arguments
            } else if let Some(arg_end_cap) = arg_end_regex.captures(&tail) {
                if func_args.len() >= min_arg_count {
                    // If fulfilled minimum # expected arguments, return success
                    let (_, [following_str]) = arg_end_cap.extract();
                    return Ok((func_args, String::from(following_str)));
                } else {
                    // If not fulfilled minimum # expected arguments, return error
                    return Err(Error::FormulaSyntaxError {
                        msg: format!("Too few arguments for function {fn_name}."),
                        full_formula: full_str.clone(),
                        substring_with_error: String::from(remaining_str.trim_start()),
                    });
                }
            } else {
                // If argument is followed by neither end of argument nor transition to next argument, return error
                return Err(Error::FormulaSyntaxError {
                    msg: format!("Unexpected character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }
    }


    /// Invoked after a table name is found. Checks if the table name is immediately followed by a backreference expression, and otherwise treats the table name as a source.
    fn conn_parse_backreference_expr(conn: &Connection, full_str: &String, remaining_str: &str, table_name: String) -> Result<(Self, String), Error> {
        let table: TableMetadata = TableMetadata::conn_find(conn, table_name)?;

        // Check for if immediately followed by backreference expression
        let where1_regex: Regex = Regex::new(r#"(?is)^\s*where\s+([A-Za-z0-9_]*)\s*=\s*(.*)"#).unwrap();
        if let Some(cap) = where1_regex.captures(remaining_str) {
            let (_, [column_name, following]) = cap.extract();
            let (_, column) = TableColumnMetadata::conn_find(conn, table.oid.clone(), String::from(column_name))?;
            let (source, following) = Self::conn_parse_expr(conn, full_str, following)?;
            return Ok((
                Self::RecordFunc(RecordFunc::Backreference { 
                    record: Box::new(source.into_record()?), 
                    table, 
                    column 
                }),
                following
            ));
        }
        let where2_regex: Regex = Regex::new(r#"(?is)^\s*where\s*"((?:[^"\\]|\\\\|\\")*)"\s*=\s*(.*)"#).unwrap();
        if let Some(cap) = where2_regex.captures(remaining_str) {
            let (_, [column_name_raw, following]) = cap.extract();
            let column_name = column_name_raw.replace("\\\"", "\"").replace("\\\\", "\\");
            let (_, column) = TableColumnMetadata::conn_find(conn, table.oid.clone(), column_name)?;
            let (source, following) = Self::conn_parse_expr(conn, full_str, following)?;
            return Ok((
                Self::RecordFunc(RecordFunc::Backreference { 
                    record: Box::new(source.into_record()?), 
                    table, 
                    column 
                }),
                following
            ));
        }

        let expr: Self = Self::RecordFunc(RecordFunc::Source(table));
        expr.conn_parse_dependent_expr(conn, full_str, remaining_str)
    }

    /// Parses if there is an expression dependent on this one (e.g. a binary operator, a column reference).
    fn conn_parse_dependent_expr(self, conn: &Connection, full_str: &String, remaining_str: &str) -> Result<(Self, String), Error> {
        // Check for reference
        let reference1_regex: Regex = Regex::new(r#"(?is)^\s*\.\s*([A-Za-z0-9_]*)\s*(.*)""#).unwrap();
        if let Some(reference_cap) = reference1_regex.captures(remaining_str) {
            let (_, [column_name, following]) = reference_cap.extract();
            let record = self.into_record()?;
            let (table_oid, column) = TableColumnMetadata::conn_find(conn, record.table_oid()?, String::from(column_name))?;
            
            let expr: Self = match &column.column_type {
                TableColumnType::Primitive { .. }
                | TableColumnType::File { .. }
                | TableColumnType::Subreport { .. } => Self::Func(Func::Column { record, table_oid, column }),
                TableColumnType::Object { .. }
                | TableColumnType::SingleSelect { .. }
                | TableColumnType::MultiSelect { .. } => Self::Ambiguous { record, table_oid, column }
            };
            return expr.conn_parse_dependent_expr(conn, full_str, following);
        }
        let reference2_regex: Regex = Regex::new(r#"(?is)^\s*\.\s*"((?:[^"\\]|\\\\|\\")*)"\s*(.*)"#).unwrap();
        if let Some(reference_cap) = reference2_regex.captures(remaining_str) {
            let (_, [column_name_raw, following]) = reference_cap.extract();
            let column_name = column_name_raw.replace("\\\"", "\"").replace("\\\\", "\\");
            let record = self.into_record()?;
            let (table_oid, column) = TableColumnMetadata::conn_find(conn, record.table_oid()?, String::from(column_name))?;
            
            let expr: Self = match &column.column_type {
                TableColumnType::Primitive { .. }
                | TableColumnType::File { .. }
                | TableColumnType::Subreport { .. } => Self::Func(Func::Column { record, table_oid, column }),
                TableColumnType::Object { .. }
                | TableColumnType::SingleSelect { .. }
                | TableColumnType::MultiSelect { .. } => Self::Ambiguous { record, table_oid, column }
            };
            return expr.conn_parse_dependent_expr(conn, full_str, following);
        }

        // Check for OR operator
        let or_regex: Regex = Regex::new(r#"(?is)^\s*or\b(.*)"#).unwrap();
        if let Some(cap) = or_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    OR_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Or(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for AND operator
        let and_regex: Regex = Regex::new(r#"(?is)^\s*and\b(.*)"#).unwrap();
        if let Some(cap) = and_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    AND_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::And(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for + operator
        let add_regex: Regex = Regex::new(r#"(?is)^\s*\+\b(.*)"#).unwrap();
        if let Some(cap) = add_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    ADD_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Add(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for - operator
        let sub_regex: Regex = Regex::new(r#"(?is)^\s*-\b(.*)"#).unwrap();
        if let Some(cap) = sub_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    SUBTRACT_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Sub(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for * operator
        let mul_regex: Regex = Regex::new(r#"(?is)^\s*\*\b(.*)"#).unwrap();
        if let Some(cap) = mul_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    MULTIPLY_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Mul(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for / operator
        let div_regex: Regex = Regex::new(r#"(?is)^\s*/\b(.*)"#).unwrap();
        if let Some(cap) = div_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    DIVIDE_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Div(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // Check for % operator
        let mod_regex: Regex = Regex::new(r#"(?is)^\s*%\b(.*)"#).unwrap();
        if let Some(cap) = mod_regex.captures(remaining_str) {
            let (_, [following]) = cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;

            // Apply binary order precedence
            return Ok((
                Self::binary_operator_rotate(
                    MODULO_PRECEDENCE, 
                    self.into_func()?, rhs.into_func()?, 
                    |lhs, rhs| Func::Mod(Box::new(lhs), Box::new(rhs))
                ),
                following_rhs,
            ));
        }

        // No dependent expression, so treat as function
        Ok((self, String::from(remaining_str)))
    }

    /// Parses a single expression with no antecedent.
    pub fn conn_parse_expr(conn: &Connection, full_str: &String, remaining_str: &str) -> Result<(Self, String), Error> {
        // Check for open parenthesis
        let open_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\((.*)"#).unwrap();
        let close_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\)(.*)"#).unwrap();
        if let Some(open_parenthesis_cap) = open_parenthesis_regex.captures(remaining_str) {
            let (_, [following]) = open_parenthesis_cap.extract();
            let (inside_parenthesis_expr, following_after_expr) =
                Self::conn_parse_expr(conn, full_str, following)?;
            if let Some(close_parenthesis_cap) =
                close_parenthesis_regex.captures(&following_after_expr)
            {
                let (_, [following_after_parenthesis]) = close_parenthesis_cap.extract();
                let expr: Self = match inside_parenthesis_expr {
                    Self::Func(func) => Self::Func(Func::Wrap(Box::new(func))),
                    _ => inside_parenthesis_expr
                };
                return expr.conn_parse_dependent_expr(
                    conn,
                    full_str,
                    following_after_parenthesis
                );
            } else {
                return Err(Error::FormulaSyntaxError {
                    msg: String::from("Expected ')' character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Check for a string literal
        let string_literal_regex: Regex = Regex::new(r#"(?s)^\s*'((?:[^']|'')*)'(.*)"#).unwrap();
        if let Some(string_literal_cap) = string_literal_regex.captures(remaining_str) {
            let (_, [string_literal_content, following]) = string_literal_cap.extract();
            let expr: Self = Self::Func(Func::Text(String::from(string_literal_content.replace("''", "'"))));
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Check for a hexadecimal integer literal
        let hexint_literal_regex: Regex =
            Regex::new(r#"(?is)^\s*([+\-]?)0x([0-9a-f]+)(.*)"#).unwrap();
        if let Some(hexint_literal_cap) = hexint_literal_regex.captures(remaining_str) {
            let (_, [hexint_literal_sign, hexint_literal_content, following]) =
                hexint_literal_cap.extract();
            let hexint_literal_src: String =
                format!("{hexint_literal_sign}{hexint_literal_content}");
            let Ok(int_literal) = i64::from_str_radix(&hexint_literal_src, 16) else {
                return Err(Error::FormulaSyntaxError {
                    msg: String::from("Unable to parse hexadecimal integer literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            let expr: Self = Self::Func(Func::Integer(int_literal));
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Check for a real literal
        let real_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d*\.\d+)(.*)"#).unwrap();
        if let Some(real_literal_cap) = real_literal_regex.captures(remaining_str) {
            let (_, [real_literal_content, following]) = real_literal_cap.extract();
            let Ok(real_literal) = real_literal_content.parse::<f64>() else {
                return Err(Error::FormulaSyntaxError {
                    msg: String::from("Unable to parse float literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            let expr: Self = Self::Func(Func::Number(real_literal));
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Check for an integer literal
        let int_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d+)(.*)"#).unwrap();
        if let Some(int_literal_cap) = int_literal_regex.captures(remaining_str) {
            let (_, [int_literal_content, following]) = int_literal_cap.extract();
            let Ok(int_literal) = int_literal_content.parse::<i64>() else {
                return Err(Error::FormulaSyntaxError {
                    msg: String::from("Unable to parse integer literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            let expr: Self = Self::Func(Func::Integer(int_literal));
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Check for a true/false boolean literal
        let bool_literal_regex: Regex = Regex::new(r#"(?is)^\s*(true|false)(.*)"#).unwrap();
        if let Some(bool_literal_cap) = bool_literal_regex.captures(remaining_str) {
            let (_, [bool_literal_content, following]) = bool_literal_cap.extract();
            let bool_literal = bool_literal_content.to_uppercase() == "TRUE";
            let expr: Self = Self::Func(Func::Boolean(bool_literal));
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Check for a null literal
        let null_literal_regex: Regex = Regex::new(r#"(?is)^\s*null(.*)"#).unwrap();
        if let Some(null_literal_cap) = null_literal_regex.captures(remaining_str) {
            let (_, [following]) = null_literal_cap.extract();
            let expr: Self = Self::Func(Func::Null);
            return expr.conn_parse_dependent_expr(
                conn,
                full_str,
                following
            );
        }

        // Callable functions with one argument
        let mut arg1_fn_names: HashMap<_, fn (Func) -> Func> = HashMap::new();
        arg1_fn_names.insert("floor", |arg1| Func::Floor(Box::new(arg1)));
        arg1_fn_names.insert("ceil", |arg1| Func::Ceiling(Box::new(arg1)));
        arg1_fn_names.insert("round", |arg1| Func::Round(Box::new(arg1)));
        arg1_fn_names.insert("abs", |arg1| Func::Abs(Box::new(arg1)));
        arg1_fn_names.insert("sign", |arg1| Func::Sign(Box::new(arg1)));
        arg1_fn_names.insert("length", |arg1| Func::StringLength(Box::new(arg1)));
        arg1_fn_names.insert("upper", |arg1| Func::Upper(Box::new(arg1)));
        arg1_fn_names.insert("lower", |arg1| Func::Lower(Box::new(arg1)));
        arg1_fn_names.insert("unixtime", |arg1| Func::ToUnixEpoch(Box::new(arg1)));
        arg1_fn_names.insert("fromunixtime", |arg1| Func::FromUnixEpoch(Box::new(arg1)));

        for (fn_name, fn_parser) in arg1_fn_names {
            let fn_regex: Regex = Regex::new(&format!("(?is)^\\s*{fn_name}\\s*\\((.*)")).unwrap();
            if let Some(fn_cap) = fn_regex.captures(remaining_str) {
                let (_, [following]) = fn_cap.extract();

                let ([arg1], after_fn_close) = Self::conn_parse_fixed_args(
                    conn,
                    full_str,
                    following,
                    fn_name.to_uppercase(),
                    &close_parenthesis_regex,
                )?;
                let expr: Self = Self::Func(fn_parser(arg1));
                return expr.conn_parse_dependent_expr(
                    conn, 
                    full_str, 
                    &after_fn_close
                );
            }
        }

        // Callable functions with two arguments
        let mut arg2_fn_names: HashMap<_, fn (Func, Func) -> Func> = HashMap::new();
        arg2_fn_names.insert("join", |arg1, arg2| Func::Join { list: Box::new(arg1), delimiter: Box::new(arg2) });
        arg2_fn_names.insert("pow", |arg1, arg2| Func::Pow(Box::new(arg1), Box::new(arg2)));

        for (fn_name, fn_parser) in arg2_fn_names {
            let fn_regex: Regex = Regex::new(&format!("(?is)^\\s*{fn_name}\\s*\\((.*)")).unwrap();
            if let Some(fn_cap) = fn_regex.captures(remaining_str) {
                let (_, [following]) = fn_cap.extract();

                let ([arg1, arg2], after_fn_close) = Self::conn_parse_fixed_args(
                    conn,
                    full_str,
                    following,
                    fn_name.to_uppercase(),
                    &close_parenthesis_regex,
                )?;
                let expr: Self = Self::Func(fn_parser(arg1, arg2));
                return expr.conn_parse_dependent_expr(
                    conn, 
                    full_str, 
                    &after_fn_close
                );
            }
        }

        // Callable functions with variable arguments
        let mut argvar_fn_names: HashMap<_, fn (Vec<Func>) -> Func> = HashMap::new();
        argvar_fn_names.insert("coalesce", |args| Func::Coalesce(args));

        for (fn_name, fn_parser) in argvar_fn_names {
            let fn_regex: Regex = Regex::new(&format!("(?is)^\\s*{fn_name}\\s*\\((.*)")).unwrap();
            if let Some(fn_cap) = fn_regex.captures(remaining_str) {
                let (_, [following]) = fn_cap.extract();

                let (args, after_fn_close) = Self::conn_parse_variable_args(
                    conn,
                    full_str,
                    following,
                    fn_name.to_uppercase(),
                    &close_parenthesis_regex,
                    2
                )?;
                let expr: Self = Self::Func(fn_parser(args));
                return expr.conn_parse_dependent_expr(
                    conn, 
                    full_str, 
                    &after_fn_close
                );
            }
        }

        // Check for the special CAST function
        let cast_regex: Regex = Regex::new(r#"(?is)^\s*cast\s*\((.*)"#).unwrap();
        if let Some(cast_cap) = cast_regex.captures(remaining_str) {
            let (_, [following]) = cast_cap.extract();

            let (record, following) = Self::conn_parse_expr(conn, full_str, following)?;

            let as1_regex: Regex = Regex::new(r#"(?is)^\s*as\s*([A-Za-z0-9_]*)\s*\)(.*)"#).unwrap();
            if let Some(table_cap) = as1_regex.captures(&following) {
                let (_, [table_name, following]) = table_cap.extract();
                let table: TableMetadata = TableMetadata::conn_find(conn, String::from(table_name))?;
                let expr: Self = Self::RecordFunc(RecordFunc::Cast { 
                    record: Box::new(record.into_record()?), 
                    table 
                });
                return expr.conn_parse_dependent_expr(conn, full_str, following);
            }
            let as2_regex: Regex = Regex::new(r#"(?is)^\s*as\s*"((?:[^"\\]|\\\\|\\")*)"\s*\)(.*)"#).unwrap();
            if let Some(table_cap) = as2_regex.captures(&following) {
                let (_, [table_name_raw, following]) = table_cap.extract();
                let table_name = table_name_raw.replace("\\\"", "\"").replace("\\\\", "\\");
                let table: TableMetadata = TableMetadata::conn_find(conn, table_name)?;
                let expr: Self = Self::RecordFunc(RecordFunc::Cast { 
                    record: Box::new(record.into_record()?), 
                    table 
                });
                return expr.conn_parse_dependent_expr(conn, full_str, following);
            }

            return Err(Error::adhoc("Expected AS \"<TABLE NAME>\" keyword."));
        }

        // Check for NOT unary operator
        let not_regex: Regex = Regex::new(r#"(?is)^\s*(?:!|not\b)(.*)"#).unwrap();
        if let Some(not_cap) = not_regex.captures(remaining_str) {
            let (_, [following]) = not_cap.extract();
            let (rhs, following_rhs) = Self::conn_parse_expr(conn, full_str, following)?;
            return Ok((Self::Func(Func::Not(Box::new(rhs.into_func()?))), following_rhs));
        }

        // Check for table name expression
        let table1_regex: Regex = Regex::new(r#"(?is)^\s*([A-Za-z0-9_]*)\s*(.*)""#).unwrap();
        if let Some(table_cap) = table1_regex.captures(remaining_str) {
            let (_, [table_name, following]) = table_cap.extract();
            return Self::conn_parse_backreference_expr(conn, full_str, following, String::from(table_name));
        }
        let table2_regex: Regex = Regex::new(r#"(?is)^\s*"((?:[^"\\]|\\\\|\\")*)"\s*(.*)"#).unwrap();
        if let Some(table_cap) = table2_regex.captures(remaining_str) {
            let (_, [table_name_raw, following]) = table_cap.extract();
            let table_name = table_name_raw.replace("\\\"", "\"").replace("\\\\", "\\");
            return Self::conn_parse_backreference_expr(conn, full_str, following, table_name);
        }

        return Err(Error::FormulaSyntaxError {
            msg: String::from("Unknown formula expression."),
            full_formula: full_str.clone(),
            substring_with_error: String::from(remaining_str.trim_start()),
        });
    }
}

impl Func {
    /// Parse a formula.
    pub fn parse(formula: String) -> Result<Self, Error> {
        let conn = db::open()?;
        
        // Parse as a function
        let (expr, remainder) = Expression::conn_parse_expr(&conn, &formula, &formula)?;

        // If there is any non-whitespace in remainder, formula is invalid
        let nonwhitespace_regex: Regex = Regex::new(r#"\S"#).unwrap();
        if let Some(_) = nonwhitespace_regex.captures(&remainder) {
            Err(Error::FormulaSyntaxError { 
                msg: String::from("Expected end of expression."), 
                full_formula: formula, 
                substring_with_error: remainder 
            })
        } else {
            // If all remainder is whitespace or empty, formula is valid
            expr.into_func()
        }
    }
}