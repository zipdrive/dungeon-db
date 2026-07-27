use std::collections::HashSet;
use regex::Regex;
use rusqlite::{Connection};
use crate::util::error::Error;
use crate::data::{column, column_type};

#[derive(Clone)]
pub struct FormulaReturnType {
    /// The primitive types that the parameter can conform to.
    primitive_types: HashSet<column_type::Primitive>
}

impl FormulaReturnType {
    /// Creates a new type representing a null value.
    pub fn new() -> Self {
        Self {
            primitive_types: HashSet::new()
        }
    }

    /// Creates a new type representing a specific primitive type.
    pub fn from(prim: column_type::Primitive) -> Self {
        Self {
            primitive_types: HashSet::from_iter(match &prim {
                column_type::Primitive::Datetime => vec![
                    column_type::Primitive::Date, 
                    prim
                ],
                column_type::Primitive::PlainText => vec![
                    column_type::Primitive::JsonText, 
                    prim
                ],
                column_type::Primitive::Number => vec![
                    column_type::Primitive::Integer, 
                    column_type::Primitive::Datetime, 
                    column_type::Primitive::Date, 
                    column_type::Primitive::Boolean,
                    prim
                ],
                column_type::Primitive::Integer => vec![
                    column_type::Primitive::Boolean,
                    prim
                ],
                column_type::Primitive::File => vec![
                    column_type::Primitive::Image, 
                    prim
                ],
                _ => vec![prim]
            })
        }
    }

    /// Constructs a type that represents the most specific type that encompasses both this type and the given type.
    pub fn generalize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.union(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Constructs a type that represents the most general type that conforms to both this type and the given type.
    pub fn specialize(&self, other: &Self) -> Self {
        Self {
            primitive_types: HashSet::from_iter(self.primitive_types.intersection(&(other.primitive_types)).map(|p| p.clone()))
        }
    }

    /// Returns true if an instance of the given type can always be passed as a value of this type.
    pub fn encompasses(&self, other: &Self) -> bool {
        self.primitive_types.is_superset(&(other.primitive_types))
    }


    /// Returns true if a value of this type can be text.
    fn is_text_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::PlainText)
            || self.primitive_types.contains(&column_type::Primitive::JsonText);
    }

    /// Returns true if a value of this type can be numeric.
    fn is_numeric_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::Number)
            || self.primitive_types.contains(&column_type::Primitive::Datetime)
            || self.primitive_types.contains(&column_type::Primitive::Date)
            || self.primitive_types.contains(&column_type::Primitive::Integer)
            || self.primitive_types.contains(&column_type::Primitive::Boolean);
    }

    /// Returns true if a value of this type can be a file.
    fn is_file_type(&self) -> bool {
        return self.primitive_types.contains(&column_type::Primitive::File)
            || self.primitive_types.contains(&column_type::Primitive::Image);
    }


    /// Constructs an expression for a value's label.
    /// This should be used in cases where an operation combines two or more values, and not in cases where a value is selected from a list.
    pub fn construct_plain_label_expr(&self, value_expr: &String) -> String {
        // Check if pure file
        if self.is_file_type() && !self.is_text_type() && !self.is_numeric_type() {
            return format!("(SELECT f.LABEL FROM METADATA_FILE_VIEW f WHERE f.OID = {value_expr})");
        }

        // Check if pure text
        if self.is_text_type() && !self.is_file_type() && !self.is_numeric_type() {
            return value_expr.clone();
        }
        
        // Check if pure number
        if self.is_numeric_type() && !self.is_file_type() && !self.is_text_type() {
            if self.primitive_types.contains(&column_type::Primitive::Number) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Datetime) {
                return format!("STRFTIME('%FT%TZ', {value_expr}, 'julianday')");
            } else if self.primitive_types.contains(&column_type::Primitive::Date) {
                return format!("DATE({value_expr}, 'julianday')");
            } else if self.primitive_types.contains(&column_type::Primitive::Integer) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Boolean) {
                return format!("IF({value_expr}, 'true', {value_expr} IS NULL, NULL, 'false')")
            }
        }

        // Mixed, unknown type
        return format!("CAST({value_expr} AS TEXT)");
    }

    /// Constructs an expression for a value's label.
    /// This should be used in cases where an operation combines two or more values, and not in cases where a value is selected from a list.
    pub fn construct_json_label_expr(&self, value_expr: &String) -> String {
        // Check if pure file
        if self.is_file_type() && !self.is_text_type() && !self.is_numeric_type() {
            return format!("'\"' || (SELECT REPLACE(REPLACE(f.LABEL, '\\', '\\\\'), '\"', '\\\"') FROM METADATA_FILE_VIEW f WHERE f.OID = {value_expr}) || '\"'");
        }

        // Check if pure text
        if self.is_text_type() && !self.is_file_type() && !self.is_numeric_type() {
            if self.primitive_types.contains(&column_type::Primitive::JsonText) && !self.primitive_types.contains(&column_type::Primitive::PlainText) {
                return value_expr.clone();
            } else {
                return format!("'\"' || REPLACE(REPLACE({value_expr}, '\\', '\\\\'), '\"', '\\\"') || '\"'");
            }
        }
        
        // Check if pure number
        if self.is_numeric_type() && !self.is_file_type() && !self.is_text_type() {
            if self.primitive_types.contains(&column_type::Primitive::Number) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Datetime) {
                return format!("'\"' || STRFTIME('%FT%TZ', {value_expr}, 'julianday') || '\"'");
            } else if self.primitive_types.contains(&column_type::Primitive::Date) {
                return format!("'\"' || DATE({value_expr}, 'julianday') || '\"'");
            } else if self.primitive_types.contains(&column_type::Primitive::Integer) {
                return format!("CAST({value_expr} AS TEXT)");
            } else if self.primitive_types.contains(&column_type::Primitive::Boolean) {
                return format!("IF({value_expr}, 'true', {value_expr} IS NULL, NULL, 'false')")
            }
        }

        // Mixed, unknown type
        return format!("'\"' || REPLACE(REPLACE(CAST({value_expr} AS TEXT), '\\', '\\\\'), '\"', '\\\"') || '\"'");
    }

    /// Describes the type.
    pub fn to_string(&self) -> String {
        let mut temp = self.primitive_types.clone();
        if temp.contains(&column_type::Primitive::Datetime) {
            temp.remove(&column_type::Primitive::Date);
        }
        if temp.contains(&column_type::Primitive::PlainText) {
            temp.remove(&column_type::Primitive::JsonText);
        }
        if temp.contains(&column_type::Primitive::Number) {
            temp.remove(&column_type::Primitive::Integer);
        }
        if temp.contains(&column_type::Primitive::File) {
            temp.remove(&column_type::Primitive::Image);
        }
        temp.into_iter()
            .map(|prim| String::from(prim.to_str()))
            .reduce(|acc, e| format!("{acc} | {e}"))
            .unwrap_or(String::from("null"))
    }
}



#[derive(Clone)]
pub enum Formula {
    Param {
        datasource_alias: String,
        column_oid: i64,
    },
    Null,
    LiteralBool(bool),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralString(String),
    LiteralArray(Vec<Formula>),
    RandomInt,

    And(Box<Formula>, Box<Formula>),
    Or(Box<Formula>, Box<Formula>),
    Not(Box<Formula>),
    LessThan(Box<Formula>, Box<Formula>),
    LessThanOrEq(Box<Formula>, Box<Formula>),
    Eq(Box<Formula>, Box<Formula>),
    In {
        value: Box<Formula>,
        collection: Box<Formula>,
    },
    Glob {
        str: Box<Formula>,
        pattern: Box<Formula>,
    },

    Add(Box<Formula>, Box<Formula>),
    Subtract(Box<Formula>, Box<Formula>),
    Multiply(Box<Formula>, Box<Formula>),
    Divide(Box<Formula>, Box<Formula>),
    Modulo(Box<Formula>, Box<Formula>),
    Exponent(Box<Formula>, Box<Formula>),
    Abs(Box<Formula>),
    Sign(Box<Formula>),
    Round(Box<Formula>),
    Floor(Box<Formula>),
    Ceiling(Box<Formula>),

    Concat(Box<Formula>, Box<Formula>),
    Lowercase(Box<Formula>),
    Uppercase(Box<Formula>),
    Substring {
        str: Box<Formula>,
        start: Box<Formula>,
        length: Option<Box<Formula>>,
    },
    Replace {
        original: Box<Formula>,
        pattern: Box<Formula>,
        replacement: Box<Formula>,
    },
    Length(Box<Formula>),
    Format {
        format: Box<Formula>,
        format_params: Vec<Formula>,
    },

    Index {
        collection: Box<Formula>,
        index: Box<Formula>
    },

    Wrap(Box<Formula>),
    Argmin(Vec<Formula>),
    Argmax(Vec<Formula>),
    Coalesce(Vec<Formula>),
    Conditional {
        condition: Box<Formula>,
        formula_if_true: Box<Formula>,
        formula_if_false: Box<Formula>,
    },
    Switch {
        value: Box<Formula>,
        matches: Vec<(Formula, Formula)>,
        formula_if_no_match: Box<Formula>,
    },
    NullIf {
        value: Box<Formula>,
        null_if_match: Box<Formula>,
    },

    Sum(Box<Formula>),
    Average(Box<Formula>),
    Min(Box<Formula>),
    Max(Box<Formula>),
    Count(Box<Formula>),
    Join {
        collection: Box<Formula>,
        delimiter: Box<Formula>,
    },
}


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

impl Formula {
    /// Checks for precedence of a binary operator. Returns None if the formula is not a binary operator.
    /// For a formula like "[exprA] [binary1] [exprB] [binary2] [exprC]" (where binary1 and binary2 are binary operators),
    /// the formula is normally evaluated as Binary1(exprA, Binary2(exprB, exprC)).
    /// However, if the precedence of the binary1 operator is greater than the precedence of the binary2 operator,
    /// the order of evaluation is rotated to Binary2(Binary1(exprA, exprB), exprC).
    fn binary_operator_precedence(&self) -> Option<usize> {
        match self {
            Self::Or(_, _) => Some(OR_PRECEDENCE),
            Self::And(_, _) => Some(AND_PRECEDENCE),
            Self::Not(_) => Some(NOT_PRECEDENCE),
            Self::Eq(_, _) => Some(EQ_PRECEDENCE),
            Self::In { .. } => Some(IN_PRECEDENCE),
            Self::LessThan(_, _) => Some(LT_PRECEDENCE),
            Self::LessThanOrEq(_, _) => Some(LTEQ_PRECEDENCE),
            Self::Add(_, _) => Some(ADD_PRECEDENCE),
            Self::Subtract(_, _) => Some(SUBTRACT_PRECEDENCE),
            Self::Multiply(_, _) => Some(MULTIPLY_PRECEDENCE),
            Self::Divide(_, _) => Some(DIVIDE_PRECEDENCE),
            Self::Modulo(_, _) => Some(MODULO_PRECEDENCE),
            Self::Concat(_, _) => Some(CONCAT_PRECEDENCE),
            _ => None,
        }
    }

    /// Rotates the order of evaluation of binary operations, according to the rules laid out in Formula::binary_operator_precedence().
    fn binary_operator_rotate<F: FnOnce(Self, Self) -> Self>(
        self,
        outer_precedence: usize,
        lhs: Self,
        construct_operator: F,
    ) -> Self {
        if let Some(self_precedence) = self.binary_operator_precedence() {
            if self_precedence < outer_precedence {
                // Do the rotation
                match self {
                    Self::Or(mid, rhs) => {
                        return Self::Or(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::And(mid, rhs) => {
                        return Self::And(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Not(rhs) => {
                        return Self::Not(Box::new(construct_operator(lhs, *rhs)));
                    }
                    Self::Eq(mid, rhs) => {
                        return Self::Eq(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::In {
                        value: mid,
                        collection: rhs,
                    } => {
                        return Self::In {
                            value: Box::new(construct_operator(lhs, *mid)),
                            collection: rhs,
                        };
                    }
                    Self::LessThan(mid, rhs) => {
                        return Self::LessThan(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::LessThanOrEq(mid, rhs) => {
                        return Self::LessThanOrEq(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Add(mid, rhs) => {
                        return Self::Add(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Subtract(mid, rhs) => {
                        return Self::Subtract(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Multiply(mid, rhs) => {
                        return Self::Multiply(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Divide(mid, rhs) => {
                        return Self::Divide(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Modulo(mid, rhs) => {
                        return Self::Modulo(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    Self::Concat(mid, rhs) => {
                        return Self::Concat(Box::new(construct_operator(lhs, *mid)), rhs);
                    }
                    _ => { /* This case shouldn't occur, but if it does then do not rotate */ }
                }
            } else {
                // Do not do rotation
            }
        }
        construct_operator(lhs, self)
    }

    /// Parses a fixed-length list of arguments.
    fn parse_fixed_args<const N: usize>(
        full_str: &String,
        remaining_str: &str,
        fn_name: String,
        arg_end_regex: &Regex,
    ) -> Result<([Self; N], String), Error> {
        let arg_divider_regex: Regex = Regex::new(r#"(?s)\s*,(.*)"#).unwrap();

        let mut formula_args: [Formula; N] = [const { Formula::Null }; N];
        let mut following: String = String::from(remaining_str);

        for k in 0..(N - 1) {
            let tail: String;
            (formula_args[k], tail) = Self::parse_expr(full_str, &following)?;

            // Test for divider between prev argument and next argument
            if let Some(arg_divider_cap) = arg_divider_regex.captures(&tail) {
                let (_, [following_str]) = arg_divider_cap.extract();
                following = following_str.into();
            // Test if end of arguments
            } else if arg_end_regex.is_match(&following) {
                return Err(Error::FormulaParseError {
                    msg: format!("Too few arguments for function {fn_name}."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            } else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Unexpected character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Parse final argument
        if N > 0 {
            (formula_args[N - 1], following) = Self::parse_expr(full_str, &following)?;
        }

        // Check to make sure final argument is capped off
        if let Some(arg_end_cap) = arg_end_regex.captures(&following) {
            let (_, [following_str]) = arg_end_cap.extract();
            return Ok((formula_args, String::from(following_str)));
        } else if arg_divider_regex.is_match(&following) {
            return Err(Error::FormulaParseError {
                msg: format!("Too many arguments for function {fn_name}."),
                full_formula: full_str.clone(),
                substring_with_error: String::from(remaining_str.trim_start()),
            });
        } else {
            // If argument is followed by neither end of argument nor transition to next argument, return error
            return Err(Error::FormulaParseError {
                msg: format!("Unexpected character."),
                full_formula: full_str.clone(),
                substring_with_error: String::from(remaining_str.trim_start()),
            });
        }
    }

    /// Parses a variable list of arguments.
    fn parse_variable_args(
        full_str: &String,
        remaining_str: &str,
        fn_name: String,
        arg_end_regex: &Regex,
        min_arg_count: usize,
    ) -> Result<(Vec<Self>, String), Error> {
        // Test to see if no arguments provided
        if let Some(arg_end_cap) = arg_end_regex.captures(remaining_str) {
            if min_arg_count == 0 {
                // If no minimum # expected arguments, return success
                let (_, [following_str]) = arg_end_cap.extract();
                return Ok((Vec::new(), String::from(following_str)));
            } else {
                // If not fulfilled minimum # expected arguments, return error
                return Err(Error::FormulaParseError {
                    msg: format!("Too few arguments for function {fn_name}."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        let arg_divider_regex: Regex = Regex::new(r#"(?s)\s*,(.*)"#).unwrap();

        let mut formula_args: Vec<Self> = Vec::new();
        let mut following: String = String::from(remaining_str);

        loop {
            // Parse next argument
            let (next_formula_arg, tail) = Self::parse_expr(full_str, &following)?;
            formula_args.push(next_formula_arg);

            // Test for divider between prev argument and next argument
            if let Some(arg_divider_cap) = arg_divider_regex.captures(&tail) {
                let (_, [following_str]) = arg_divider_cap.extract();
                following = following_str.into();
            // Test if end of arguments
            } else if let Some(arg_end_cap) = arg_end_regex.captures(&tail) {
                if formula_args.len() >= min_arg_count {
                    // If fulfilled minimum # expected arguments, return success
                    let (_, [following_str]) = arg_end_cap.extract();
                    return Ok((formula_args, String::from(following_str)));
                } else {
                    // If not fulfilled minimum # expected arguments, return error
                    return Err(Error::FormulaParseError {
                        msg: format!("Too few arguments for function {fn_name}."),
                        full_formula: full_str.clone(),
                        substring_with_error: String::from(remaining_str.trim_start()),
                    });
                }
            } else {
                // If argument is followed by neither end of argument nor transition to next argument, return error
                return Err(Error::FormulaParseError {
                    msg: format!("Unexpected character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }
    }

    /// Parses a single expression with an antecedent formula.
    fn parse_dependent_expr(
        full_str: &String,
        remaining_str: &str,
        lhs: Self,
    ) -> Result<(Self, String), Error> {
        // Check for OR operator
        let or_regex: Regex = Regex::new(r#"(?is)^\s*or\b(.*)"#).unwrap();
        if let Some(or_cap) = or_regex.captures(remaining_str) {
            let (_, [following]) = or_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(OR_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Or(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for AND operator
        let and_regex: Regex = Regex::new(r#"(?is)^\s*and\b(.*)"#).unwrap();
        if let Some(and_cap) = and_regex.captures(remaining_str) {
            let (_, [following]) = and_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(AND_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::And(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for equals operator
        let equals_regex: Regex = Regex::new(r#"(?s)^\s*=(.*)"#).unwrap();
        if let Some(equals_cap) = equals_regex.captures(remaining_str) {
            let (_, [following]) = equals_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(EQ_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Eq(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for not equals operator
        let neq_regex: Regex = Regex::new(r#"(?s)^\s*<>(.*)"#).unwrap();
        if let Some(neq_cap) = neq_regex.captures(remaining_str) {
            let (_, [following]) = neq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(EQ_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Not(Box::new(Formula::Eq(Box::new(lhs), Box::new(rhs))))
                }),
                following_rhs,
            ));
        }

        // Check for IN operator
        let in_regex: Regex = Regex::new(r#"(?is)^\s*in\b(.*)"#).unwrap();
        if let Some(in_cap) = in_regex.captures(remaining_str) {
            let (_, [following]) = in_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(IN_PRECEDENCE, lhs, |lhs, rhs| Formula::In {
                    value: Box::new(lhs),
                    collection: Box::new(rhs),
                }),
                following_rhs,
            ));
        }

        // Check for less-than-or-equals operator
        let leq_regex: Regex = Regex::new(r#"(?s)^\s*<=(.*)"#).unwrap();
        if let Some(leq_cap) = leq_regex.captures(remaining_str) {
            let (_, [following]) = leq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LTEQ_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::LessThanOrEq(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for greater-than-or-equals operator
        let geq_regex: Regex = Regex::new(r#"(?s)^\s*>=(.*)"#).unwrap();
        if let Some(geq_cap) = geq_regex.captures(remaining_str) {
            let (_, [following]) = geq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LTEQ_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Not(Box::new(Formula::LessThan(Box::new(lhs), Box::new(rhs))))
                }),
                following_rhs,
            ));
        }

        // Check for less-than operator
        let lt_regex: Regex = Regex::new(r#"(?s)^\s*<(.*)"#).unwrap();
        if let Some(lt_cap) = lt_regex.captures(remaining_str) {
            let (_, [following]) = lt_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LT_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::LessThan(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for greater-than operator
        let gt_regex: Regex = Regex::new(r#"(?s)^\s*>(.*)"#).unwrap();
        if let Some(gt_cap) = gt_regex.captures(remaining_str) {
            let (_, [following]) = gt_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LT_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Not(Box::new(Formula::LessThanOrEq(
                        Box::new(lhs),
                        Box::new(rhs),
                    )))
                }),
                following_rhs,
            ));
        }

        // Check for addition operator
        let addition_regex: Regex = Regex::new(r#"(?s)^\s*\+(.*)"#).unwrap();
        if let Some(addition_cap) = addition_regex.captures(remaining_str) {
            let (_, [following]) = addition_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(ADD_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Add(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for subtraction operator
        let subtraction_regex: Regex = Regex::new(r#"(?s)^\s*-(.*)"#).unwrap();
        if let Some(subtraction_cap) = subtraction_regex.captures(remaining_str) {
            let (_, [following]) = subtraction_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(SUBTRACT_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Subtract(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for multiplication operator
        let multiplication_regex: Regex = Regex::new(r#"(?s)^\s*\*(.*)"#).unwrap();
        if let Some(multiplication_cap) = multiplication_regex.captures(remaining_str) {
            let (_, [following]) = multiplication_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(MULTIPLY_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Multiply(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for division operator
        let division_regex: Regex = Regex::new(r#"(?s)^\s*/(.*)"#).unwrap();
        if let Some(division_cap) = division_regex.captures(remaining_str) {
            let (_, [following]) = division_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(DIVIDE_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Divide(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for modulo operator
        let modulo_regex: Regex = Regex::new(r#"(?s)^\s*%(.*)"#).unwrap();
        if let Some(modulo_cap) = modulo_regex.captures(remaining_str) {
            let (_, [following]) = modulo_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(MODULO_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Modulo(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for concatenation operator
        let concat_regex: Regex = Regex::new(r#"(?s)^\s*&(.*)"#).unwrap();
        if let Some(concat_cap) = concat_regex.captures(remaining_str) {
            let (_, [following]) = concat_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(CONCAT_PRECEDENCE, lhs, |lhs, rhs| {
                    Formula::Concat(Box::new(lhs), Box::new(rhs))
                }),
                following_rhs,
            ));
        }

        // Check for collection index operator
        let open_index_regex: Regex = Regex::new(r#"(?s)^\s*\{(.*)"#).unwrap();
        let close_index_regex: Regex = Regex::new(r#"(?s)^\s*\}(.*)"#).unwrap();
        if let Some(open_index_cap) = open_index_regex.captures(remaining_str) {
            let (_, [following]) = open_index_cap.extract();
            let (inside_indexer_formula, following_after_expr) =
                Self::parse_expr(full_str, following)?;
            if let Some(close_index_cap) =
                close_index_regex.captures(&following_after_expr)
            {
                let (_, [following_after_indexer]) = close_index_cap.extract();
                return Self::parse_dependent_expr(
                    full_str,
                    following_after_indexer,
                    Formula::Index {
                        collection: Box::new(lhs),
                        index: Box::from(inside_indexer_formula)
                    }
                );
            } else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Expected '}' character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // If no known operator was appended, return the antecedent alone
        return Ok((lhs, String::from(remaining_str)));
    }

    /// Parses a single expression with no antecedent.
    fn parse_expr(full_str: &String, remaining_str: &str) -> Result<(Self, String), Error> {
        // Check for open parenthesis
        let open_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\((.*)"#).unwrap();
        let close_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\)(.*)"#).unwrap();
        if let Some(open_parenthesis_cap) = open_parenthesis_regex.captures(remaining_str) {
            let (_, [following]) = open_parenthesis_cap.extract();
            let (inside_parenthesis_formula, following_after_expr) =
                Self::parse_expr(full_str, following)?;
            if let Some(close_parenthesis_cap) =
                close_parenthesis_regex.captures(&following_after_expr)
            {
                let (_, [following_after_parenthesis]) = close_parenthesis_cap.extract();
                return Self::parse_dependent_expr(
                    full_str,
                    following_after_parenthesis,
                    Formula::Wrap(Box::from(inside_parenthesis_formula)), // Wrap the inner expression to make sure it doesn't get shifted around by order of mathematical operations
                );
            } else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Expected ')' character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Check for an array literal
        let open_bracket_regex: Regex = Regex::new(r#"(?s)^\s*\[(.*)"#).unwrap();
        let close_bracket_regex: Regex = Regex::new(r#"(?s)^\s*\](.*)"#).unwrap();
        if let Some(open_bracket_cap) = open_bracket_regex.captures(remaining_str) {
            let (_, [following]) = open_bracket_cap.extract();
            let (array_item_formulae, following_after_expr) = Self::parse_variable_args(
                full_str,
                following,
                String::from("LIST"),
                &close_bracket_regex,
                0,
            )?;
            if let Some(close_bracket_cap) = close_bracket_regex.captures(&following_after_expr) {
                let (_, [following_after_bracket]) = close_bracket_cap.extract();
                return Self::parse_dependent_expr(
                    full_str,
                    following_after_bracket,
                    Formula::LiteralArray(array_item_formulae),
                );
            } else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Expected ']' character."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Check for a string literal
        let string_literal_regex: Regex = Regex::new(r#"(?s)^\s*"((?:[^\\"]|\\.)*)"(.*)"#).unwrap();
        if let Some(string_literal_cap) = string_literal_regex.captures(remaining_str) {
            let (_, [string_literal_content, following]) = string_literal_cap.extract();
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::LiteralString(String::from(string_literal_content)),
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
                return Err(Error::FormulaParseError {
                    msg: String::from("Unable to parse hexadecimal integer literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::LiteralInt(int_literal),
            );
        }

        // Check for a real literal
        let real_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d*\.\d+)(.*)"#).unwrap();
        if let Some(real_literal_cap) = real_literal_regex.captures(remaining_str) {
            let (_, [real_literal_content, following]) = real_literal_cap.extract();
            let Ok(real_literal) = real_literal_content.parse::<f64>() else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Unable to parse float literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::LiteralFloat(real_literal),
            );
        }

        // Check for an integer literal
        let int_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d+)(.*)"#).unwrap();
        if let Some(int_literal_cap) = int_literal_regex.captures(remaining_str) {
            let (_, [int_literal_content, following]) = int_literal_cap.extract();
            let Ok(int_literal) = int_literal_content.parse::<i64>() else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Unable to parse integer literal."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::LiteralInt(int_literal),
            );
        }

        // Check for a true/false boolean literal
        let bool_literal_regex: Regex = Regex::new(r#"(?is)^\s*(true|false)(.*)"#).unwrap();
        if let Some(bool_literal_cap) = bool_literal_regex.captures(remaining_str) {
            let (_, [bool_literal_content, following]) = bool_literal_cap.extract();
            let bool_literal = bool_literal_content.to_uppercase() == "TRUE";
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::LiteralBool(bool_literal),
            );
        }

        // Check for a null literal
        let null_literal_regex: Regex = Regex::new(r#"(?is)^\s*null(.*)"#).unwrap();
        if let Some(null_literal_cap) = null_literal_regex.captures(remaining_str) {
            let (_, [following]) = null_literal_cap.extract();
            return Self::parse_dependent_expr(full_str, following, Formula::Null);
        }

        // Check for a parameter
        let param_regex: Regex = Regex::new(
            r#"(?is)^\s*@\{(ROOT\d+(?:_MASTER\d+|_INHERITOR\d+|_COLUMN\d+)*)_COLUMN(\d+)\}(.*)"#,
        )
        .unwrap();
        if let Some(param_cap) = param_regex.captures(remaining_str) {
            let (_, [datasource_alias, column_oid_content, following]) = param_cap.extract();
            let Ok(column_oid) = column_oid_content.parse::<i64>() else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Unable to parse formula parameter."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            };
            return Self::parse_dependent_expr(
                full_str,
                following,
                Formula::Param {
                    datasource_alias: String::from(datasource_alias),
                    column_oid,
                },
            );
        }

        // Check for a function call
        let fn_regex: Regex = Regex::new(r#"(?is)^\s*(random|abs|sign|pow|round|floor|ceil|format|lower|upper|substr|replace|length|ismatch|if|switch|coalesce|nullif|sum|avg|min|max|count|join)\s*\((.*)"#).unwrap();
        if let Some(fn_cap) = fn_regex.captures(remaining_str) {
            let (_, [fn_name, following]) = fn_cap.extract();

            let regular_fn_name: String = fn_name.to_lowercase();
            if regular_fn_name == "random" {
                // Pseudo-random number

                let ([], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(full_str, &after_fn_close, Formula::RandomInt);
            } else if regular_fn_name == "abs" {
                // Absolute value of number

                let ([abs_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Abs(Box::from(abs_arg)),
                );
            } else if regular_fn_name == "sign" {
                // Sign of number

                let ([sign_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Sign(Box::from(sign_arg)),
                );
            } else if regular_fn_name == "round" {
                // Round number to nearest value

                let ([round_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Round(Box::from(round_arg)),
                );
            } else if regular_fn_name == "floor" {
                // Round number down to nearest whole number

                let ([floor_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Floor(Box::from(floor_arg)),
                );
            } else if regular_fn_name == "ceil" {
                // Round number up to nearest whole number

                let ([ceil_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Ceiling(Box::from(ceil_arg)),
                );
            } else if regular_fn_name == "pow" {
                // Raise LHS to the power of RHS

                let ([exp_lhs, exp_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Exponent(Box::from(exp_lhs), Box::from(exp_rhs)),
                );
            } else if regular_fn_name == "lower" {
                // Lowercase of string

                let ([lower_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Lowercase(Box::from(lower_arg)),
                );
            } else if regular_fn_name == "upper" {
                // Uppercase of string

                let ([upper_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Uppercase(Box::from(upper_arg)),
                );
            } else if regular_fn_name == "substr" {
                // Extract substring from string

                let (substr_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    2,
                )?;

                let substr_formula: Formula = Formula::Substring {
                    str: Box::from(substr_args[0].clone()),
                    start: Box::from(substr_args[1].clone()),
                    length: if substr_args.len() > 3 {
                        return Err(Error::FormulaParseError {
                            msg: String::from("Too many arguments for function substr."),
                            full_formula: full_str.clone(),
                            substring_with_error: String::from(remaining_str.trim_start()),
                        });
                    } else if substr_args.len() > 2 {
                        Some(Box::from(substr_args[2].clone()))
                    } else {
                        None
                    },
                };

                return Self::parse_dependent_expr(full_str, &after_fn_close, substr_formula);
            } else if regular_fn_name == "replace" {
                // String replacement

                let ([original_arg, pattern_arg, replacement_arg], after_fn_close) =
                    Self::parse_fixed_args(
                        full_str,
                        following,
                        regular_fn_name,
                        &close_parenthesis_regex,
                    )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Replace {
                        original: Box::from(original_arg),
                        pattern: Box::from(pattern_arg),
                        replacement: Box::from(replacement_arg),
                    },
                );
            } else if regular_fn_name == "length" {
                // Length of string

                let ([length_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Length(Box::from(length_arg)),
                );
            } else if regular_fn_name == "format" {
                // Format arguments into string

                let (format_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    1,
                )?;

                let format_formula: Formula = Formula::Format {
                    format: Box::from(format_args[0].clone()),
                    format_params: format_args[1..].to_vec(),
                };

                return Self::parse_dependent_expr(full_str, &after_fn_close, format_formula);
            } else if regular_fn_name == "ismatch" {
                // Matches a GLOB pattern against the contents of the string

                let ([glob_lhs, glob_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Glob {
                        str: Box::from(glob_lhs),
                        pattern: Box::from(glob_rhs),
                    },
                );
            } else if regular_fn_name == "if" {
                // Branch statement

                let (cond_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    2,
                )?;

                let conditional_formula = Formula::Conditional {
                    condition: Box::from(cond_args[0].clone()),
                    formula_if_true: Box::from(cond_args[1].clone()),
                    formula_if_false: if cond_args.len() > 3 {
                        return Err(Error::FormulaParseError {
                            msg: String::from("Too many arguments for function if."),
                            full_formula: full_str.clone(),
                            substring_with_error: String::from(remaining_str.trim_start()),
                        });
                    } else if cond_args.len() > 2 {
                        Box::from(cond_args[2].clone())
                    } else {
                        Box::from(Formula::Null)
                    },
                };

                return Self::parse_dependent_expr(full_str, &after_fn_close, conditional_formula);
            } else if regular_fn_name == "switch" {
                // Switch statement, return expression associated with first to match value

                let (switch_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    3,
                )?;

                let switch_chunks = switch_args[1..(switch_args.len() - 1)].as_chunks::<2>();
                let switch_formula = Formula::Switch {
                    value: Box::from(switch_args[0].clone()),
                    matches: switch_chunks
                        .0
                        .iter()
                        .map(|tup| (tup[0].clone(), tup[1].clone()))
                        .collect(),
                    formula_if_no_match: if switch_chunks.1.len() == 0 {
                        Box::from(Formula::Null)
                    } else {
                        Box::from(switch_chunks.1[0].clone())
                    },
                };

                return Self::parse_dependent_expr(full_str, &after_fn_close, switch_formula);
            } else if regular_fn_name == "coalesce" {
                // Return first non-null argument

                let (coalesce_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    2,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Coalesce(coalesce_args),
                );
            } else if regular_fn_name == "nullif" {
                // Return null if the two values match, otherwise return the first value

                let ([nullif_lhs, nullif_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::NullIf {
                        value: Box::from(nullif_lhs),
                        null_if_match: Box::from(nullif_rhs),
                    },
                );
            } else if regular_fn_name == "sum" {
                // Sum of numbers in collection

                let ([sum_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Sum(Box::from(sum_arg)),
                );
            } else if regular_fn_name == "avg" {
                // Average of numbers in collection

                let ([avg_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Average(Box::from(avg_arg)),
                );
            } else if regular_fn_name == "min" {
                // If 1 argument is provided, return minimum item in collection
                // If >1 argument is provided, return minimum argument

                let (min_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    1,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    if min_args.len() > 1 {
                        Formula::Argmin(min_args)
                    } else {
                        Formula::Min(Box::from(min_args[0].clone()))
                    },
                );
            } else if regular_fn_name == "max" {
                // If 1 argument is provided, return maximum item in collection
                // If >1 argument is provided, return maximum argument

                let (max_args, after_fn_close) = Self::parse_variable_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                    1,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    if max_args.len() > 1 {
                        Formula::Argmax(max_args)
                    } else {
                        Formula::Max(Box::from(max_args[0].clone()))
                    },
                );
            } else if regular_fn_name == "count" {
                // Count items in collection

                let ([count_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Count(Box::from(count_arg)),
                );
            } else if regular_fn_name == "join" {
                // Collection concatenation by delimiter

                let ([join_arg, join_delimiter], after_fn_close) = Self::parse_fixed_args(
                    full_str,
                    following,
                    regular_fn_name,
                    &close_parenthesis_regex,
                )?;
                return Self::parse_dependent_expr(
                    full_str,
                    &after_fn_close,
                    Formula::Join {
                        collection: Box::from(join_arg),
                        delimiter: Box::from(join_delimiter),
                    },
                );
            } else {
                return Err(Error::FormulaParseError {
                    msg: String::from("Unknown function name."),
                    full_formula: full_str.clone(),
                    substring_with_error: String::from(remaining_str.trim_start()),
                });
            }
        }

        // Check for NOT unary operator
        let not_regex: Regex = Regex::new(r#"(?is)^\s*(?:!|not\b)(.*)"#).unwrap();
        if let Some(not_cap) = not_regex.captures(remaining_str) {
            let (_, [following]) = not_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Not(Box::from(rhs)), following_rhs));
        }

        return Err(Error::FormulaParseError {
            msg: String::from("Unknown formula expression."),
            full_formula: full_str.clone(),
            substring_with_error: String::from(remaining_str.trim_start()),
        });
    }

    /// Parse a formula from a string.
    pub fn parse(str: String) -> Result<Self, Error> {
        // Parse the formula
        let (parsed_formula, remainder) = Self::parse_expr(&str, &str)?;
        let nonempty_regex: Regex = Regex::new(r#"\S"#).unwrap();
        if nonempty_regex.is_match(&remainder) {
            return Err(Error::FormulaParseError {
                msg: String::from("Unexpected character."),
                full_formula: str,
                substring_with_error: String::from(remainder.trim_start()),
            });
        }

        // Return validated formula
        return Ok(parsed_formula);
    }


    /// Iterates over all parameters used by the formula.
    pub fn iter_all_params(&self) -> Iterator<(String, i64)> {
        match self {
            Formula::Null 
            | Formula::LiteralBool(_) 
            | Formula::LiteralFloat(_) 
            | Formula::LiteralInt(_) 
            | Formula::LiteralString(_) 
            | Formula::RandomInt => {
                Vec::new()
            }
            
            /*
             * Single-parameter functions
             */

            Formula::Wrap(inner)
            | Formula::Round(inner)
            | Formula::Ceiling(inner)
            | Formula::Floor(inner)
            | Formula::Sign(inner)
            | Formula::Abs(inner)
            | Formula::Lowercase(inner)
            | Formula::Uppercase(inner)
            | Formula::Length(inner)
            | Formula::Not(inner) => {
                inner.iter_all_params()
            }
            
            /*
             * Two-parameter operation functions
             */

            Formula::Add(lhs, rhs) 
            | Formula::Subtract(lhs, rhs) 
            | Formula::Multiply(lhs, rhs) 
            | Formula::Modulo(lhs, rhs) 
            | Formula::Divide(lhs, rhs) 
            | Formula::Exponent(lhs, rhs) 
            | Formula::Concat(lhs, rhs) 
            | Formula::And(lhs, rhs) 
            | Formula::Or(lhs, rhs) 
            | Formula::Eq(lhs, rhs) 
            | Formula::LessThan(lhs, rhs) 
            | Formula::LessThanOrEq(lhs, rhs) 
            | Formula::Glob { str: lhs, pattern: rhs } 
            | Formula::Index { collection: lhs, index: rhs } 
            | Formula::NullIf { value: lhs, null_if_match: rhs } => {
                lhs.iter_all_params()
                    .chain(rhs.iter_all_params())
            }

            /*
             * Three-parameter functions
             */

            Formula::Conditional { condition: x1, formula_if_true: x2, formula_if_false: x3 } 
            | Formula::Substring { str: x1, start: x2, length: x3 } 
            | Formula::Replace { original: x1, pattern: x2, replacement: x3 } => {
                x1.iter_all_params()
                    .chain(x2.iter_all_params())
                    .chain(x3.iter_all_params())
            }

            /*
             * Arbitrary parameter functions
             */

            Formula::LiteralArray(inners) 
            | Formula::Coalesce(inners) 
            | Formula::Argmax(inners) 
            | Formula::Argmin(inners) => {
                inners.iter().flat_map(|inner| inner.iter_all_params()) 
            }

            Formula::Switch { value, matches, formula_if_no_match } => {
                vec![value, formula_if_no_match].iter()
                    .flat_map(|inner| inner.iter_all_params())
                    .chain(
                        matches.iter()
                            .flat_map(|(x1, x2)| {
                                x1.iter_all_params()
                                    .chain(x2.iter_all_params())
                            })
                    )
            }

            Formula::Format { format, format_params } => {
                vec![format].iter()
                    .flat_map(|inner| inner.iter_all_params())
                    .chain(
                        format_params.iter()
                            .flat_map(|inner| {
                                inner.iter_all_params()
                            })
                    )
            }

            /*
             * Aggregation functions
             */

            Formula::Count(collection)
            | Formula::Average(collection) 
            | Formula::Sum(collection) 
            | Formula::Max(collection) 
            | Formula::Min(collection) => {
                collection.iter_all_params()
            }

            Formula::Join { collection, delimiter } => {
                collection.iter_all_params()
                    .chain(delimiter.iter_all_params())
            }

            /*
             * Parameter
             */
            
            Formula::Param { datasource_alias, column_oid } => {
                vec![datasource_alias.clone(), column_oid.clone()]
            }
        }
    }

    /// Determines the scalar type of the value returned by this formula.
    pub fn get_scalar_type(&self, conn: &Connection) -> Result<FormulaReturnType, Error> {
        /// Verifies that the scalar type of a formula conforms to an expected type.
        macro_rules! verify_scalar_type {
            ( $s:expr, $expected_type:expr, $to_verify:expr ) => {
                {
                    let inner_name: String = $to_verify.to_string();
                    let inner_scalar_type: FormulaReturnType = $to_verify.get_scalar_type(conn)?;
                    if !$expected_type.encompasses(&inner_scalar_type) {
                        return Err(Error::FormulaTypeValidationError {
                            outer_name: $s, 
                            inner_name,
                            expected_type: $expected_type.to_string(), 
                            received_type: inner_scalar_type.to_string()
                        });
                    }
                    inner_scalar_type
                }
            };
        }

        Ok(match self {
            Formula::Null => FormulaReturnType::new(),
            Formula::LiteralBool(_) => FormulaReturnType::from(column_type::Primitive::Boolean),
            Formula::LiteralFloat(_) => FormulaReturnType::from(column_type::Primitive::Number),
            Formula::LiteralInt(_) => FormulaReturnType::from(column_type::Primitive::Integer),
            Formula::LiteralString(_) => FormulaReturnType::from(column_type::Primitive::PlainText),
            Formula::RandomInt => FormulaReturnType::from(column_type::Primitive::Integer),
            
            /*
             * Single-parameter functions
             */

            Formula::Wrap(inner) => inner.get_scalar_type(conn)?,

            Formula::Round(inner) => {
                verify_scalar_type!(
                    "Argument x of ROUND(x: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    inner 
                );
                FormulaReturnType::from(column_type::Primitive::Integer)
            }
            Formula::Ceiling(inner) => {
                verify_scalar_type!(
                    "Argument x of CEIL(x: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    inner 
                );
                FormulaReturnType::from(column_type::Primitive::Integer)
            }
            Formula::Floor(inner) => {
                verify_scalar_type!(
                    "Argument x of FLOOR(x: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    inner 
                );
                FormulaReturnType::from(column_type::Primitive::Integer)
            }
            Formula::Sign(inner) => {
                verify_scalar_type!(
                    "Argument x of SIGN(x: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    inner 
                );
                FormulaReturnType::from(column_type::Primitive::Integer)
            }
            Formula::Abs(inner) => {
                verify_scalar_type!(
                    "Argument x of ABS(x: Number)", 
                    FormulaReturnType::from(column_type::Primitive::Number),
                    inner
                )
            }

            Formula::Lowercase(inner) => {
                verify_scalar_type!(
                    "Argument x of UPPER(x: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    inner 
                )
            }
            Formula::Uppercase(inner) => {
                verify_scalar_type!(
                    "Argument x of UPPER(x: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    inner 
                )
            }
            Formula::Length(inner) => {
                verify_scalar_type!(
                    "Argument x of LENGTH(x: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    inner 
                );
                FormulaReturnType::from(column_type::Primitive::Integer)
            }

            Formula::Not(inner) => {
                verify_scalar_type!(
                    "Argument x of NOT(x: Boolean)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    inner 
                )
            }
            
            /*
             * Two-parameter operation functions
             */

            Formula::Add(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of ADD(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of ADD(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            Formula::Subtract(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of SUBTRACT(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of SUBTRACT(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            Formula::Multiply(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of MULTIPLY(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of MULTIPLY(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            Formula::Modulo(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of MODULO(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of MODULO(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            Formula::Divide(lhs, rhs) => {
                verify_scalar_type!(
                    "Argument lhs of DIVIDE(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                verify_scalar_type!(
                    "Argument rhs of DIVIDE(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                FormulaReturnType::from(column_type::Primitive::Number)
            }
            Formula::Exponent(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of POW(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of POW(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            
            Formula::Concat(lhs, rhs) => {
                verify_scalar_type!(
                    "Argument lhs of CONCAT(lhs: Text, rhs: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    lhs
                );
                verify_scalar_type!(
                    "Argument rhs of CONCAT(lhs: Text, rhs: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    rhs
                );
                FormulaReturnType::from(column_type::Primitive::PlainText)
            }

            Formula::And(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of AND(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of AND(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }
            Formula::Or(lhs, rhs) => {
                let lhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument lhs of OR(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    lhs
                );
                let rhs_scalar_type: FormulaReturnType = verify_scalar_type!(
                    "Argument rhs of OR(lhs: Number, rhs: Number)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    rhs
                );
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }

            Formula::Eq(lhs, rhs) => FormulaReturnType::from(column_type::Primitive::Boolean),
            Formula::LessThan(lhs, rhs) => {
                let inner_expected_type: FormulaReturnType = FormulaReturnType::from(column_type::Primitive::Number)
                    .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText));
                verify_scalar_type!(
                    "Argument lhs of LESSTHAN(lhs: Number | Text, rhs: Number | Text)",
                    inner_expected_type,
                    lhs 
                );
                verify_scalar_type!(
                    "Argument rhs of LESSTHAN(lhs: Number | Text, rhs: Number | Text)",
                    inner_expected_type,
                    rhs
                );
                FormulaReturnType::from(column_type::Primitive::Boolean)
            }
            Formula::LessThanOrEq(lhs, rhs) => {
                let inner_expected_type: FormulaReturnType = FormulaReturnType::from(column_type::Primitive::Number)
                    .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText));
                verify_scalar_type!(
                    "Argument lhs of LESSTHANEQ(lhs: Number | Text, rhs: Number | Text)",
                    inner_expected_type,
                    lhs 
                );
                verify_scalar_type!(
                    "Argument rhs of LESSTHANEQ(lhs: Number | Text, rhs: Number | Text)",
                    inner_expected_type,
                    rhs
                );
                FormulaReturnType::from(column_type::Primitive::Boolean)
            }

            Formula::Glob { str, pattern } => {
                verify_scalar_type!(
                    "Argument str of ISMATCH(str: Text, pattern: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    str 
                );
                verify_scalar_type!(
                    "Argument pattern of ISMATCH(str: Text, pattern: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    pattern
                );
                FormulaReturnType::from(column_type::Primitive::Boolean)
            }

            Formula::Index { collection, index } => {
                verify_scalar_type!(
                    "Argument idx of INDEX(x: List<Any>, idx: Integer)",
                    FormulaReturnType::from(column_type::Primitive::Integer),
                    index
                );
                collection.get_scalar_type(conn)?
            }
            Formula::NullIf { value, null_if_match } => value.get_scalar_type(conn)?,

            /*
             * Three-parameter functions
             */

            Formula::Conditional { condition, formula_if_true, formula_if_false } => {
                verify_scalar_type!(
                    "Argument x of IF(x: Boolean, a: Any, b: Any)",
                    FormulaReturnType::from(column_type::Primitive::Boolean),
                    condition
                );
                let lhs_scalar_type: FormulaReturnType = formula_if_true.get_scalar_type(conn)?;
                let rhs_scalar_type: FormulaReturnType = formula_if_false.get_scalar_type(conn)?;
                lhs_scalar_type.generalize(&rhs_scalar_type)
            }

            Formula::Substring { str, start, length } => {
                verify_scalar_type!(
                    match length {
                        Some(_) => "Argument str of SUBSTRING(str: Text, start: Integer, length: Integer)",
                        None => "Argument str of SUBSTRING(str: Text, start: Integer)"
                    },
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    str 
                );
                verify_scalar_type!(
                    match length {
                        Some(_) => "Argument start of SUBSTRING(str: Text, start: Integer, length: Integer)",
                        None => "Argument start of SUBSTRING(str: Text, start: Integer)"
                    },
                    FormulaReturnType::from(column_type::Primitive::Integer),
                    start 
                );
                if let Some(length) = length {
                    verify_scalar_type!(
                        "Argument length of SUBSTRING(str: Text, start: Integer, length: Integer)",
                        FormulaReturnType::from(column_type::Primitive::Integer),
                        length 
                    );
                }
                FormulaReturnType::from(column_type::Primitive::PlainText)
            }
            Formula::Replace { original, pattern, replacement } => {
                verify_scalar_type!(
                    "Argument str of REPLACE(str: Text, pattern: Text, replacement: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    original 
                );
                verify_scalar_type!(
                    "Argument pattern of REPLACE(str: Text, pattern: Text, replacement: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    pattern 
                );
                verify_scalar_type!(
                    "Argument replacement of REPLACE(str: Text, pattern: Text, replacement: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    replacement
                );
                FormulaReturnType::from(column_type::Primitive::PlainText)
            }

            /*
             * Arbitrary parameter functions
             */

            Formula::LiteralArray(inners) => {
                let mut scalar_type: FormulaReturnType = FormulaReturnType::new();
                for inner in inners {
                    scalar_type = scalar_type.generalize(&inner.get_scalar_type(conn)?);
                }
                scalar_type
            }

            Formula::Coalesce(inners) => {
                let mut scalar_type: FormulaReturnType = FormulaReturnType::new();
                for inner in inners {
                    scalar_type = scalar_type.generalize(&inner.get_scalar_type(conn)?);
                }
                scalar_type
            }
            Formula::Argmax(inners) => {
                let inner_expected_type: FormulaReturnType = FormulaReturnType::from(column_type::Primitive::Number)
                    .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText));
                let mut scalar_type: FormulaReturnType = FormulaReturnType::new();
                for inner in inners {
                    scalar_type = scalar_type.generalize(&verify_scalar_type!(
                        "Argument x of ARGMAX(...x: Number | Text)",
                        inner_expected_type,
                        inner 
                    ));
                }
                scalar_type
            }
            Formula::Argmin(inners) => {
                let inner_expected_type: FormulaReturnType = FormulaReturnType::from(column_type::Primitive::Number)
                    .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText));
                let mut scalar_type: FormulaReturnType = FormulaReturnType::new();
                for inner in inners {
                    scalar_type = scalar_type.generalize(&verify_scalar_type!(
                        "Argument x of ARGMIN(...x: Number | Text)",
                        inner_expected_type,
                        inner 
                    ));
                }
                scalar_type
            }
            Formula::Switch { value, matches, formula_if_no_match } => {
                let mut scalar_type: FormulaReturnType = FormulaReturnType::new();
                for (_, inner) in matches {
                    scalar_type = scalar_type.generalize(&inner.get_scalar_type(conn)?);
                }
                if let Some(inner) = formula_if_no_match {
                    scalar_type = scalar_type.generalize(&inner.get_scalar_type(conn)?);
                }
                scalar_type
            }

            Formula::Format { format, format_params } => {
                verify_scalar_type!(
                    "Argument format of FORMAT(format: Text, ...x: Any)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    format 
                );
                FormulaReturnType::from(column_type::Primitive::PlainText)
            }

            /*
             * Aggregation functions
             */

            Formula::Count(collection) => FormulaReturnType::from(column_type::Primitive::Integer),

            Formula::Average(collection) => {
                verify_scalar_type!(
                    "Argument x of AVG(x: List<Number>)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    collection
                );
                FormulaReturnType::from(column_type::Primitive::Number)
            }
            Formula::Sum(collection) => {
                verify_scalar_type!(
                    "Argument x of SUM(x: List<Number>)",
                    FormulaReturnType::from(column_type::Primitive::Number),
                    collection
                )
            }
            Formula::Max(collection) => {
                verify_scalar_type!(
                    "Argument x of MAX(x: List<Number | Text>)",
                    FormulaReturnType::from(column_type::Primitive::Number)
                        .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText)),
                    collection
                )
            }
            Formula::Min(collection) => {
                verify_scalar_type!(
                    "Argument x of MIN(x: List<Number | Text>)",
                    FormulaReturnType::from(column_type::Primitive::Number)
                        .generalize(&FormulaReturnType::from(column_type::Primitive::PlainText)),
                    collection
                )
            }

            Formula::Join { collection, delimiter } => {
                verify_scalar_type!(
                    "Argument x of JOIN(x: List<Text>, delimiter: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    collection
                );
                verify_scalar_type!(
                    "Argument delimiter of JOIN(x: List<Text>, delimiter: Text)",
                    FormulaReturnType::from(column_type::Primitive::PlainText),
                    delimiter
                );
                FormulaReturnType::from(column_type::Primitive::PlainText)
            }

            /*
             * Parameter
             */
            
            Formula::Param { column_oid, .. } => {
                match column::FullMetadata::get_transact(conn, column_oid.clone()) {
                    Ok(column_metadata) => {
                        match column_metadata.column_type {
                            column_type::ColumnType::Primitive(prim) => FormulaReturnType::from(prim),
                            _ => FormulaReturnType::new()
                        }
                    }
                    Err(Error::SqlError { .. }) => {
                        // Parameter has been orphaned
                        FormulaReturnType::new()
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        })
    }

    /// Converts formula to a basic string indicating the function name.
    pub fn to_string(&self) -> String {
        match self {
            Self::Abs(_) => String::from("ABS(x: Number) -> Number"),
            Self::Add(_, _) => String::from("ADD<_T: Number>(lhs: _T, rhs: _T) -> _T"),
            Self::And(_, _) => String::from("AND(lhs: Boolean, rhs: Boolean) -> Boolean"),
            Self::Argmax(_) => String::from("MAX<_T: Number>(...args: _T) -> _T"),
            Self::Argmin(_) => String::from("MIN<_T: Number>(...args: _T) -> _T"),
            Self::Average(_) => String::from("AVG(collection: List<Number>) -> Number"),
            Self::Ceiling(_) => String::from("CEIL(x: Number) -> Integer"),
            Self::Coalesce(_) => String::from("COALESCE<_T: Any>(...args: _T) -> _T"),
            Self::Concat(_, _) => String::from("CONCAT(lhs: Text, rhs: Text) -> Text"),
            Self::Conditional { .. } => String::from("IF<_T: Any>(condition: Boolean, valueIfTrue: _T, valueIfFalse?: _T) -> _T"),
            Self::Count(_) => String::from("COUNT(collection: List<Any>) -> Integer"),
            Self::Divide(_, _) => String::from("DIVIDE(lhs: Number, rhs: Number) -> Number"),
            Self::Eq(_, _) => String::from("EQ(lhs: Any, rhs: Any) -> Boolean"),
            Self::Exponent(_, _) => String::from("POW<_T: Number>(base: _T, exponent: _T) -> _T"),
            Self::Floor(_) => String::from("FLOOR(x: Number) -> Integer"),
            Self::Format { .. } => String::from("FORMAT(str: Text, ...args: Any) -> Text"),
            Self::Glob { .. } => String::from("ISMATCH(str: Text, pattern: Text) -> Boolean"),
            Self::In { .. } => String::from("CONTAINS(collection: List<Any>, x: Any) -> Boolean"),
            Self::Index { .. } => String::from("INDEX<_T: Any>(collection: List<_T>, index: Integer) -> _T"),
            Self::Join { .. } => String::from("JOIN(collection: List<Text>) -> Text"),
            Self::Length(_) => String::from("LENGTH(x: Text) -> Integer"),
            Self::LessThan(_, _) => String::from("LESSTHAN(lhs: Number, rhs: Number) -> Boolean"),
            Self::LessThanOrEq(_, _) => String::from("LESSTHANEQUALTO(lhs: Number, rhs: Number) -> Boolean"),
            Self::LiteralArray(items) => String::from("LIST<_T>(...args: _T) -> List<_T>"),
            Self::LiteralBool(b) => String::from(if *b { "true" } else { "false" }),
            Self::LiteralFloat(lit) => format!("{lit}"),
            Self::LiteralInt(lit) => format!("{lit}"),
            Self::LiteralString(str) => format!("\"{}\"", str.replace("\"", "\\\"").replace("\\", "\\\\")),
            Self::Lowercase(_) => String::from("LOWER<_T: Text>(x: _T) -> _T"),
            Self::Max(_) => String::from("MAX<_T: Number>(collection: List<_T>) -> _T"),
            Self::Min(_) => String::from("MIN<_T: Number>(collection: List<_T>) -> _T"),
            Self::Modulo(_, _) => String::from("MODULO<_T: Number>(numerator: _T, modulus: _T) -> _T"),
            Self::Multiply(_, _) => String::from("MULTIPLY<_T: Number>(lhs: _T, rhs: _T) -> _T"),
            Self::Not(_) => String::from("NOT(x: Boolean) -> Boolean"),
            Self::Null => String::from("null"),
            Self::NullIf { .. } => String::from("NULLIF<_T: Any>(x: _T, y: Any) -> _T"),
            Self::Or(_, _) => String::from("OR(lhs: Boolean"),
            Self::Param { .. } => String::from("PARAM"),
            Self::RandomInt => String::from("RANDOM() -> Integer"),
            Self::Replace { .. } => String::from("REPLACE(str: Text, pattern: Text, replacement: Text) -> Text"),
            Self::Round(_) => String::from("ROUND(x: Number) -> Integer"),
            Self::Sign(_) => String::from("SIGN(x: Number) -> Integer"),
            Self::Substring { .. } => String::from("SUBSTRING(str: Text, start: Integer, length?: Integer) -> Text"),
            Self::Subtract(_, _) => String::from("SUBTRACT<_T: Number>(lhs: _T, rhs: _T) -> _T"),
            Self::Sum(_) => String::from("SUM<_T: Number>(collection: List<_T>) -> _T"),
            Self::Switch { .. } => String::from("SWITCH<_T: Any>(x: Any, ...[matchedValue1: Any, returnedValue1: _T], returnValueIfNoMatch?: _T) -> _T"),
            Self::Uppercase(_) => String::from("UPPER<_T: Text>(x: _T) -> _T"),
            Self::Wrap(inner) => inner.to_string(),
        }
    }
}
