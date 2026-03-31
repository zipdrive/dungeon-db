use regex::Regex;
use crate::util::error;

#[derive(Clone)]
pub enum Formula {
    Param {
        datasource_path: Vec<String>,
        column_oid: i64
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
        collection: Box<Formula>
    },
    Glob {
        str: Box<Formula>, 
        pattern: Box<Formula>
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
        length: Option<Box<Formula>>
    },
    Replace {
        original: Box<Formula>,
        pattern: Box<Formula>,
        replacement: Box<Formula>
    },
    Length(Box<Formula>),
    Format {
        format: Box<Formula>,
        format_params: Vec<Formula>
    },

    Slice {
        collection: Box<Formula>,
        start: i64,
        length: i64
    },
    
    Wrap(Box<Formula>),
    Argmin(Vec<Formula>),
    Argmax(Vec<Formula>),
    Coalesce(Vec<Formula>),
    Conditional {
        condition: Box<Formula>,
        formula_if_true: Box<Formula>,
        formula_if_false: Box<Formula>
    },
    Switch {
        value: Box<Formula>,
        matches: Vec<(Formula, Formula)>,
        formula_if_no_match: Box<Formula>
    },
    NullIf {
        value: Box<Formula>,
        null_if_match: Box<Formula>
    },

    Sum(Box<Formula>),
    Average(Box<Formula>),
    Min(Box<Formula>),
    Max(Box<Formula>),
    Count(Box<Formula>),
    Join {
        collection: Box<Formula>,
        delimiter: Box<Formula>
    }
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
            _ => None
        }
    }

    /// Rotates the order of evaluation of binary operations, according to the rules laid out in Formula::binary_operator_precedence().
    fn binary_operator_rotate<F: FnOnce(Self, Self) -> Self>(self, outer_precedence: usize, lhs: Self, construct_operator: F) -> Self {
        if let Some(self_precedence) = self.binary_operator_precedence() {
            if self_precedence < outer_precedence {
                // Do the rotation
                match self {
                    Self::Or(mid, rhs) => { return Self::Or(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::And(mid, rhs) => { return Self::And(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Not(rhs) => { return Self::Not(Box::new(construct_operator(lhs, *rhs))); }
                    Self::Eq(mid, rhs) => { return Self::Eq(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::In { value: mid, collection: rhs } => { return Self::In { value: Box::new(construct_operator(lhs, *mid)), collection: rhs }; },
                    Self::LessThan(mid, rhs) => { return Self::LessThan(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::LessThanOrEq(mid, rhs) => { return Self::LessThanOrEq(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Add(mid, rhs) => { return Self::Add(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Subtract(mid, rhs) => { return Self::Subtract(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Multiply(mid, rhs) => { return Self::Multiply(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Divide(mid, rhs) => { return Self::Divide(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Modulo(mid, rhs) => { return Self::Modulo(Box::new(construct_operator(lhs, *mid)), rhs); },
                    Self::Concat(mid, rhs) => { return Self::Concat(Box::new(construct_operator(lhs, *mid)), rhs); },
                    _ => { /* This case shouldn't occur, but if it does then do not rotate */ }
                }
            } else {
                // Do not do rotation
            }
        }
        construct_operator(lhs, self)
    }

    /// Parses a fixed-length list of arguments.
    fn parse_fixed_args<const N: usize>(full_str: &String, remaining_str: &str, fn_name: String, arg_end_regex: &Regex) -> Result<([Self; N], String), error::Error> {
        let arg_divider_regex: Regex = Regex::new(r#"(?s)\s*,(.*)"#).unwrap();
        
        let mut formula_args: [Formula; N] = [const { Formula::Null }; N];
        let mut following: String = String::from(remaining_str);

        for k in 0..(N-1) {
            let tail: String;
            (formula_args[k], tail) = Self::parse_expr(full_str, &following)?;
            
            // Test for divider between prev argument and next argument
            if let Some(arg_divider_cap) = arg_divider_regex.captures(&tail) {
                let (_, [following_str]) = arg_divider_cap.extract();
                following = following_str.into();
            // Test if end of arguments
            } else if arg_end_regex.is_match(&following) {
                return Err(error::Error::FormulaParseError { 
                    msg: format!("Too few arguments for function {fn_name}."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            } else {
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unexpected character."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
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
            return Err(error::Error::FormulaParseError { 
                msg: format!("Too many arguments for function {fn_name}."), 
                full_formula: full_str.clone(), 
                substring_with_error: String::from(remaining_str.trim_start()) 
            }); 
        } else {
            // If argument is followed by neither end of argument nor transition to next argument, return error
            return Err(error::Error::FormulaParseError { 
                msg: format!("Unexpected character."), 
                full_formula: full_str.clone(), 
                substring_with_error: String::from(remaining_str.trim_start()) 
            }); 
        }
    }

    /// Parses a variable list of arguments.
    fn parse_variable_args(full_str: &String, remaining_str: &str, fn_name: String, arg_end_regex: &Regex, min_arg_count: usize) -> Result<(Vec<Self>, String), error::Error> {
        // Test to see if no arguments provided
        if let Some(arg_end_cap) = arg_end_regex.captures(remaining_str) {
            if min_arg_count == 0 {
                // If no minimum # expected arguments, return success
                let (_, [following_str]) = arg_end_cap.extract();
                return Ok((Vec::new(), String::from(following_str)));
            } else {
                // If not fulfilled minimum # expected arguments, return error
                return Err(error::Error::FormulaParseError { 
                    msg: format!("Too few arguments for function {fn_name}."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
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
                    return Err(error::Error::FormulaParseError { 
                        msg: format!("Too few arguments for function {fn_name}."), 
                        full_formula: full_str.clone(), 
                        substring_with_error: String::from(remaining_str.trim_start()) 
                    }); 
                }
            } else {
                // If argument is followed by neither end of argument nor transition to next argument, return error
                return Err(error::Error::FormulaParseError { 
                    msg: format!("Unexpected character."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            }
        }
    }

    /// Parses a single expression with an antecedent formula.
    fn parse_dependent_expr(full_str: &String, remaining_str: &str, lhs: Self) -> Result<(Self, String), error::Error> {

        // Check for OR operator
        let or_regex: Regex = Regex::new(r#"(?is)^\s*or\b(.*)"#).unwrap();
        if let Some(or_cap) = or_regex.captures(remaining_str) {
            let (_, [following]) = or_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(OR_PRECEDENCE, lhs, |lhs, rhs| Formula::Or(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for AND operator
        let and_regex: Regex = Regex::new(r#"(?is)^\s*and\b(.*)"#).unwrap();
        if let Some(and_cap) = and_regex.captures(remaining_str) {
            let (_, [following]) = and_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(AND_PRECEDENCE, lhs, |lhs, rhs| Formula::And(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for equals operator
        let equals_regex: Regex = Regex::new(r#"(?s)^\s*=(.*)"#).unwrap();
        if let Some(equals_cap) = equals_regex.captures(remaining_str) {
            let (_, [following]) = equals_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(EQ_PRECEDENCE, lhs, |lhs, rhs| Formula::Eq(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for not equals operator
        let neq_regex: Regex = Regex::new(r#"(?s)^\s*<>(.*)"#).unwrap();
        if let Some(neq_cap) = neq_regex.captures(remaining_str) {
            let (_, [following]) = neq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(EQ_PRECEDENCE, lhs, |lhs, rhs| Formula::Not(Box::new(Formula::Eq(Box::new(lhs), Box::new(rhs))))), 
                following_rhs
            ));
        }

        // Check for IN operator
        let in_regex: Regex = Regex::new(r#"(?is)^\s*in\b(.*)"#).unwrap();
        if let Some(in_cap) = in_regex.captures(remaining_str) {
            let (_, [following]) = in_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(IN_PRECEDENCE, lhs, |lhs, rhs| Formula::In { value: Box::new(lhs), collection: Box::new(rhs) }), 
                following_rhs
            ));
        }

        // Check for less-than-or-equals operator
        let leq_regex: Regex = Regex::new(r#"(?s)^\s*<=(.*)"#).unwrap();
        if let Some(leq_cap) = leq_regex.captures(remaining_str) {
            let (_, [following]) = leq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LTEQ_PRECEDENCE, lhs, |lhs, rhs| Formula::LessThanOrEq(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for greater-than-or-equals operator
        let geq_regex: Regex = Regex::new(r#"(?s)^\s*>=(.*)"#).unwrap();
        if let Some(geq_cap) = geq_regex.captures(remaining_str) {
            let (_, [following]) = geq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LTEQ_PRECEDENCE, lhs, |lhs, rhs| Formula::Not(Box::new(Formula::LessThan(Box::new(lhs), Box::new(rhs))))), 
                following_rhs
            ));
        }

        // Check for less-than operator
        let lt_regex: Regex = Regex::new(r#"(?s)^\s*<(.*)"#).unwrap();
        if let Some(lt_cap) = lt_regex.captures(remaining_str) {
            let (_, [following]) = lt_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LT_PRECEDENCE, lhs, |lhs, rhs| Formula::LessThan(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for greater-than operator
        let gt_regex: Regex = Regex::new(r#"(?s)^\s*>(.*)"#).unwrap();
        if let Some(gt_cap) = gt_regex.captures(remaining_str) {
            let (_, [following]) = gt_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(LT_PRECEDENCE, lhs, |lhs, rhs| Formula::Not(Box::new(Formula::LessThanOrEq(Box::new(lhs), Box::new(rhs))))), 
                following_rhs
            ));
        }
        
        // Check for addition operator
        let addition_regex: Regex = Regex::new(r#"(?s)^\s*\+(.*)"#).unwrap();
        if let Some(addition_cap) = addition_regex.captures(remaining_str) {
            let (_, [following]) = addition_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(ADD_PRECEDENCE, lhs, |lhs, rhs| Formula::Add(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for subtraction operator
        let subtraction_regex: Regex = Regex::new(r#"(?s)^\s*-(.*)"#).unwrap();
        if let Some(subtraction_cap) = subtraction_regex.captures(remaining_str) {
            let (_, [following]) = subtraction_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(SUBTRACT_PRECEDENCE, lhs, |lhs, rhs| Formula::Subtract(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for multiplication operator
        let multiplication_regex: Regex = Regex::new(r#"(?s)^\s*\*(.*)"#).unwrap();
        if let Some(multiplication_cap) = multiplication_regex.captures(remaining_str) {
            let (_, [following]) = multiplication_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(MULTIPLY_PRECEDENCE, lhs, |lhs, rhs| Formula::Multiply(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for division operator
        let division_regex: Regex = Regex::new(r#"(?s)^\s*/(.*)"#).unwrap();
        if let Some(division_cap) = division_regex.captures(remaining_str) {
            let (_, [following]) = division_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(DIVIDE_PRECEDENCE, lhs, |lhs, rhs| Formula::Divide(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for modulo operator
        let modulo_regex: Regex = Regex::new(r#"(?s)^\s*%(.*)"#).unwrap();
        if let Some(modulo_cap) = modulo_regex.captures(remaining_str) {
            let (_, [following]) = modulo_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(MODULO_PRECEDENCE, lhs, |lhs, rhs| Formula::Modulo(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for concatenation operator
        let concat_regex: Regex = Regex::new(r#"(?s)^\s*&(.*)"#).unwrap();
        if let Some(concat_cap) = concat_regex.captures(remaining_str) {
            let (_, [following]) = concat_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;

            // Apply binary order precedence
            return Ok((
                rhs.binary_operator_rotate(CONCAT_PRECEDENCE, lhs, |lhs, rhs| Formula::Concat(Box::new(lhs), Box::new(rhs))), 
                following_rhs
            ));
        }

        // Check for nth-value operator
        // For simplicity, nth-value operator only allows for literal integer indices, since on the backend it is implemented by LIMIT {length} OFFSET {start}
        let indexer_regex: Regex = Regex::new(r#"(?s)^\s*\{(\d+)(?::(\d+))?\}(.*)"#).unwrap();
        if let Some(indexer_cap) = indexer_regex.captures(remaining_str) {
            let (_, [slice_start, slice_end, following]) = indexer_cap.extract();
            let Ok(start) = slice_start.parse::<i64>() else {
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Expected a literal integer as index."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };

            match slice_end.parse::<i64>() {
                Ok(end) => {
                    if end < start {
                        return Err(error::Error::FormulaParseError { 
                            msg: String::from("End of slice cannot be before start of slice."), 
                            full_formula: full_str.clone(), 
                            substring_with_error: String::from(remaining_str.trim_start()) 
                        }); 
                    }

                    return Self::parse_dependent_expr(
                        full_str, 
                        following, 
                        Formula::Slice {
                            collection: Box::from(lhs),
                            start: start,
                            length: end - start
                        }
                    );
                },
                Err(_) => {
                    if slice_end != "" {
                        return Err(error::Error::FormulaParseError { 
                            msg: String::from("Expected a literal integer as end of slice."), 
                            full_formula: full_str.clone(), 
                            substring_with_error: String::from(remaining_str.trim_start()) 
                        }); 
                    }

                    return Self::parse_dependent_expr(
                        full_str, 
                        following, 
                        Formula::Slice {
                            collection: Box::from(lhs),
                            start: start,
                            length: 1
                        }
                    );
                }
            }
        }

        // If no known operator was appended, return the antecedent alone
        return Ok((lhs, String::from(remaining_str)));
    }

    /// Parses a single expression with no antecedent.
    fn parse_expr(full_str: &String, remaining_str: &str) -> Result<(Self, String), error::Error> {
        // Check for open parenthesis
        let open_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\((.*)"#).unwrap();
        let close_parenthesis_regex: Regex = Regex::new(r#"(?s)^\s*\)(.*)"#).unwrap();
        if let Some(open_parenthesis_cap) = open_parenthesis_regex.captures(remaining_str) {
            let (_, [following]) = open_parenthesis_cap.extract();
            let (inside_parenthesis_formula, following_after_expr) = Self::parse_expr(full_str, following)?;
            if let Some(close_parenthesis_cap) = close_parenthesis_regex.captures(&following_after_expr) {
                let (_, [following_after_parenthesis]) = close_parenthesis_cap.extract();
                return Self::parse_dependent_expr(
                    full_str, 
                    following_after_parenthesis, 
                    Formula::Wrap(Box::from(inside_parenthesis_formula)) // Wrap the inner expression to make sure it doesn't get shifted around by order of mathematical operations
                );
            } else {
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Expected ')' character."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
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
                String::from("ARRAY"),
                &close_bracket_regex,
                0
            )?;
            if let Some(close_bracket_cap) = close_bracket_regex.captures(&following_after_expr) {
                let (_, [following_after_bracket]) = close_bracket_cap.extract();
                return Self::parse_dependent_expr(
                    full_str, 
                    following_after_bracket, 
                    Formula::LiteralArray(array_item_formulae)
                );
            } else {
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Expected ']' character."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            }
        }

        // Check for a string literal
        let string_literal_regex: Regex = Regex::new(r#"(?s)^\s*"((?:[^\\"]|\\.)*)"(.*)"#).unwrap();
        if let Some(string_literal_cap) = string_literal_regex.captures(remaining_str) {
            let (_, [string_literal_content, following]) = string_literal_cap.extract();
            return Self::parse_dependent_expr(full_str, following, Formula::LiteralString(String::from(string_literal_content)));
        }

        // Check for a hexadecimal integer literal
        let hexint_literal_regex: Regex = Regex::new(r#"(?is)^\s*([+\-]?)0x([0-9a-f]+)(.*)"#).unwrap();
        if let Some(hexint_literal_cap) = hexint_literal_regex.captures(remaining_str) {
            let (_, [hexint_literal_sign, hexint_literal_content, following]) = hexint_literal_cap.extract();
            let hexint_literal_src: String = format!("{hexint_literal_sign}{hexint_literal_content}");
            let Ok(int_literal) = i64::from_str_radix(&hexint_literal_src, 16) else { 
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unable to parse hexadecimal integer literal."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };
            return Self::parse_dependent_expr(full_str, following, Formula::LiteralInt(int_literal));
        }

        // Check for a real literal
        let real_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d*\.\d+)(.*)"#).unwrap();
        if let Some(real_literal_cap) = real_literal_regex.captures(remaining_str) {
            let (_, [real_literal_content, following]) = real_literal_cap.extract();
            let Ok(real_literal) = real_literal_content.parse::<f64>() else { 
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unable to parse float literal."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };
            return Self::parse_dependent_expr(full_str, following, Formula::LiteralFloat(real_literal));
        }

        // Check for an integer literal
        let int_literal_regex: Regex = Regex::new(r#"(?s)^\s*([+\-]?\d+)(.*)"#).unwrap();
        if let Some(int_literal_cap) = int_literal_regex.captures(remaining_str) {
            let (_, [int_literal_content, following]) = int_literal_cap.extract();
            let Ok(int_literal) = int_literal_content.parse::<i64>() else { 
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unable to parse integer literal."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };
            return Self::parse_dependent_expr(full_str, following, Formula::LiteralInt(int_literal));
        }

        // Check for a true/false boolean literal
        let bool_literal_regex: Regex = Regex::new(r#"(?is)^\s*(true|false)(.*)"#).unwrap();
        if let Some(bool_literal_cap) = bool_literal_regex.captures(remaining_str) {
            let (_, [bool_literal_content, following]) = bool_literal_cap.extract();
            let bool_literal = bool_literal_content.to_uppercase() == "TRUE";
            return Self::parse_dependent_expr(full_str, following, Formula::LiteralBool(bool_literal));
        }

        // Check for a null literal
        let null_literal_regex: Regex = Regex::new(r#"(?is)^\s*null(.*)"#).unwrap();
        if let Some(null_literal_cap) = null_literal_regex.captures(remaining_str) {
            let (_, [following]) = null_literal_cap.extract();
            return Self::parse_dependent_expr(full_str, following, Formula::Null);
        }

        // Check for a parameter
        let param_regex: Regex = Regex::new(r#"(?is)^\s*@\{(\d+(?:_MASTER\d+|_INHERITOR\d+|_COLUMN\d+)*)_COLUMN(\d+)\}(.*)"#).unwrap();
        if let Some(param_cap) = param_regex.captures(remaining_str) {
            let (_, [datasource_path_content, column_oid_content, following]) = param_cap.extract();
            let datasource_path: Vec<String> = datasource_path_content.split('_').map(|s| String::from(s)).collect();
            let Ok(column_oid) = column_oid_content.parse::<i64>() else { 
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unable to parse formula parameter."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };
            return Self::parse_dependent_expr(full_str, following, Formula::Param { datasource_path, column_oid });
        }

        // Check for a function call
        let fn_regex: Regex = Regex::new(r#"(?is)^\s*(random|abs|sign|pow|round|floor|ceil|format|lower|upper|substr|replace|length|match|if|switch|coalesce|nullif|sum|avg|min|max|count|join)\s*\((.*)"#).unwrap();
        if let Some(fn_cap) = fn_regex.captures(remaining_str) {
            let (_, [fn_name, following]) = fn_cap.extract();

            let regular_fn_name: String = fn_name.to_lowercase();
            if regular_fn_name == "random" {
                // Pseudo-random number

                let ([], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::RandomInt
                );
            } else if regular_fn_name == "abs" {
                // Absolute value of number

                let ([abs_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Abs(Box::from(abs_arg))
                );
            } else if regular_fn_name == "sign" {
                // Sign of number

                let ([sign_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Sign(Box::from(sign_arg))
                );
            } else if regular_fn_name == "round" {
                // Round number to nearest value

                let ([round_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Round(Box::from(round_arg))
                );
            } else if regular_fn_name == "floor" {
                // Round number down to nearest whole number

                let ([floor_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Floor(Box::from(floor_arg))
                );
            } else if regular_fn_name == "ceil" {
                // Round number up to nearest whole number

                let ([ceil_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Ceiling(Box::from(ceil_arg))
                );
            } else if regular_fn_name == "pow" {
                // Raise LHS to the power of RHS

                let ([exp_lhs, exp_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Exponent(Box::from(exp_lhs), Box::from(exp_rhs))
                );
            } else if regular_fn_name == "lower" {
                // Lowercase of string

                let ([lower_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Lowercase(Box::from(lower_arg))
                );
            } else if regular_fn_name == "upper" {
                // Uppercase of string

                let ([upper_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Uppercase(Box::from(upper_arg))
                );
            } else if regular_fn_name == "substr" {
                // Extract substring from string

                let (substr_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    2
                )?;

                let substr_formula: Formula = Formula::Substring { 
                    str: Box::from(substr_args[0].clone()), 
                    start: Box::from(substr_args[1].clone()), 
                    length: if substr_args.len() > 3 {
                        return Err(error::Error::FormulaParseError { 
                            msg: String::from("Too many arguments for function substr."), 
                            full_formula: full_str.clone(), 
                            substring_with_error: String::from(remaining_str.trim_start()) 
                        });
                    } else if substr_args.len() > 2 {
                        Some(Box::from(substr_args[2].clone()))
                    } else {
                        None
                    }
                };

                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    substr_formula
                );
            } else if regular_fn_name == "replace" {
                // String replacement

                let ([original_arg, pattern_arg, replacement_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Replace { 
                        original: Box::from(original_arg), 
                        pattern: Box::from(pattern_arg), 
                        replacement: Box::from(replacement_arg)
                    }
                );
            } else if regular_fn_name == "length" {
                // Length of string

                let ([length_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Length(Box::from(length_arg))
                );
            } else if regular_fn_name == "format" {
                // Format arguments into string

                let (format_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    1
                )?;

                let format_formula: Formula = Formula::Format { 
                    format: Box::from(format_args[0].clone()), 
                    format_params: format_args[1..].to_vec()
                };

                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    format_formula
                );
            } else if regular_fn_name == "match" {
                // Matches a GLOB pattern against the contents of the string

                let ([glob_lhs, glob_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Glob {
                        str: Box::from(glob_lhs), 
                        pattern: Box::from(glob_rhs)
                    }
                );
            } else if regular_fn_name == "if" {
                // Branch statement

                let (cond_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    2
                )?;

                let conditional_formula = Formula::Conditional { 
                    condition: Box::from(cond_args[0].clone()), 
                    formula_if_true: Box::from(cond_args[1].clone()), 
                    formula_if_false: if cond_args.len() > 3 {
                        return Err(error::Error::FormulaParseError { 
                            msg: String::from("Too many arguments for function if."), 
                            full_formula: full_str.clone(), 
                            substring_with_error: String::from(remaining_str.trim_start()) 
                        });
                    } else if cond_args.len() > 2 {
                        Box::from(cond_args[2].clone())
                    } else {
                        Box::from(Formula::Null)
                    }
                };

                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    conditional_formula
                );
            } else if regular_fn_name == "switch" {
                // Switch statement, return expression associated with first to match value

                let (switch_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    3
                )?;

                let switch_chunks = switch_args[1..(switch_args.len() - 1)].as_chunks::<2>();
                let switch_formula = Formula::Switch { 
                    value: Box::from(switch_args[0].clone()), 
                    matches: switch_chunks.0.iter().map(|tup| (tup[0].clone(), tup[1].clone())).collect(),
                    formula_if_no_match: if switch_chunks.1.len() == 0 {
                        Box::from(Formula::Null)
                    } else {
                        Box::from(switch_chunks.1[0].clone())
                    }
                };

                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    switch_formula
                );
            } else if regular_fn_name == "coalesce" {
                // Return first non-null argument

                let (coalesce_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    2
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Coalesce(coalesce_args)
                );
            } else if regular_fn_name == "nullif" {
                // Return null if the two values match, otherwise return the first value

                let ([nullif_lhs, nullif_rhs], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::NullIf {
                        value: Box::from(nullif_lhs),
                        null_if_match: Box::from(nullif_rhs)
                    }
                );
            } else if regular_fn_name == "sum" {
                // Sum of numbers in collection

                let ([sum_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Sum(Box::from(sum_arg))
                );
            } else if regular_fn_name == "avg" {
                // Average of numbers in collection

                let ([avg_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Average(Box::from(avg_arg))
                );
            } else if regular_fn_name == "min" {
                // If 1 argument is provided, return minimum item in collection
                // If >1 argument is provided, return minimum argument

                let (min_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    1
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    if min_args.len() > 1 {
                        Formula::Argmin(min_args)
                    } else {
                        Formula::Min(Box::from(min_args[0].clone()))
                    }
                );
            } else if regular_fn_name == "max" {
                // If 1 argument is provided, return maximum item in collection
                // If >1 argument is provided, return maximum argument

                let (max_args, after_fn_close) = Self::parse_variable_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex,
                    1
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    if max_args.len() > 1 {
                        Formula::Argmax(max_args)
                    } else {
                        Formula::Max(Box::from(max_args[0].clone()))
                    }
                );
            } else if regular_fn_name == "count" {
                // Count items in collection

                let ([count_arg], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Count(Box::from(count_arg))
                );
            } else if regular_fn_name == "join" {
                // Collection concatenation by delimiter

                let ([join_arg, join_delimiter], after_fn_close) = Self::parse_fixed_args(
                    full_str, 
                    following, 
                    regular_fn_name, 
                    &close_parenthesis_regex
                )?;
                return Self::parse_dependent_expr(
                    full_str, 
                    &after_fn_close, 
                    Formula::Join {
                        collection: Box::from(join_arg), 
                        delimiter: Box::from(join_delimiter)
                    }
                );
            } else {
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unknown function name."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
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

        return Err(error::Error::FormulaParseError { 
            msg: String::from("Unknown formula expression."), 
            full_formula: full_str.clone(), 
            substring_with_error: String::from(remaining_str.trim_start()) 
        }); 
    }

    /// Parse a formula from a string.
    pub fn parse(str: String) -> Result<Self, error::Error> {
        // Parse the formula
        let (parsed_formula, remainder) = Self::parse_expr(&str, &str)?;
        let nonempty_regex: Regex = Regex::new(r#"\S"#).unwrap();
        if nonempty_regex.is_match(&remainder) {
            return Err(error::Error::FormulaParseError { 
                msg: String::from("Unexpected character."), 
                full_formula: str, 
                substring_with_error: String::from(remainder.trim_start())
            }); 
        }

        // Return validated formula
        return Ok(parsed_formula);
    }
    
    /// Converts formula to a basic string indicating the function name.
    pub fn to_string(&self) -> String {
        match self {
            Self::Abs(_) => String::from("abs"),
            Self::Add(_, _) => String::from("operator+"),
            Self::And(_, _) => String::from("and"),
            Self::Argmax(_) => String::from("max"),
            Self::Argmin(_) => String::from("min"),
            Self::Average(_) => String::from("avg"),
            Self::Ceiling(_) => String::from("ceil"),
            Self::Coalesce(_) => String::from("coalesce"),
            Self::Concat(_, _) => String::from("operator&"),
            Self::Conditional { .. } => String::from("if"),
            Self::Count(_) => String::from("count"),
            Self::Divide(_, _) => String::from("operator/"),
            Self::Eq(_, _) => String::from("operator="),
            Self::Exponent(_, _) => String::from("pow"),
            Self::Floor(_) => String::from("floor"),
            Self::Format { .. } => String::from("format"),
            Self::Glob { .. } => String::from("match"),
            Self::In { .. } => String::from("in"),
            Self::Join { .. } => String::from("join"),
            Self::Length(_) => String::from("length"),
            Self::LessThan(_, _) => String::from("operator<"),
            Self::LessThanOrEq(_, _) => String::from("operator<="),
            Self::LiteralArray(items) => String::from("array"),
            Self::LiteralBool(b) => String::from(if *b { "true" } else { "false" }),
            Self::LiteralFloat(lit) => format!("{lit}"),
            Self::LiteralInt(lit) => format!("{lit}"),
            Self::LiteralString(str) => format!("\"{}\"", str.replace("\"", "\\\"")),
            Self::Lowercase(_) => String::from("lower"),
            Self::Max(_) => String::from("max"),
            Self::Min(_) => String::from("min"),
            Self::Modulo(_, _) => String::from("operator%"),
            Self::Multiply(_, _) => String::from("operator*"),
            Self::Not(_) => String::from("not"),
            Self::Null => String::from("null"),
            Self::NullIf { .. } => String::from("nullif"),
            Self::Or(_, _) => String::from("or"),
            Self::Param { .. } => String::from("parameter"),
            Self::RandomInt => String::from("random"),
            Self::Replace { .. } => String::from("replace"),
            Self::Round(_) => String::from("round"),
            Self::Sign(_) => String::from("sign"),
            Self::Slice { .. } => String::from("operator{}"),
            Self::Substring { .. } => String::from("substr"),
            Self::Subtract(_, _) => String::from("operator-"),
            Self::Sum(_) => String::from("sum"),
            Self::Switch { .. } => String::from("switch"),
            Self::Uppercase(_) => String::from("upper"),
            Self::Wrap(inner) => inner.to_string()
        }
    }
}