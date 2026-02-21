use regex::Regex;
use time::error::Format;
use crate::util::error;

#[derive(Clone, PartialEq)]
enum ScalarType {
    Any,
    Bool,
    Int,
    Float,
    Text,
    Date,
    Null
}

impl ScalarType {
    /// Converts scalar type to string.
    fn to_string(&self) -> String {
        match self {
            Self::Any => String::from("any"),
            Self::Bool => String::from("boolean"),
            Self::Int => String::from("integer"),
            Self::Float => String::from("number"),
            Self::Text => String::from("text"),
            Self::Date => String::from("date"),
            Self::Null => String::from("null")
        }
    }

    /// Returns true if the given type could be assigned to a value of this type.
    fn encompasses(&self, other: &Self) -> bool {
        match self {
            Self::Any => { return true; }
            Self::Bool => {
                return match other {
                    Self::Bool | Self::Null => true,
                    _ => false
                };
            }
            Self::Int => {
                return match other {
                    Self::Int | Self::Null => true,
                    _ => false
                };
            }
            Self::Float => {
                return match other {
                    Self::Float | Self::Int | Self::Null => true,
                    _ => false
                };
            }
            Self::Text => {
                return match other {
                    Self::Text | Self::Null => true,
                    _ => false
                }
            }
            Self::Date => {
                return match other {
                    Self::Date | Self::Null => true,
                    _ => false
                };
            }
            Self::Null => {
                return match other { 
                    Self::Null => true,
                    _ => false 
                };
            }
        }
    }

    /// Returns the most restrictive of self and other.
    fn restrict(&self, other: Self) -> Option<Self> {
        match self {
            Self::Any => { return Some(other); }
            Self::Bool => {
                return match other {
                    Self::Bool | Self::Null => Some(other),
                    Self::Any => Some(Self::Bool),
                    _ => None
                };
            }
            Self::Int => {
                return match other {
                    Self::Int | Self::Null => Some(other),
                    Self::Any | Self::Float => Some(Self::Int),
                    _ => None
                };
            }
            Self::Float => {
                return match other {
                    Self::Float | Self::Int | Self::Null => Some(other),
                    Self::Any => Some(Self::Float),
                    _ => None
                };
            }
            Self::Text => {
                return match other {
                    Self::Text | Self::Null => Some(other),
                    Self::Any => Some(Self::Text),
                    _ => None
                }
            }
            Self::Date => {
                return match other {
                    Self::Date | Self::Null => Some(other),
                    Self::Any => Some(Self::Date),
                    _ => None
                };
            }
            Self::Null => {
                return Some(other);
            }
        }
    }

    /// Returns the least restrictive of self and other.
    fn relax(&self, other: Self) -> Self {
        match self {
            Self::Any => { Self::Any }
            Self::Bool => {
                match other {
                    Self::Bool | Self::Null => Self::Bool,
                    _ => Self::Any
                }
            }
            Self::Int => {
                match other {
                    Self::Int | Self::Null => Self::Int,
                    Self::Float => Self::Float,
                    _ => Self::Any
                }
            }
            Self::Float => {
                match other {
                    Self::Float | Self::Int | Self::Null => Self::Float,
                    _ => Self::Any
                }
            }
            Self::Text => {
                match other {
                    Self::Text | Self::Null => Self::Text,
                    _ => Self::Any
                }
            }
            Self::Date => {
                match other {
                    Self::Date | Self::Null => Self::Date,
                    _ => Self::Any
                }
            }
            Self::Null => {
                other
            }
        }
    }
}

#[derive(Clone, PartialEq)]
enum CollectionSize {
    Any,
    Fixed(usize),
    ParamBased(i64)
}

impl CollectionSize {
    /// Return the most restrictive size of self and other.
    fn restrict(&self, other: Self) -> Option<Self> {
        match self {
            Self::Any => Some(other),
            Self::Fixed(self_size) => {
                match other {
                    Self::Any => Some(Self::Fixed(self_size.clone())),
                    Self::Fixed(other_size) => {
                        if *self_size == other_size {
                            Some(Self::Fixed(self_size.clone()))
                        } else {
                            None
                        }
                    }
                    _ => None
                }
            }
            Self::ParamBased(self_param_oid) => {
                match other {
                    Self::Any => Some(Self::ParamBased(self_param_oid.clone())),
                    Self::ParamBased(other_param_oid) => {
                        if *self_param_oid == other_param_oid {
                            Some(Self::ParamBased(self_param_oid.clone()))
                        } else {
                            None
                        }
                    }
                    _ => None
                }
            }
        }
    }
}


#[derive(Clone)]
enum FormulaReturnType {
    Scalar(ScalarType),
    Collection(ScalarType, CollectionSize)
}

impl FormulaReturnType {
    /// Converts formula return type to string.
    fn to_string(&self) -> String {
        match self {
            Self::Scalar(self_scalar) => self_scalar.to_string(),
            Self::Collection(self_scalar, _) => format!("array<{}>", self_scalar.to_string())
        }
    }

    /// Gets the scalar type of this return type.
    fn get_scalar_type(&self) -> ScalarType {
        match self {
            Self::Scalar(scalar_type)
            | Self::Collection(scalar_type, _) => scalar_type.clone()
        }
    }

    /// Changes the scalar type of this return type.
    fn change_scalar_type(&self, scalar_type: ScalarType) -> Self {
        match self {
            Self::Scalar(_) => {
                Self::Scalar(scalar_type)
            }
            Self::Collection(_, self_size) => {
                Self::Collection(scalar_type, self_size.clone())
            }
        }
    }

    /// Returns true if the given type could be assigned to a value of this type.
    fn encompasses(&self, other: Self) -> Option<Self> {
        match self {
            Self::Scalar(self_scalar) => {
                if let Self::Scalar(other_scalar) = &other {
                    if self_scalar.encompasses(&other_scalar) {
                        return Some(other);
                    }
                }
                return None;
            }
            Self::Collection(self_scalar, self_size) => {
                match &other {
                    Self::Collection(other_scalar, other_size) => {
                        if self_scalar.encompasses(other_scalar) && *self_size == *other_size {
                            return Some(other);
                        }
                        return None;
                    }
                    Self::Scalar(other_scalar) => {
                        if self_scalar.encompasses(other_scalar) {
                            return Some(Self::Collection(other_scalar.clone(), self_size.clone()));
                        }
                        return None;
                    }
                }
            }
        }
    }

    /// Returns the narrower of this type and another, or None if the types have no intersection.
    fn restrict(&self, other: Self) -> Option<Self> {
        match self {
            Self::Scalar(self_scalar) => {
                if let Self::Scalar(other_scalar) = other {
                    if let Some(restrict_scalar) = self_scalar.restrict(other_scalar) {
                        return Some(Self::Scalar(restrict_scalar));
                    }
                }
            }
            Self::Collection(self_scalar, self_size) => {
                match other {
                    Self::Collection(other_scalar, other_size) => {
                        if let Some(restrict_scalar) = self_scalar.restrict(other_scalar) {
                            if let Some(restrict_size) = self_size.restrict(other_size) {
                                return Some(Self::Collection(restrict_scalar, restrict_size));
                            }
                        }
                    }
                    Self::Scalar(other_scalar) => {
                        if let Some(restrict_scalar) = self_scalar.restrict(other_scalar) {
                            return Some(Self::Collection(restrict_scalar, self_size.clone()));
                        }
                    }
                }
            }
        }
        return None;
    }

    /// Returns the narrower of this type and another, or None if the types have no intersection.
    fn relax(&self, other: Self) -> Option<Self> {
        match self {
            Self::Scalar(self_scalar) => {
                if let Self::Scalar(other_scalar) = other {
                    return Some(Self::Scalar(self_scalar.relax(other_scalar)));
                }
            }
            Self::Collection(self_scalar, self_size) => {
                if let Self::Collection(other_scalar, other_size) = other {
                    if *self_size == other_size {
                        return Some(Self::Collection(self_scalar.relax(other_scalar), self_size.clone()));
                    }
                }
            }
        }
        return None;
    }
}


#[derive(Clone)]
pub enum Formula {
    Param(i64),
    Null,
    LiteralBool(bool),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralString(String),
    LiteralArray(Vec<Formula>),
    Random,
    
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

impl Formula {
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

        // Check for AND operator
        let and_regex: Regex = Regex::new(r#"(?is)^\s*and\b(.*)"#).unwrap();
        if let Some(and_cap) = and_regex.captures(remaining_str) {
            let (_, [following]) = and_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::And(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for OR operator
        let or_regex: Regex = Regex::new(r#"(?is)^\s*or\b(.*)"#).unwrap();
        if let Some(or_cap) = or_regex.captures(remaining_str) {
            let (_, [following]) = or_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Or(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for equals operator
        let equals_regex: Regex = Regex::new(r#"(?s)^\s*=(.*)"#).unwrap();
        if let Some(equals_cap) = equals_regex.captures(remaining_str) {
            let (_, [following]) = equals_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Eq(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for not equals operator
        let neq_regex: Regex = Regex::new(r#"(?s)^\s*<>(.*)"#).unwrap();
        if let Some(neq_cap) = neq_regex.captures(remaining_str) {
            let (_, [following]) = neq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Not(Box::from(Formula::Eq(Box::from(lhs), Box::from(rhs)))), following_rhs));
        }

        // Check for less-than-or-equals operator
        let leq_regex: Regex = Regex::new(r#"(?s)^\s*<=(.*)"#).unwrap();
        if let Some(leq_cap) = leq_regex.captures(remaining_str) {
            let (_, [following]) = leq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::LessThanOrEq(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for greater-than-or-equals operator
        let geq_regex: Regex = Regex::new(r#"(?s)^\s*>=(.*)"#).unwrap();
        if let Some(geq_cap) = geq_regex.captures(remaining_str) {
            let (_, [following]) = geq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Not(Box::from(Formula::LessThan(Box::from(lhs), Box::from(rhs)))), following_rhs));
        }

        // Check for less-than operator
        let leq_regex: Regex = Regex::new(r#"(?s)^\s*<(.*)"#).unwrap();
        if let Some(leq_cap) = leq_regex.captures(remaining_str) {
            let (_, [following]) = leq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::LessThan(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for greater-than operator
        let geq_regex: Regex = Regex::new(r#"(?s)^\s*>(.*)"#).unwrap();
        if let Some(geq_cap) = geq_regex.captures(remaining_str) {
            let (_, [following]) = geq_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Not(Box::from(Formula::LessThanOrEq(Box::from(lhs), Box::from(rhs)))), following_rhs));
        }

        // Check for IN operator
        let in_regex: Regex = Regex::new(r#"(?is)^\s*in\b(.*)"#).unwrap();
        if let Some(in_cap) = in_regex.captures(remaining_str) {
            let (_, [following]) = in_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((
                Formula::In {
                    value: Box::from(lhs), 
                    collection: Box::from(rhs)
                }, 
                following_rhs
            ));
        }

        // Check for addition operator
        let addition_regex: Regex = Regex::new(r#"(?s)^\s*\+(.*)"#).unwrap();
        if let Some(addition_cap) = addition_regex.captures(remaining_str) {
            let (_, [following]) = addition_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Add(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for subtraction operator
        let subtraction_regex: Regex = Regex::new(r#"(?s)^\s*-(.*)"#).unwrap();
        if let Some(subtraction_cap) = subtraction_regex.captures(remaining_str) {
            let (_, [following]) = subtraction_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Subtract(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for multiplication operator
        let multiplication_regex: Regex = Regex::new(r#"(?s)^\s*\*(.*)"#).unwrap();
        if let Some(multiplication_cap) = multiplication_regex.captures(remaining_str) {
            let (_, [following]) = multiplication_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Multiply(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for division operator
        let division_regex: Regex = Regex::new(r#"(?s)^\s*/(.*)"#).unwrap();
        if let Some(division_cap) = division_regex.captures(remaining_str) {
            let (_, [following]) = division_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Divide(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for modulo operator
        let modulo_regex: Regex = Regex::new(r#"(?s)^\s*%(.*)"#).unwrap();
        if let Some(modulo_cap) = modulo_regex.captures(remaining_str) {
            let (_, [following]) = modulo_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Modulo(Box::from(lhs), Box::from(rhs)), following_rhs));
        }

        // Check for concatenation operator
        let concat_regex: Regex = Regex::new(r#"(?s)^\s*&(.*)"#).unwrap();
        if let Some(concat_cap) = concat_regex.captures(remaining_str) {
            let (_, [following]) = concat_cap.extract();
            let (rhs, following_rhs) = Self::parse_expr(full_str, following)?;
            return Ok((Formula::Concat(Box::from(lhs), Box::from(rhs)), following_rhs));
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
        let param_regex: Regex = Regex::new(r#"(?is)^\s*param(\d+)(.*)"#).unwrap();
        if let Some(param_cap) = param_regex.captures(remaining_str) {
            let (_, [param_content, following]) = param_cap.extract();
            let Ok(param_oid) = param_content.parse::<i64>() else { 
                return Err(error::Error::FormulaParseError { 
                    msg: String::from("Unable to parse formula parameter."), 
                    full_formula: full_str.clone(), 
                    substring_with_error: String::from(remaining_str.trim_start()) 
                }); 
            };
            return Self::parse_dependent_expr(full_str, following, Formula::Param(param_oid));
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
                    Formula::Random
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
            msg: String::from("Unknown expression."), 
            full_formula: full_str.clone(), 
            substring_with_error: String::from(remaining_str.trim_start()) 
        }); 
    }

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

        // Validate the formula
        // TODO

        // Return validated formula
        return Ok(parsed_formula);
    }

    fn validate_return_type(&self, outer_name: &'static str, return_type: &FormulaReturnType) -> Result<(Self, FormulaReturnType), error::Error> {
        match &self {
            Self::Null => {
                return Ok((self.clone(), FormulaReturnType::Scalar(ScalarType::Null)));
            }
            Self::LiteralBool(_) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    return Ok((self.clone(), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::LiteralInt(_) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Int)) {
                    return Ok((self.clone(), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Int).to_string()
                    });
                }
            }
            Self::LiteralFloat(_) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Float)) {
                    return Ok((self.clone(), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::LiteralString(_) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    return Ok((self.clone(), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Random => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Float)) {
                    return Ok((self.clone(), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Wrap(arg0) => {
                let (arg0, arg0_return) = arg0.validate_return_type("operator()", &return_type)?;
                return Ok((Self::Wrap(Box::from(arg0)), arg0_return));
            }
            Self::Conditional { condition, formula_if_true, formula_if_false } => {
                let (condition, _) = condition.validate_return_type("if(condition, _, _)", &FormulaReturnType::Scalar(ScalarType::Bool))?;
                let (formula_if_true, return_type_if_true) = formula_if_true.validate_return_type("if(_, value_if_true, _)", &return_type)?;
                let (formula_if_false, return_type_if_false) = formula_if_false.validate_return_type("if(_, _, value_if_false)", &return_type)?;
                if let Some(return_type) = return_type_if_true.relax(return_type_if_false) {
                    return Ok((
                        Self::Conditional { 
                            condition: Box::from(condition), 
                            formula_if_true: Box::from(formula_if_true),
                            formula_if_false: Box::from(formula_if_false)
                        }, 
                        return_type
                    ));
                } else {
                    return Err(error::Error::FormulaTypeConflictError {
                        name: self.to_string(), 
                        type1: return_type_if_true.to_string(),
                        type2: return_type_if_false.to_string()
                    });
                }
            }
            Self::Switch { value, matches, formula_if_no_match } => {
                let (value, value_return_type) = value.validate_return_type("switch(value, ..)", &FormulaReturnType::Scalar(ScalarType::Any))?;
                let mut new_matches: Vec<(Formula, Formula)> = Vec::new();
                let mut restricted_return_type = match return_type {
                    FormulaReturnType::Scalar(_) => FormulaReturnType::Scalar(ScalarType::Null),
                    FormulaReturnType::Collection(_, collection_size) => FormulaReturnType::Collection(ScalarType::Null, collection_size.clone())
                };
                for (old_value_match, old_expr) in matches.into_iter() {
                    let (new_value_match, _) = old_value_match.validate_return_type("switch(.., matchN, ..)", &value_return_type)?;
                    let (new_expr, new_expr_return_type) = old_expr.validate_return_type("switch(.., exprN, ..)", &return_type)?;
                    new_matches.push((
                        new_value_match,
                        new_expr
                    ));
                    if let Some(relaxed_type) = restricted_return_type.relax(new_expr_return_type) {
                        restricted_return_type = relaxed_type;
                    } else {
                        return Err(error::Error::FormulaTypeConflictError {
                            name: self.to_string(), 
                            type1: restricted_return_type.to_string(),
                            type2: new_expr_return_type.to_string()
                        });
                    }
                }
                
                let (formula_if_no_match, return_type_if_no_match) = formula_if_no_match.validate_return_type("switch(.., expr_default)", &return_type)?;
                if let Some(relaxed_type) = restricted_return_type.relax(return_type_if_no_match) {
                    restricted_return_type = relaxed_type;
                } else {
                    return Err(error::Error::FormulaTypeConflictError {
                        name: self.to_string(), 
                        type1: restricted_return_type.to_string(),
                        type2: return_type_if_no_match.to_string()
                    });
                }

                return Ok((
                    Formula::Switch { 
                        value: Box::from(value), 
                        matches: new_matches, 
                        formula_if_no_match: Box::from(formula_if_no_match) 
                    },
                    restricted_return_type
                ));
            }
            Self::And(arg0, arg1) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    let (arg0, _) = arg0.validate_return_type("and(lhs, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("and(_, rhs)", &return_type)?;
                    return Ok((Self::And(Box::from(arg0), Box::from(arg1)), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::Or(arg0, arg1) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    let (arg0, _) = arg0.validate_return_type("or(lhs, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("or(_, rhs)", &return_type)?;
                    return Ok((Self::Or(Box::from(arg0), Box::from(arg1)), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::Not(arg0) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    let (arg0, _) = arg0.validate_return_type("not", &return_type)?;
                    return Ok((Self::Not(Box::from(arg0)), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::Eq(arg0, arg1) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    let arg_return_type = return_type.change_scalar_type(ScalarType::Any);
                    let (arg0, _) = arg0.validate_return_type("operator=(lhs, _)", &arg_return_type)?;
                    let (arg1, _) = arg1.validate_return_type("operator=(_, rhs)", &arg_return_type)?;
                    return Ok((Self::Eq(Box::from(arg0), Box::from(arg1)), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::LessThanOrEq(arg0, arg1) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Bool)) {
                    let arg_return_type = return_type.change_scalar_type(ScalarType::Any);
                    let (arg0, arg0_return_type) = arg0.validate_return_type("operator<=(lhs, _)", &arg_return_type)?;
                    let (arg1, arg1_return_type) = arg1.validate_return_type("operator<=(_, rhs)", &arg_return_type)?;

                    // Verify that the return types can be compared
                    let Some(shared_return_type) = arg0_return_type.relax(arg1_return_type) else {
                        return Err(error::Error::FormulaTypeConflictError {
                            name: self.to_string(), 
                            type1: arg0_return_type.to_string(),
                            type2: arg1_return_type.to_string()
                        });
                    };
                    if shared_return_type.get_scalar_type() == ScalarType::Any {
                        return Err(error::Error::FormulaTypeConflictError {
                            name: self.to_string(), 
                            type1: arg0_return_type.to_string(),
                            type2: arg1_return_type.to_string()
                        });
                    }

                    return Ok((Self::LessThanOrEq(Box::from(arg0), Box::from(arg1)), return_type));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Bool).to_string()
                    });
                }
            }
            Self::Add(arg0, arg1) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (arg0, _) = arg0.validate_return_type("operator+(lhs, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("operator+(_, rhs)", &return_type)?;
                    return Ok((Self::Add(Box::from(arg0), Box::from(arg1)), return_type.clone()));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Subtract(arg0, arg1) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (arg0, _) = arg0.validate_return_type("operator-(lhs, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("operator-(_, rhs)", &return_type)?;
                    return Ok((Self::Subtract(Box::from(arg0), Box::from(arg1)), return_type.clone()));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(), 
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Multiply(arg0, arg1) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (lhs, _) = arg0.validate_return_type("operator*(lhs, _)", &return_type)?;
                    let (rhs, _) = arg1.validate_return_type("operator*(_, rhs)", &return_type)?;
                    // Preserve order of operations by swapping root with children, if necessary
                    match lhs {
                        Self::Add(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Add(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Add(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Add(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, Box::from(rhs)))),
                                        return_type
                                    ));
                                }
                            }
                        }
                        Self::Subtract(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Subtract(lhs_arg0, Box::from(Self::Multiply(lhs_arg1, Box::from(rhs)))),
                                        return_type
                                    ));
                                }
                            }
                        }
                        _ => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Multiply(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Multiply(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Multiply(Box::from(lhs), Box::from(rhs)),
                                        return_type
                                    ));
                                }
                            }
                        }
                    }
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(), 
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Divide(arg0, arg1) => {
                if let Some(return_type) = return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (lhs, _) = arg0.validate_return_type("operator/(lhs, _)", &FormulaReturnType::Scalar(ScalarType::Float))?;
                    let (rhs, _) = arg1.validate_return_type("operator/(_, rhs)", &FormulaReturnType::Scalar(ScalarType::Float))?;
                    // Preserve order of operations by swapping root with children, if necessary
                    match lhs {
                        Self::Add(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, Box::from(rhs)))),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                            }
                        }
                        Self::Subtract(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, Box::from(rhs)))),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                            }
                        }
                        _ => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Divide(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Divide(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Divide(Box::from(lhs), Box::from(rhs)),
                                        FormulaReturnType::Scalar(ScalarType::Float)
                                    ));
                                }
                            }
                        }
                    }
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(), 
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Modulo(arg0, arg1) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (arg0, _) = arg0.validate_return_type("operator%(dividend, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("operator%(_, divisor)", &return_type)?;
                    return Ok((Self::Modulo(Box::from(arg0), Box::from(arg1)), return_type.clone()));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Exponent(arg0, arg1) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (arg0, _) = arg0.validate_return_type("pow(base, _)", &return_type)?;
                    let (arg1, _) = arg1.validate_return_type("pow(_, exponent)", &return_type)?;
                    return Ok((Self::Modulo(Box::from(arg0), Box::from(arg1)), return_type.clone()));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Abs(arg0) => {
                if let Some(return_type) = return_type.restrict(FormulaReturnType::Scalar(ScalarType::Float)) {
                    let (arg0, _) = arg0.validate_return_type("abs", &return_type)?;
                    return Ok((Self::Abs(Box::from(arg0)), return_type.clone()));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Float).to_string()
                    });
                }
            }
            Self::Sign(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Int)) {
                    let (arg0, _) = arg0.validate_return_type("sign", &FormulaReturnType::Scalar(ScalarType::Float))?;
                    return Ok((
                        Self::Sign(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Int)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Int).to_string()
                    });
                }
            }
            Self::Floor(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Int)) {
                    let (arg0, _) = arg0.validate_return_type("floor", &FormulaReturnType::Scalar(ScalarType::Float))?;
                    return Ok((
                        Self::Floor(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Int)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Int).to_string()
                    });
                }
            }
            Self::Ceiling(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Int)) {
                    let (arg0, _) = arg0.validate_return_type("ceil", &FormulaReturnType::Scalar(ScalarType::Float))?;
                    return Ok((
                        Self::Ceiling(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Int)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Int).to_string()
                    });
                }
            }
            Self::Length(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Int)) {
                    let (arg0, _) = arg0.validate_return_type("length", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    return Ok((
                        Self::Length(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Int)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Int).to_string()
                    });
                }
            }
            Self::Concat(arg0, arg1) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (arg0, _) = arg0.validate_return_type("operator+(lhs, _)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    let (arg1, _) = arg1.validate_return_type("operator+(_, rhs)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    return Ok((
                        Self::Concat(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::Scalar(ScalarType::Text)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Lowercase(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (arg0, _) = arg0.validate_return_type("lower", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    return Ok((
                        Self::Lowercase(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Text)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Uppercase(arg0) => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (arg0, _) = arg0.validate_return_type("upper", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    return Ok((
                        Self::Uppercase(Box::from(arg0)), 
                        FormulaReturnType::Scalar(ScalarType::Text)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Substring { str, start, length } => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (str, _) = str.validate_return_type("substr(str, _, _)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    let (start, _) = start.validate_return_type("substr(_, start, _)", &FormulaReturnType::Scalar(ScalarType::Int))?;
                    match length {
                        Some(l) => {
                            let (length, _) = l.validate_return_type("substr(_, _, length)", &FormulaReturnType::Scalar(ScalarType::Int))?;
                            return Ok((
                                Self::Substring {
                                    str: Box::from(str),
                                    start: Box::from(start),
                                    length: Some(Box::from(length))
                                }, 
                                FormulaReturnType::Scalar(ScalarType::Text)
                            ));
                        }
                        None => {
                            return Ok((
                                Self::Substring { 
                                    str: Box::from(str), 
                                    start: Box::from(start), 
                                    length: None
                                },
                                FormulaReturnType::Scalar(ScalarType::Text)
                            ));
                        }
                    }
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Replace { original, pattern, replacement } => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (original, _) = original.validate_return_type("replace(original, _, _)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    let (pattern, _) = pattern.validate_return_type("replace(_, pattern, _)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    let (replacement, _) = replacement.validate_return_type("replace(_, _, replacement)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    return Ok((
                        Self::Replace {
                            original: Box::from(original),
                            pattern: Box::from(pattern),
                            replacement: Box::from(replacement)
                        }, 
                        FormulaReturnType::Scalar(ScalarType::Text)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            Self::Format { format, format_params } => {
                if return_type.encompasses(FormulaReturnType::Scalar(ScalarType::Text)) {
                    let (format, _) = format.validate_return_type("format(fmt, ..)", &FormulaReturnType::Scalar(ScalarType::Text))?;
                    let mut new_format_params: Vec<Formula> = Vec::new();
                    for old_format_param in format_params.into_iter() {
                        let (new_format_param, _) = old_format_param.validate_return_type("format(.., argN, ..)", &FormulaReturnType::Scalar(ScalarType::Any))?;
                        new_format_params.push(new_format_param);
                    }
                    return Ok((
                        Self::Format { 
                            format: Box::from(format), 
                            format_params: new_format_params 
                        },
                        FormulaReturnType::Scalar(ScalarType::Text)
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_type.to_string(),
                        received_type: FormulaReturnType::Scalar(ScalarType::Text).to_string()
                    });
                }
            }
            /*
            Self::Null
            | Self::LiteralInt(_) => {
                return Ok(self);
            }
            Self::Argmin(arglist) => {
                let (arg0, inner_return_type) = arglist[0].clone().validate_return_type("min(arg0, ..)", return_type)?;

                let mut new_arglist: Vec<Formula> = vec![arg0];
                for arg_result in arglist.iter().skip(1).map(|item| item.clone().validate_return_type("min(.., argN, ..)", inner_return_type.clone())) {
                    let (argN, _) = arg_result?;
                    new_arglist.push(argN);
                }
                return Ok((Self::Argmin(new_arglist), inner_return_type));
            }
            Self::Argmax(arglist) => {
                let (arg0, inner_return_type) = arglist[0].clone().validate_return_type("min(arg0, ..)", return_type)?;

                let mut new_arglist: Vec<Formula> = vec![arg0];
                for arg_result in arglist.iter().skip(1).map(|item| item.clone().validate_return_type("min(.., argN, ..)", inner_return_type.clone())) {
                    let (argN, _) = arg_result?;
                    new_arglist.push(argN);
                }
                return Ok((Self::Argmax(new_arglist), inner_return_type));
            }
            Self::Coalesce(arglist) => {
                let (arg0, inner_return_type) = arglist[0].clone().validate_return_type("min(arg0, ..)", return_type)?;

                let mut new_arglist: Vec<Formula> = vec![arg0];
                for arg_result in arglist.iter().skip(1).map(|item| item.clone().validate_return_type("min(.., argN, ..)", inner_return_type.clone())) {
                    let (argN, _) = arg_result?;
                    new_arglist.push(argN);
                }
                return Ok((Self::Coalesce(new_arglist), inner_return_type));
            }
            Self::Round(arg0) => {
                let arg0 = arg0.validate_float("round")?;
                return Ok(Self::Round(Box::from(arg0)));
            }
            Self::Sum(arg0) => {
                let arg0 = arg0.validate_int_collection("sum")?;
                return Ok(Self::Sum(Box::from(arg0)));
            }
            Self::Min(arg0) => {
                let arg0 = arg0.validate_int_collection("min")?;
                return Ok(Self::Min(Box::from(arg0)));
            }
            Self::Max(arg0) => {
                let arg0 = arg0.validate_int_collection("max")?;
                return Ok(Self::Max(Box::from(arg0)));
            }
            Self::Count(arg0) => {
                let arg0 = arg0.validate_collection("count")?;
                return Ok(Self::Count(Box::from(arg0)));
            }
             */
            Self::Average(_) => {
                return Err(error::Error::FormulaTypeValidationError { 
                    outer_name, 
                    inner_name: self.to_string(), 
                    expected_type: "integer".to_string(), 
                    received_type: "float".to_string()
                });
            }
            Self::Eq(_, _)
            | Self::LessThan(_, _)
            | Self::LessThanOrEq(_, _)
            | Self::In { .. }
            | Self::Glob { .. } => {
                return Err(error::Error::FormulaTypeValidationError { 
                    outer_name, 
                    inner_name: self.to_string(), 
                    expected_type: "integer".to_string(), 
                    received_type: "boolean".to_string()
                });
            }
            Self::Join { .. } => {
                return Err(error::Error::FormulaTypeValidationError { 
                    outer_name, 
                    inner_name: self.to_string(), 
                    expected_type: "integer".to_string(), 
                    received_type: "text".to_string()
                });
            }
            _ => {
                return Err(error::Error::FormulaTypeValidationError { 
                    outer_name, 
                    inner_name: self.to_string(), 
                    expected_type: "integer".to_string(), 
                    received_type: "text".to_string()
                });
            }
        }
    }
    
    /// Converts formula to a basic string indicating the function name.
    fn to_string(&self) -> String {
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
            Self::LiteralArray(_) => String::from("[...]"),
            Self::LiteralBool(b) => String::from(if *b { "true" } else { "false" }),
            Self::LiteralFloat(lit) => format!("{lit}"),
            Self::LiteralInt(lit) => format!("{lit}"),
            Self::LiteralString(str) => format!("\"{str}\""),
            Self::Lowercase(_) => String::from("lower"),
            Self::Max(_) => String::from("max"),
            Self::Min(_) => String::from("min"),
            Self::Modulo(_, _) => String::from("operator%"),
            Self::Multiply(_, _) => String::from("operator*"),
            Self::Not(_) => String::from("not"),
            Self::Null => String::from("null"),
            Self::NullIf { .. } => String::from("nullif"),
            Self::Or(_, _) => String::from("or"),
            Self::Param(_) => String::from("parameter"),
            Self::Random => String::from("random"),
            Self::Replace { .. } => String::from("replace"),
            Self::Round(_) => String::from("round"),
            Self::Sign(_) => String::from("sign"),
            Self::Slice { .. } => String::from("operator{}"),
            Self::Substring { .. } => String::from("substr"),
            Self::Subtract(_, _) => String::from("operator-"),
            Self::Sum(_) => String::from("sum"),
            Self::Switch { .. } => String::from("switch"),
            Self::Uppercase(_) => String::from("upper"),
            Self::Wrap(_) => String::from("operator()")
        }
    }
}