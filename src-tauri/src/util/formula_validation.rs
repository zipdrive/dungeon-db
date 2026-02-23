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
            Self::Any => String::from("scalar"),
            Self::Bool => String::from("boolean"),
            Self::Int => String::from("integer"),
            Self::Float => String::from("number"),
            Self::Text => String::from("text"),
            Self::Date => String::from("date"),
            Self::Null => String::from("null")
        }
    }

    /// Returns true if the given type could be assigned to a value of this type.
    fn encompasses(&self, other: Self) -> bool {
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
enum FormulaReturnTypeSize {
    Scalar,
    Fixed(usize),
    ParamBased(i64)
}

impl FormulaReturnTypeSize {
    fn cross(&self, other: &Self) -> Option<Self> {
        match self {
            Self::Scalar => Some(other.clone()),
            Self::Fixed(_) => {
                match other {
                    Self::Scalar => Some(self.clone()),
                    _ => None
                }
            }
            Self::ParamBased(self_param_oid) => {
                match other {
                    Self::Scalar => Some(self.clone()),
                    Self::ParamBased(other_param_oid) => {
                        if self_param_oid == other_param_oid {
                            Some(self.clone())
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
struct FormulaReturnType {
    scalar_type: ScalarType,
    size: FormulaReturnTypeSize
}

impl FormulaReturnType {
    /// Creates a new formula return type from a scalar type and a size. 
    /// Defaults to scalar size.
    fn new(scalar_type: ScalarType, size: Option<FormulaReturnTypeSize>) -> Self {
        Self {
            scalar_type,
            size: size.unwrap_or(FormulaReturnTypeSize::Scalar)
        }
    }

    /// Creates a new formula return type from a scalar type and various input types with sizes.
    /// Incompatible sizes cause an error.
    fn cross(scalar_type: ScalarType, components: &[Self], name: &str) -> Result<Self, error::Error> {
        if components.len() == 0 {
            return Ok(Self {
                scalar_type,
                size: FormulaReturnTypeSize::Scalar
            });
        }

        let mut size = components[0].size.clone();
        for k in 1..components.len() {
            match size.cross(&components[k].size) {
                Some(s) => { size = s; }
                None => {
                    return Err(error::Error::FormulaTypeCardinalityError { 
                        name: String::from(name), 
                        types: components.iter().map(|c| c.to_string()).collect()
                    });
                }
            }
        }
        return Ok(Self {
            scalar_type,
            size 
        });
    }
    
    /// Converts formula return type to string.
    fn to_string(&self) -> String {
        match self.size {
            FormulaReturnTypeSize::Scalar => self.scalar_type.to_string(),
            FormulaReturnTypeSize::Fixed(size) => format!("{}[{size}]", self.scalar_type.to_string()),
            FormulaReturnTypeSize::ParamBased(_) => format!("query<{}>", self.scalar_type.to_string())
        }
    }
}


impl Formula {

    fn validate_return_type(&self, outer_name: &'static str, return_scalar_type: ScalarType) -> Result<(Self, FormulaReturnType), error::Error> {
        todo!("Formula validation has not been fully implemented!");
        match &self {
            Self::Null => {
                return Ok((self.clone(), FormulaReturnType::new(ScalarType::Null, None)));
            }
            Self::LiteralBool(_) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    return Ok((self.clone(), FormulaReturnType::new(ScalarType::Bool, None)));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::LiteralInt(_) => {
                if return_scalar_type.encompasses(ScalarType::Int) {
                    return Ok((self.clone(), FormulaReturnType::new(ScalarType::Int, None)));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Int.to_string()
                    });
                }
            }
            Self::LiteralFloat(_) => {
                if return_scalar_type.encompasses(ScalarType::Float) {
                    return Ok((self.clone(), FormulaReturnType::new(ScalarType::Float, None)));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::LiteralString(_) => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    return Ok((self.clone(), FormulaReturnType::new(ScalarType::Text, None)));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::LiteralArray(arglist) => {
                let mut new_arglist: Vec<Formula> = Vec::new(); 
                let mut most_restricted_return_scalar_type = ScalarType::Null;
                let mut return_types: Vec<FormulaReturnType> = Vec::new();

                for arg in arglist.iter() {
                    let (arg, return_type_arg) = arg.validate_return_type("[.., exprN, ..]", return_scalar_type.clone())?;
                    most_restricted_return_scalar_type = most_restricted_return_scalar_type.relax(return_type_arg.scalar_type.clone());
                    return_types.push(return_type_arg);
                    new_arglist.push(arg);
                }

                return Ok((
                    Formula::LiteralArray(new_arglist),
                    FormulaReturnType::cross(
                        most_restricted_return_scalar_type, 
                        &return_types[..], 
                        "[..]"
                    )?
                ));
            }
            Self::Random => {
                if return_scalar_type.encompasses(ScalarType::Float) {
                    return Ok((self.clone(), FormulaReturnType::new(ScalarType::Float, None)));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Wrap(arg0) => {
                let (arg0, arg0_return) = arg0.validate_return_type(outer_name, return_scalar_type)?;
                return Ok((Self::Wrap(Box::from(arg0)), arg0_return));
            }
            Self::Conditional { condition, formula_if_true, formula_if_false } => {
                let (condition, return_type_condition) = condition.validate_return_type("if(condition, _, _)", ScalarType::Bool)?;
                let (formula_if_true, return_type_if_true) = formula_if_true.validate_return_type("if(_, value_if_true, _)", return_scalar_type.clone())?;
                let (formula_if_false, return_type_if_false) = formula_if_false.validate_return_type("if(_, _, value_if_false)", return_scalar_type.clone())?;

                let return_type = FormulaReturnType::cross(
                    return_type_if_true.scalar_type.relax(return_type_if_false.scalar_type.clone()),
                    &[return_type_condition, return_type_if_true, return_type_if_false],
                    "if"
                )?;
                return Ok((
                    Self::Conditional { 
                        condition: Box::from(condition), 
                        formula_if_true: Box::from(formula_if_true),
                        formula_if_false: Box::from(formula_if_false)
                    }, 
                    return_type
                ));
            }
            Self::Switch { value, matches, formula_if_no_match } => {
                let mut return_types: Vec<FormulaReturnType> = Vec::new();

                let (value, value_return_type) = value.validate_return_type("switch(value, ..)", ScalarType::Any)?;
                return_types.push(value_return_type);

                // Parse each group of matched value -> returned expression
                let mut new_matches: Vec<(Formula, Formula)> = Vec::new();
                let mut most_restricted_return_scalar_type = ScalarType::Null;
                for (old_value_match, old_expr) in matches.into_iter() {
                    let (new_value_match, new_value_match_return_type) = old_value_match.validate_return_type("switch(.., matchN, ..)", ScalarType::Any)?;
                    return_types.push(new_value_match_return_type);

                    let (new_expr, new_expr_return_type) = old_expr.validate_return_type("switch(.., exprN, ..)", return_scalar_type.clone())?;
                    new_matches.push((
                        new_value_match,
                        new_expr
                    ));
                    most_restricted_return_scalar_type = most_restricted_return_scalar_type.relax(new_expr_return_type.scalar_type.clone());
                    return_types.push(new_expr_return_type);
                }
                
                let (formula_if_no_match, return_type_if_no_match) = formula_if_no_match.validate_return_type("switch(.., expr_default)", return_scalar_type.clone())?;
                most_restricted_return_scalar_type = most_restricted_return_scalar_type.relax(return_type_if_no_match.scalar_type.clone());
                return_types.push(return_type_if_no_match);
                
                return Ok((
                    Formula::Switch { 
                        value: Box::from(value), 
                        matches: new_matches, 
                        formula_if_no_match: Box::from(formula_if_no_match) 
                    },
                    FormulaReturnType::cross(
                        most_restricted_return_scalar_type, 
                        &return_types[..], 
                        "switch"
                    )?
                ));
            }
            Self::Coalesce(arglist) => {
                let mut new_arglist: Vec<Formula> = Vec::new(); 
                let mut most_restricted_return_scalar_type = ScalarType::Null;
                let mut return_types: Vec<FormulaReturnType> = Vec::new();

                for arg in arglist.iter() {
                    let (arg, return_type_arg) = arg.validate_return_type("coalesce(.., exprN, ..)", return_scalar_type.clone())?;
                    most_restricted_return_scalar_type = most_restricted_return_scalar_type.relax(return_type_arg.scalar_type.clone());
                    return_types.push(return_type_arg);
                    new_arglist.push(arg);
                }

                return Ok((
                    Formula::Coalesce(new_arglist),
                    FormulaReturnType::cross(
                        most_restricted_return_scalar_type, 
                        &return_types[..], 
                        "coalesce"
                    )?
                ));
            }
            Self::And(arg0, arg1) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("and(lhs, _)", ScalarType::Bool)?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("and(_, rhs)", ScalarType::Bool)?;
                    return Ok((
                        Self::And(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_arg0, return_type_arg1],
                            "and"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::Or(arg0, arg1) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("or(lhs, _)", ScalarType::Bool)?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("or(_, rhs)", ScalarType::Bool)?;
                    return Ok((
                        Self::Or(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_arg0, return_type_arg1],
                            "or"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::Not(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("not", ScalarType::Bool)?;
                    return Ok((
                        Self::Not(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_arg0],
                            "not"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::Eq(arg0, arg1) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator=(lhs, _)", ScalarType::Any)?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator=(_, rhs)", ScalarType::Any)?;
                    return Ok((
                        Self::And(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_arg0, return_type_arg1],
                            "operator="
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::LessThanOrEq(arg0, arg1) => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator<=(lhs, _)", ScalarType::Any)?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator<=(_, rhs)", ScalarType::Any)?;

                    todo!("Testing that arguments to operator<= are of comparable type has not been implemented!");

                    return Ok((
                        Self::LessThanOrEq(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_arg0, return_type_arg1],
                            "operator<="
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::Glob { str, pattern } => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (str, return_type_str) = str.validate_return_type("match(str, _)", ScalarType::Text)?;
                    let (pattern, return_type_pattern) = pattern.validate_return_type("match(_, pattern)", ScalarType::Text)?;
                    return Ok((
                        Self::Glob {
                            str: Box::from(str),
                            pattern: Box::from(pattern)
                        }, 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_str, return_type_pattern],
                            "match"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::In { value, collection } => {
                if return_scalar_type.encompasses(ScalarType::Bool) {
                    let (value, return_type_value) = value.validate_return_type("in(value, _)", ScalarType::Any)?;
                    let (collection, return_type_collection) = collection.validate_return_type("in(_, collection)", ScalarType::Any)?;

                    todo!("Testing that right-hand side of IN operator is not a scalar has not yet been implemented!");

                    return Ok((
                        Self::In {
                            value: Box::from(value),
                            collection: Box::from(collection)
                        }, 
                        FormulaReturnType::cross(
                            ScalarType::Bool,
                            &[return_type_value],
                            "contains"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Bool.to_string()
                    });
                }
            }
            Self::Add(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator+(lhs, _)", return_scalar_type.clone())?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator+(_, rhs)", return_scalar_type.clone())?;
                    return Ok((
                        Self::Add(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0, return_type_arg1],
                            "operator+"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Subtract(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator-(lhs, _)", return_scalar_type.clone())?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator-(_, rhs)", return_scalar_type.clone())?;
                    return Ok((
                        Self::Subtract(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0, return_type_arg1],
                            "operator-"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Multiply(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (lhs, return_type_lhs) = arg0.validate_return_type("operator*(lhs, _)", return_scalar_type.clone())?;
                    let (rhs, return_type_rhs) = arg1.validate_return_type("operator*(_, rhs)", return_scalar_type.clone())?;
                    let return_type: FormulaReturnType = FormulaReturnType::cross(
                        return_scalar_type,
                        &[return_type_lhs, return_type_rhs],
                        "operator*"
                    )?;

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
                        expected_type: return_scalar_type.to_string(), 
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Divide(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (lhs, return_type_lhs) = arg0.validate_return_type("operator/(lhs, _)", return_scalar_type.clone())?;
                    let (rhs, return_type_rhs) = arg1.validate_return_type("operator/(_, rhs)", return_scalar_type.clone())?;
                    let return_type: FormulaReturnType = FormulaReturnType::cross(
                        return_scalar_type,
                        &[return_type_lhs, return_type_rhs],
                        "operator/"
                    )?;

                    // Preserve order of operations by swapping root with children, if necessary
                    match lhs {
                        Self::Add(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Add(lhs_arg0, Box::from(Self::Divide(lhs_arg1, Box::from(rhs)))),
                                        return_type
                                    ));
                                }
                            }
                        }
                        Self::Subtract(lhs_arg0, lhs_arg1) => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, rhs_arg0)))), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Subtract(lhs_arg0, Box::from(Self::Divide(lhs_arg1, Box::from(rhs)))),
                                        return_type
                                    ));
                                }
                            }
                        }
                        _ => {
                            match rhs {
                                Self::Add(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Add(Box::from(Self::Divide(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        return_type
                                    ));
                                }
                                Self::Subtract(rhs_arg0, rhs_arg1) => {
                                    return Ok((
                                        Self::Subtract(Box::from(Self::Divide(Box::from(lhs), rhs_arg0)), rhs_arg1),
                                        return_type
                                    ));
                                }
                                _ => {
                                    return Ok((
                                        Self::Divide(Box::from(lhs), Box::from(rhs)),
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
                        expected_type: return_scalar_type.to_string(), 
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Modulo(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator%(lhs, _)", return_scalar_type.clone())?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator%(_, rhs)", return_scalar_type.clone())?;
                    return Ok((
                        Self::Modulo(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0, return_type_arg1],
                            "operator%"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Exponent(arg0, arg1) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("pow(base, _)", return_scalar_type.clone())?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("pow(_, exponent)", return_scalar_type.clone())?;
                    return Ok((
                        Self::Exponent(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0, return_type_arg1],
                            "pow"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Abs(arg0) => {
                if let Some(return_scalar_type) = return_scalar_type.restrict(ScalarType::Float) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("abs", return_scalar_type.clone())?;
                    return Ok((
                        Self::Abs(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0],
                            "abs"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Float.to_string()
                    });
                }
            }
            Self::Sign(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Int) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("sign", ScalarType::Float)?;
                    return Ok((
                        Self::Sign(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0],
                            "sign"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Int.to_string()
                    });
                }
            }
            Self::Floor(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Int) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("floor", ScalarType::Float)?;
                    return Ok((
                        Self::Floor(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0],
                            "floor"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Int.to_string()
                    });
                }
            }
            Self::Ceiling(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Int) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("ceil", ScalarType::Float)?;
                    return Ok((
                        Self::Ceiling(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0],
                            "ceil"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Int.to_string()
                    });
                }
            }
            Self::Length(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Int) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("length", ScalarType::Text)?;
                    return Ok((
                        Self::Abs(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            return_scalar_type,
                            &[return_type_arg0],
                            "length"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Int.to_string()
                    });
                }
            }
            Self::Concat(arg0, arg1) => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("operator&(lhs, _)", ScalarType::Text)?;
                    let (arg1, return_type_arg1) = arg1.validate_return_type("operator&(_, rhs)", ScalarType::Text)?;
                    return Ok((
                        Self::Concat(Box::from(arg0), Box::from(arg1)), 
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &[return_type_arg0, return_type_arg1],
                            "operator&"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Lowercase(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("lower", ScalarType::Text)?;
                    return Ok((
                        Self::Abs(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &[return_type_arg0],
                            "lower"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Uppercase(arg0) => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (arg0, return_type_arg0) = arg0.validate_return_type("upper", ScalarType::Text)?;
                    return Ok((
                        Self::Abs(Box::from(arg0)), 
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &[return_type_arg0],
                            "upper"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Substring { str, start, length } => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (str, return_type_str) = str.validate_return_type("substr(str, _, _)", ScalarType::Text)?;
                    let (start, return_type_start) = start.validate_return_type("substr(_, start, _)", ScalarType::Int)?;
                    match length {
                        Some(l) => {
                            let (length, return_type_length) = l.validate_return_type("substr(_, _, length)", ScalarType::Int)?;
                            return Ok((
                                Self::Substring {
                                    str: Box::from(str),
                                    start: Box::from(start),
                                    length: Some(Box::from(length))
                                }, 
                                FormulaReturnType::cross(
                                    ScalarType::Text,
                                    &[return_type_str, return_type_start, return_type_length],
                                    "substr"
                                )?
                            ));
                        }
                        None => {
                            return Ok((
                                Self::Substring { 
                                    str: Box::from(str), 
                                    start: Box::from(start), 
                                    length: None
                                },
                                FormulaReturnType::cross(
                                    ScalarType::Text,
                                    &[return_type_str, return_type_start],
                                    "substr"
                                )?
                            ));
                        }
                    }
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Replace { original, pattern, replacement } => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (original, return_type_original) = original.validate_return_type("replace(original, _, _)", ScalarType::Text)?;
                    let (pattern, return_type_pattern) = pattern.validate_return_type("replace(_, pattern, _)", ScalarType::Text)?;
                    let (replacement, return_type_replacement) = replacement.validate_return_type("replace(_, _, replacement)", ScalarType::Text)?;
                    return Ok((
                        Self::Replace {
                            original: Box::from(original),
                            pattern: Box::from(pattern),
                            replacement: Box::from(replacement)
                        }, 
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &[return_type_original, return_type_pattern, return_type_replacement],
                            "replace"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Format { format, format_params } => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let mut return_types: Vec<FormulaReturnType> = Vec::new();

                    let (format, return_type_formula) = format.validate_return_type("format(fmt, ..)", ScalarType::Text)?;
                    return_types.push(return_type_formula);

                    let mut new_format_params: Vec<Formula> = Vec::new();
                    for old_format_param in format_params.into_iter() {
                        let (new_format_param, return_type_format_param) = old_format_param.validate_return_type("format(.., argN, ..)", ScalarType::Any)?;
                        new_format_params.push(new_format_param);
                        return_types.push(return_type_format_param);
                    }

                    return Ok((
                        Self::Format { 
                            format: Box::from(format), 
                            format_params: new_format_params 
                        },
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &return_types[..],
                            "format"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
            Self::Join { collection, delimiter } => {
                if return_scalar_type.encompasses(ScalarType::Text) {
                    let (delimiter, return_type_delimiter) = delimiter.validate_return_type("join(delimiter, _)", ScalarType::Text)?;
                    let (collection, return_type_collection) = collection.validate_return_type("join(_, collection)", ScalarType::Text)?;

                    todo!("Validating that second argument to join function is a collection has not been implemented!");

                    return Ok((
                        Self::Join {
                            delimiter: Box::from(delimiter),
                            collection: Box::from(collection)
                        }, 
                        FormulaReturnType::cross(
                            ScalarType::Text,
                            &[return_type_delimiter],
                            "join"
                        )?
                    ));
                } else {
                    return Err(error::Error::FormulaTypeValidationError { 
                        outer_name, 
                        inner_name: self.to_string(), 
                        expected_type: return_scalar_type.to_string(),
                        received_type: ScalarType::Text.to_string()
                    });
                }
            }
        }
    }
}