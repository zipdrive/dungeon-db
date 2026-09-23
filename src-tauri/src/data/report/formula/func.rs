pub enum Func {
    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Text(String),

    Not(Box<Func>),
    And(Box<Func>, Box<Func>),
    Or(Box<Func>, Box<Func>),

    Add(Box<Func>, Box<Func>),
    Sub(Box<Func>, Box<Func>),
    Mul(Box<Func>, Box<Func>),
    Div(Box<Func>, Box<Func>),
    Mod(Box<Func>, Box<Func>),
    Pow(Box<Func>, Box<Func>),
}

impl Func {

}