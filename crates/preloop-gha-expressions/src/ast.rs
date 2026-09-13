use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
    Literal(Value),
    Path(Vec<String>),
    UnaryNot(Box<Expr>),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Call {
        name: String,
        args: Vec<Expr>,
    },
    /// Member access on an expression result, e.g. `fromJSON('...').*.name`
    MemberAccess {
        expr: Box<Expr>,
        path: Vec<String>,
    },
    /// Dynamic index with an evaluated key, e.g. `env[matrix.target.options]`.
    /// GitHub evaluates the bracket content as a full expression and uses
    /// the result as the property name.
    Index {
        base: Box<Expr>,
        key: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Or,
    And,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}
