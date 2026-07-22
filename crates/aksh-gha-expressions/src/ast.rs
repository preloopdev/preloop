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
    /// Dynamic index or member access using brackets, e.g. `fromJSON('...')[needs.meta.outputs.run-kind]`
    IndexAccess {
        expr: Box<Expr>,
        index: Box<Expr>,
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
