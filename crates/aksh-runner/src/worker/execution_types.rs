/// A workflow annotation (error/warning/notice).
#[derive(Debug, Clone)]
pub struct Annotation {
    pub level: AnnotationLevel,
    pub message: String,
    pub title: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub end_line: Option<u32>,
    pub col: Option<u32>,
    pub end_column: Option<u32>,
}

/// Annotation severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationLevel {
    Notice,
    Warning,
    Error,
}
