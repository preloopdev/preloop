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
    /// True when the annotation came from `InfrastructureError` — a runner/
    /// host fault the user cannot fix (upstream `Issue.IsInfrastructureIssue`).
    pub is_infrastructure_issue: bool,
    /// Infrastructure failure category (upstream `Issue.Category`), e.g.
    /// [`super::contexts::infra_failure_categories::DEBUGGER_TUNNEL_FAILURE`].
    /// Only meaningful when `is_infrastructure_issue` is set.
    pub category: Option<String>,
}

impl Annotation {
    /// An infrastructure-level error annotation, carrying the category the
    /// job's `completejob` reports as `infrastructureFailureCategory`.
    /// Mirrors `ExecutionContext.InfrastructureError(message, category)`.
    pub fn infrastructure_error(message: impl Into<String>, category: &str) -> Self {
        Self {
            level: AnnotationLevel::Error,
            message: message.into(),
            title: None,
            file: None,
            line: None,
            end_line: None,
            col: None,
            end_column: None,
            is_infrastructure_issue: true,
            category: Some(category.to_owned()),
        }
    }

    /// A plain job-level error annotation (upstream `context.Error`).
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            level: AnnotationLevel::Error,
            message: message.into(),
            title: None,
            file: None,
            line: None,
            end_line: None,
            col: None,
            end_column: None,
            is_infrastructure_issue: false,
            category: None,
        }
    }
}

/// Annotation severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationLevel {
    Notice,
    Warning,
    Error,
}
