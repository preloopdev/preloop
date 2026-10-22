//! Explicit output routing for reused command flows.
//!
//! `preloop init --json` runs the `preloop setup github` flow between its
//! one-object-per-line JSON steps; the flow's human output is collected and
//! attached to the step record instead of printed. Every other caller prints
//! straight to stdout. The sink is passed explicitly — output routing is not
//! process state, and nothing here touches file descriptors.

pub(crate) enum Out {
    /// Straight to stdout.
    Stdout,
    /// Collected for the caller (the `log` field of an `init --json` step).
    Captured(Vec<u8>),
}

impl Out {
    pub(crate) fn stdout() -> Self {
        Self::Stdout
    }

    pub(crate) fn capturing() -> Self {
        Self::Captured(Vec::new())
    }

    /// The collected text — empty when output went straight to stdout.
    pub(crate) fn into_captured(self) -> String {
        match self {
            Self::Stdout => String::new(),
            Self::Captured(buffer) => String::from_utf8_lossy(&buffer).into_owned(),
        }
    }
}

impl std::io::Write for Out {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Stdout => std::io::stdout().write(buf),
            Self::Captured(buffer) => buffer.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Stdout => std::io::stdout().flush(),
            Self::Captured(buffer) => buffer.flush(),
        }
    }
}
