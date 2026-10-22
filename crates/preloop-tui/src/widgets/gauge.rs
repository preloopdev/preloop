//! Horizontal bars drawn with block characters.

/// Filled cells of a `cells`-wide bar for `ratio` (clamped to 0..=1).
pub fn bar(ratio: f64, cells: usize) -> String {
    let ratio = if ratio.is_finite() {
        ratio.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let filled = (ratio * cells as f64).round() as usize;
    let filled = filled.min(cells);
    format!("{}{}", "█".repeat(filled), "░".repeat(cells - filled))
}

/// `numerator / denominator` clamped to 0..=1; 0 when the denominator is 0.
pub fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator <= 0.0 {
        0.0
    } else {
        (numerator / denominator).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bar_rounds_to_nearest_cell() {
        assert_eq!(bar(0.64, 10), "██████░░░░");
        assert_eq!(bar(0.0, 4), "░░░░");
        assert_eq!(bar(2.0, 4), "████");
        assert_eq!(bar(f64::NAN, 3), "░░░");
    }

    #[test]
    fn ratio_guards_zero_denominator() {
        assert_eq!(ratio(3.0, 0.0), 0.0);
        assert_eq!(ratio(41.0, 64.0), 41.0 / 64.0);
    }
}
