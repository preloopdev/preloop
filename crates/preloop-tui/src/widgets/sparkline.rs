//! One-line sparklines drawn with eighth-block characters.

/// Eight levels, lowest first.
const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// A sparkline of `values`, scaled so that `ceiling` is the top level.
/// Values above the ceiling clamp to the top; a zero ceiling draws the floor.
pub fn spark(values: &[u32], ceiling: u32) -> String {
    values
        .iter()
        .map(|value| {
            if ceiling == 0 {
                return LEVELS[0];
            }
            let scaled = (f64::from(*value) * 7.0 / f64::from(ceiling)).round();
            let index = (scaled as usize).min(LEVELS.len() - 1);
            LEVELS[index]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales_against_the_ceiling() {
        assert_eq!(spark(&[0, 1, 2, 4, 8], 8), "▁▂▃▅█");
    }

    #[test]
    fn clamps_values_above_the_ceiling() {
        assert_eq!(spark(&[100], 8), "█");
        assert_eq!(spark(&[3], 0), "▁");
    }
}
