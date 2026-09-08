//! DE-9IM pattern parsing.

/// Nine OGC pattern literals admitted at ingress. Construction is
/// [`Self::new`] only; engines carry this type instead of reparsing `&str`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CompiledPattern {
    cells: [u8; 9],
}

impl CompiledPattern {
    pub(crate) const fn new(pattern: &str) -> Option<Self> {
        let bytes = pattern.as_bytes();
        if bytes.len() != 9 {
            return None;
        }
        let mut cells = [0_u8; 9];
        let mut index = 0;
        while index < 9 {
            let byte = bytes[index];
            if !matches!(byte, b'T' | b'*' | b'F' | b'0' | b'1' | b'2') {
                return None;
            }
            cells[index] = byte;
            index += 1;
        }
        Some(Self { cells })
    }

    /// Validated literals for a leaf that already holds this type.
    pub(crate) const fn cells(self) -> [u8; 9] {
        self.cells
    }

    /// The pattern that asks of the transposed pair what `self` asks of this
    /// one: rows and columns swapped.
    pub(crate) const fn transpose(self) -> Self {
        let p = self.cells;
        Self {
            cells: [p[0], p[3], p[6], p[1], p[4], p[7], p[2], p[5], p[8]],
        }
    }
}

#[cfg(test)]
pub(crate) const fn compile(pattern: &str) -> CompiledPattern {
    CompiledPattern::new(pattern).expect("test DE-9IM pattern is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_admits_each_ogc_literal() {
        for literal in *b"TF*012" {
            let mut pattern = *b"*********";
            pattern[0] = literal;
            let text = std::str::from_utf8(&pattern).unwrap();
            let compiled = CompiledPattern::new(text).expect("literal is in the alphabet");
            assert_eq!(compiled.cells()[0], literal);
        }
    }

    #[test]
    fn new_rejects_length_charset_and_multibyte() {
        assert!(CompiledPattern::new("").is_none());
        assert!(CompiledPattern::new("T*******").is_none());
        assert!(CompiledPattern::new("T********X").is_none());
        assert!(CompiledPattern::new("T*******X").is_none());
        assert!(CompiledPattern::new("T******ä").is_none());
        assert!(CompiledPattern::new("T*******é").is_none());
    }

    #[test]
    fn transpose_swaps_rows_and_columns_without_reparsing() {
        let compiled = compile("TF*012F*T");
        assert_eq!(compiled.transpose().cells(), compile("T0FF1**2T").cells());
    }
}
