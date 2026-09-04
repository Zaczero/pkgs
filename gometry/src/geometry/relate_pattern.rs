//! DE-9IM pattern parsing.

use crate::geometry::relate::De9im;

#[derive(Clone, Copy)]
pub(crate) struct CompiledPattern<'a> {
    pattern: &'a str,
}

impl<'a> CompiledPattern<'a> {
    pub(crate) fn new(pattern: &'a str) -> Option<Self> {
        if pattern.len() != 9
            || pattern
                .bytes()
                .any(|byte| !matches!(byte, b'T' | b'*' | b'F' | b'0' | b'1' | b'2'))
        {
            return None;
        }
        Some(Self { pattern })
    }

    /// The nine validated literals carried by the direct engines.
    pub(crate) fn bytes(self) -> [u8; 9] {
        self.pattern.as_bytes().try_into().expect("a compiled pattern has nine literals")
    }

    pub(crate) fn matches(self, matrix: De9im) -> bool {
        matrix
            .matches(self.pattern)
            .expect("compiled DE-9IM pattern is valid")
    }
}
