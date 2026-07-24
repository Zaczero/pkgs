//! Allocation-free ASCII case-folding helpers for hot string comparisons.

/// Whether ``haystack`` contains ``needle`` as a substring, ASCII case-insensitively.
pub(crate) fn str_contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let needle = needle.as_bytes();
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}
