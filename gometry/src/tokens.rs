//! Declarative string-token enums: one macro generates the enum, its canonical
//! token table, `parse`, and the canonical `unknown <concept> <value>;
//! expected ...` message — so every token surface shares one spelling and the
//! error template cannot drift per site.

/// Render a token list as `'a', 'b', or 'c'` (`'a' or 'b'` for two, `'a'` for
/// one) for the canonical unknown-token message.
pub(crate) fn expected_tokens(tokens: &[&str]) -> String {
    match tokens {
        [] => String::new(),
        [only] => format!("'{only}'"),
        [first, last] => format!("'{first}' or '{last}'"),
        [head @ .., last] => {
            let mut out = String::new();
            for token in head {
                out.push('\'');
                out.push_str(token);
                out.push_str("', ");
            }
            out.push_str("or '");
            out.push_str(last);
            out.push('\'');
            out
        },
    }
}

/// The canonical unknown-token message: `unknown <concept> <value:?>; did
/// you mean '<closest>'? expected 'a', 'b', or 'c'` — the did-you-mean
/// clause appears when a valid token is within
/// `(len/3).clamp(1, 2)` edits, so every token surface inherits typo
/// suggestions from this one site without short nonsense matching freely.
pub(crate) fn unknown_token_message(concept: &str, value: &str, tokens: &[&str]) -> String {
    let suggestion =
        closest_token(value, tokens).map_or_default(|token| format!("did you mean '{token}'? "));
    format!(
        "unknown {concept} {value:?}; {suggestion}expected {}",
        expected_tokens(tokens)
    )
}

/// The valid token nearest to `value` within the did-you-mean threshold
/// (case-insensitive Levenshtein; first declaration wins a tie).
pub(crate) fn closest_token<'a>(value: &str, tokens: &[&'a str]) -> Option<&'a str> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    // Floor of 2 made any 2–3 char token match any other; clamp so short
    // nonsense ('xx', 'zzz') stays silent while real typos still suggest.
    let threshold = (value.len() / 3).clamp(1, 2);
    let max_token_len = tokens.iter().map(|token| token.len()).max()?;
    let mut row = vec![0; max_token_len + 1];
    let mut best: Option<(usize, &str)> = None;
    for &token in tokens {
        let distance = edit_distance_ascii_casefold(value.as_bytes(), token.as_bytes(), &mut row);
        if distance <= threshold && best.is_none_or(|(record, _)| distance < record) {
            best = Some((distance, token));
        }
    }
    best.map(|(_, token)| token)
}

/// Case-insensitive Levenshtein distance over bytes (tokens are ASCII), using
/// one reusable row and a saved diagonal cell.
fn edit_distance_ascii_casefold(left: &[u8], right: &[u8], row: &mut [usize]) -> usize {
    for (column, value) in row.iter_mut().enumerate().take(right.len() + 1) {
        *value = column;
    }
    for (row_index, &l) in left.iter().enumerate() {
        let mut diagonal = row[0];
        row[0] = row_index + 1;
        for (column, &r) in right.iter().enumerate() {
            let old_above = row[column + 1];
            let substitution = diagonal + usize::from(!l.eq_ignore_ascii_case(&r));
            row[column + 1] = substitution.min(old_above + 1).min(row[column] + 1);
            diagonal = old_above;
        }
    }
    row[right.len()]
}

/// Token normalizer for PROJ export options: trimmed ASCII uppercase with
/// `-`/` `/`:` folded to `_` (accepts `wkt2:2019` for `WKT2_2019`).
pub(crate) fn export_token(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .replace(['-', ' ', ':'], "_")
}

/// Declare a string-token enum: variants with their canonical token (plus
/// optional accepted aliases), a fixed concept name for the error message, and
/// an optional normalizer applied before matching. `token = none` omits the
/// reverse spelling method for parser-only enums.
///
/// Generates the enum, `TOKENS` (canonical tokens, declaration order),
/// `token()` (the canonical spelling of a variant), and `parse()` returning
/// the canonical unknown-token message on a miss. Pair with
/// [`token_from_pyobject!`] at the `PyO3` layer to accept the enum directly as
/// an argument type.
macro_rules! token_enum {
    (@parameter $concept:literal) => { $concept };
    (@parameter $concept:literal, $parameter:literal) => { $parameter };

    (@token [$vis:vis] [] {
        $($variant:ident = $token:literal),+ $(,)?
    }) => {
    };

    (@token [$vis:vis] [none] {
        $($variant:ident = $token:literal),+ $(,)?
    }) => {};

    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident(
            $concept:literal,
            normalize = lowercase_token
            $(, token = $token_mode:ident)?
            $(, param = $parameter:literal)?
        ) {
            $($(#[$vmeta:meta])* $variant:ident = $token:literal $(| $alias:literal)*),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// The parameter concept associated with this token vocabulary.
            $vis const PARAMETER: &'static str = $crate::tokens::token_enum!(
                @parameter $concept $(, $parameter)?
            );

            /// Canonical token per variant, in declaration order.
            $vis const TOKENS: &'static [&'static str] = &[$($token),+];

            $crate::tokens::token_enum!(@token [$vis] [$($token_mode)?] {
                $($variant = $token),+
            });

            /// Parse a token; the error is the canonical
            /// `unknown <concept> <value>; expected ...` message.
            $vis fn parse(value: &str) -> Result<Self, String> {
                let raw = value;
                let value = value.trim();
                $(
                    if value.eq_ignore_ascii_case($token)
                        $(|| value.eq_ignore_ascii_case($alias))*
                    {
                        return Ok(Self::$variant);
                    }
                )+
                Err(crate::tokens::unknown_token_message(
                    $concept,
                    raw,
                    Self::TOKENS,
                ))
            }
        }
    };

    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident(
            $concept:literal
            $(, normalize = $normalize:ident)?
            $(, token = $token_mode:ident)?
            $(, param = $parameter:literal)?
        ) {
            $($(#[$vmeta:meta])* $variant:ident = $token:literal $(| $alias:literal)*),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        $vis enum $name {
            $($(#[$vmeta])* $variant,)+
        }

        impl $name {
            /// The parameter concept associated with this token vocabulary.
            $vis const PARAMETER: &'static str = $crate::tokens::token_enum!(
                @parameter $concept $(, $parameter)?
            );

            /// Canonical token per variant, in declaration order.
            $vis const TOKENS: &'static [&'static str] = &[$($token),+];

            $crate::tokens::token_enum!(@token [$vis] [$($token_mode)?] {
                $($variant = $token),+
            });

            /// Parse a token; the error is the canonical
            /// `unknown <concept> <value>; expected ...` message.
            $vis fn parse(value: &str) -> Result<Self, String> {
                let raw = value;
                $(
                    let normalized = crate::tokens::$normalize(value);
                    let value: &str = &normalized;
                )?
                match value {
                    $($token $(| $alias)* => Ok(Self::$variant),)+
                    _ => Err(crate::tokens::unknown_token_message(
                        $concept,
                        raw,
                        Self::TOKENS,
                    )),
                }
            }
        }
    };
}
pub(crate) use token_enum;

/// Accept a [`token_enum!`] type directly as a `PyO3` argument: extract the
/// string, parse, and raise `GeometryError` with the canonical message and
/// parameter concept on a miss.
macro_rules! token_from_pyobject {
    ($($name:ty),+ $(,)?) => {
        $(
            impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for $name {
                type Error = pyo3::PyErr;

                fn extract(
                    value: pyo3::Borrowed<'a, 'py, pyo3::types::PyAny>,
                ) -> pyo3::PyResult<Self> {
                    Self::parse(value.extract::<&str>()?).map_err(|message| {
                        $crate::py::errors::parameter_error(message, Self::PARAMETER)
                    })
                }
            }
        )+
    };
}
pub(crate) use token_from_pyobject;

#[cfg(test)]
mod tests {
    use super::closest_token;

    #[test]
    fn closest_token_keeps_declaration_order_for_ties() {
        assert_eq!(closest_token("ad", &["ab", "ac"]), Some("ab"));
        assert_eq!(closest_token("ad", &["ac", "ab"]), Some("ac"));
    }

    #[test]
    fn closest_token_handles_empty_inputs_and_tokens() {
        assert_eq!(closest_token("", &["a"]), None);
        assert_eq!(closest_token("round", &[]), None);
        assert_eq!(closest_token("a", &[""]), Some(""));
    }

    #[test]
    fn closest_token_resets_the_row_for_increasing_and_decreasing_lengths() {
        let tokens = ["a", "bb", "ccc", "dddd"];
        let reversed: Vec<_> = tokens.iter().copied().rev().collect();
        assert_eq!(closest_token("ddddx", &tokens), Some("dddd"));
        assert_eq!(closest_token("ddddx", &reversed), Some("dddd"));
    }

    #[test]
    fn closest_token_keeps_raw_utf8_byte_comparison() {
        assert_eq!(closest_token("FLATT", &["flat"]), Some("flat"));
        assert_eq!(closest_token("é", &["é", "ê"]), Some("é"));
        assert_eq!(closest_token("É", &["é", "ê"]), Some("é"));
        assert_eq!(closest_token("É", &["e"]), None);
    }
}
