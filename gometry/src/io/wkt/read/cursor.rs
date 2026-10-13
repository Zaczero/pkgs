#[cfg(test)]
use super::WKT_CURSOR_WORK;
use super::{IoError, ParentScope, ParseFormat, Result, WktParser};

macro_rules! skip_wkt_whitespace {
    ($parser:ident) => {{
        loop {
            let Some(byte) = $parser.cursor.peek() else {
                break;
            };
            if byte.is_ascii_whitespace() {
                let whitespace = $parser.cursor.ascii_whitespace_len();
                $parser.advance_cursor_by(whitespace)?;
                continue;
            }
            if byte.is_ascii() {
                break;
            }
            let Some(character) = $parser
                .input
                .get($parser.position()..)
                .and_then(|tail| tail.chars().next())
            else {
                break;
            };
            if !character.is_whitespace() {
                break;
            }
            $parser.advance_cursor_by(character.len_utf8())?;
        }
    }};
}

impl WktParser<'_> {
    pub(super) fn finish_member_separator(&mut self, after_comma: &mut bool) -> Result<()> {
        self.skip_whitespace()?;
        match self.cursor.peek() {
            Some(b',') => {
                self.advance_cursor()?;
                *after_comma = true;
                Ok(())
            },
            Some(b')') => {
                *after_comma = false;
                Ok(())
            },
            None => self.fail("WKT is missing closing parenthesis"),
            Some(_) => self.fail("expected comma between WKT members"),
        }
    }

    pub(super) fn check_parent_boundary(&mut self, scope: ParentScope) -> Result<()> {
        self.skip_whitespace()?;
        match (scope, self.cursor.peek()) {
            (ParentScope::Root | ParentScope::Collection, None)
            | (ParentScope::Collection, Some(b',' | b')')) => Ok(()),
            (_, Some(_)) => self.fail("trailing text after WKT geometry"),
        }
    }

    pub(super) fn is_wkt_member_list(&mut self) -> Result<bool> {
        self.skip_whitespace()?;
        Ok(self.cursor.peek() == Some(b'(') || self.is_wkt_empty_keyword())
    }

    pub(super) fn is_wkt_empty_keyword(&self) -> bool {
        self.cursor
            .remaining
            .get(..5)
            .is_some_and(|keyword| keyword.eq_ignore_ascii_case(b"EMPTY"))
            && !self
                .cursor
                .remaining
                .get(5)
                .is_some_and(u8::is_ascii_alphanumeric)
    }

    pub(super) fn at_parenthesis_close(&mut self) -> Result<bool> {
        self.skip_whitespace()?;
        Ok(self.cursor.peek() == Some(b')'))
    }

    pub(super) fn skip_whitespace(&mut self) -> Result<()> {
        skip_wkt_whitespace!(self);
        Ok(())
    }

    pub(super) fn advance_char(&mut self) -> Result<()> {
        let Some(byte) = self.cursor.peek() else {
            return Ok(());
        };
        if byte.is_ascii() {
            self.advance_cursor()?;
        } else if let Some(character) = self
            .input
            .get(self.position()..)
            .and_then(|tail| tail.chars().next())
        {
            self.advance_cursor_by(character.len_utf8())?;
        }
        Ok(())
    }

    pub(super) fn open_parenthesis(&mut self) -> Result<()> {
        let depth = self
            .parenthesis_depth
            .checked_add(1)
            .ok_or_else(|| self.error_at(self.position(), "WKT nesting depth overflow"))?;
        self.advance_cursor()?;
        self.parenthesis_depth = depth;
        Ok(())
    }

    pub(super) fn close_parenthesis(&mut self) -> Result<()> {
        self.skip_whitespace()?;
        if self.cursor.peek() != Some(b')') {
            return if self.cursor.is_empty() {
                self.fail("WKT is missing closing parenthesis")
            } else {
                self.fail("invalid WKT parentheses")
            };
        }
        let depth = self
            .parenthesis_depth
            .checked_sub(1)
            .ok_or_else(|| self.error_at(self.position(), "WKT nesting depth underflow"))?;
        self.advance_cursor()?;
        self.parenthesis_depth = depth;
        Ok(())
    }

    pub(super) fn recover_to_geometry_close(&mut self, body_depth: usize) -> Result<bool> {
        let mut nested = self.parenthesis_depth.saturating_sub(body_depth);
        while let Some(byte) = self.cursor.peek() {
            match byte {
                b'(' => {
                    nested = nested.checked_add(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth overflow")
                    })?;
                    self.open_parenthesis()?;
                },
                b')' if nested == 0 => {
                    let depth = self.parenthesis_depth.checked_sub(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth underflow")
                    })?;
                    self.advance_cursor()?;
                    self.parenthesis_depth = depth;
                    return Ok(true);
                },
                b')' => {
                    nested = nested.checked_sub(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth underflow")
                    })?;
                    let depth = self.parenthesis_depth.checked_sub(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth underflow")
                    })?;
                    self.advance_cursor()?;
                    self.parenthesis_depth = depth;
                },
                _ => self.advance_char()?,
            }
        }
        Ok(false)
    }

    pub(super) fn recover_to_group_close(&mut self, group_depth: usize) -> Result<bool> {
        self.recover_to_geometry_close(group_depth)
    }

    pub(super) fn recover_collection_member(&mut self, base_depth: usize) -> Result<()> {
        let mut nested = self.parenthesis_depth.saturating_sub(base_depth);
        while let Some(byte) = self.cursor.peek() {
            match byte {
                b'(' => {
                    nested = nested.checked_add(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth overflow")
                    })?;
                    self.open_parenthesis()?;
                },
                b')' | b',' if nested == 0 => break,
                b')' => {
                    nested = nested.checked_sub(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth underflow")
                    })?;
                    let depth = self.parenthesis_depth.checked_sub(1).ok_or_else(|| {
                        self.error_at(self.position(), "WKT nesting depth underflow")
                    })?;
                    self.advance_cursor()?;
                    self.parenthesis_depth = depth;
                },
                _ => self.advance_char()?,
            }
        }
        debug_assert!(self.parenthesis_depth >= base_depth);
        Ok(())
    }

    pub(super) fn advance_cursor(&mut self) -> Result<()> {
        if !self.cursor.advance() {
            return self.fail("WKT input position overflow");
        }
        #[cfg(test)]
        WKT_CURSOR_WORK.with(|work| work.set(work.get().wrapping_add(1)));
        Ok(())
    }

    pub(super) fn advance_cursor_by(&mut self, count: usize) -> Result<()> {
        if !self.cursor.advance_by(count) {
            return self.fail("WKT input position overflow");
        }
        #[cfg(test)]
        WKT_CURSOR_WORK.with(|work| work.set(work.get().wrapping_add(count)));
        Ok(())
    }

    pub(super) fn error_at(
        &self,
        position: usize,
        message: impl Into<Box<str>>,
    ) -> crate::error::Error {
        IoError::parse_at(ParseFormat::Wkt, position, message)
    }

    pub(super) fn position_error(&self, error: crate::error::Error) -> crate::error::Error {
        match error.kind() {
            crate::error::ErrorKind::Geometry(_) => {
                IoError::parse_at(ParseFormat::Wkt, self.position(), error.to_string())
            },
            crate::error::ErrorKind::Io(_) => error.with_parse_position(self.position()),
            crate::error::ErrorKind::Crs(_)
            | crate::error::ErrorKind::Frame(_)
            | crate::error::ErrorKind::Resource(_) => error,
        }
    }

    pub(super) fn fail<T>(&self, message: impl Into<Box<str>>) -> Result<T> {
        self.fail_at(self.position(), message)
    }

    pub(super) fn fail_at<T>(&self, position: usize, message: impl Into<Box<str>>) -> Result<T> {
        Err(self.error_at(position, message))
    }
}

pub(super) fn remember_first(slot: &mut Option<crate::error::Error>, error: crate::error::Error) {
    if slot.is_none() {
        *slot = Some(error);
    }
}

pub(super) struct WktByteCursor<'a> {
    pub(super) remaining: &'a [u8],
}

impl<'a> WktByteCursor<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    pub(super) const fn peek(&self) -> Option<u8> {
        self.remaining.first().copied()
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    pub(super) fn ascii_whitespace_len(&self) -> usize {
        self.remaining
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
            .unwrap_or(self.remaining.len())
    }

    pub(super) const fn advance(&mut self) -> bool {
        if let Some((_, tail)) = self.remaining.split_first() {
            self.remaining = tail;
            true
        } else {
            false
        }
    }

    pub(super) fn advance_by(&mut self, count: usize) -> bool {
        let Some(remaining) = self.remaining.get(count..) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}
