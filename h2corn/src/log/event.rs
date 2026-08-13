//! Diagnostic events: the one encoder every stderr producer goes through.
//!
//! Split from the access-log formatting so the escaping and framing rules live
//! in one place rather than beside the human-readable line builder.

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU8, Ordering};

use crate::config::LogFormat;
use crate::log::{AccessLogBuf, AccessLogWriter, sink};

/// The encoding for this process's stderr, published once at serve start.
///
/// Diagnostics are emitted from paths that never see a `ServerConfig` -- a
/// panicking connection task, a supervisor signal -- so the choice is read
/// here rather than threaded through every error site. Access records still
/// take it from the request's own configuration.
static LOG_FORMAT: AtomicU8 = AtomicU8::new(0);
/// Severity of a diagnostic line.
///
/// Not chosen at the call site: it is a property of the event, so the same
/// event can never be logged at two different severities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Level {
    Info,
    Error,
}

impl Level {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Error => "error",
        }
    }
}

/// Every diagnostic h2corn writes to stderr.
///
/// The JSON `event` field is a contract with whatever consumes the stream, so
/// the vocabulary is a closed type rather than a string literal spelled out at
/// each call site: a typo is a compile error, the full set is readable in one
/// place, and renaming one is a change the compiler forces you to complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Event {
    Starting,
    Listening,
    Http1Enabled,
    Request,
    Failed,
    ConnectionPanicked,
}

impl Event {
    const fn name(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Listening => "listening",
            Self::Http1Enabled => "http1_enabled",
            Self::Request => "request",
            Self::Failed => "failed",
            Self::ConnectionPanicked => "connection_panicked",
        }
    }

    const fn level(self) -> Level {
        match self {
            Self::Starting | Self::Listening | Self::Http1Enabled | Self::Request => Level::Info,
            Self::Failed | Self::ConnectionPanicked => Level::Error,
        }
    }

    /// Emit with a human sentence and the machine-readable fields behind it.
    ///
    /// Both renderings are given together so they cannot drift apart, and the
    /// encoding comes from the one place it was published.
    pub(crate) fn emit(self, text: fmt::Arguments<'_>, fields: impl FnOnce(&mut JsonObject<'_>)) {
        let mut line = AccessLogBuf::new();
        let out = &mut AccessLogWriter(&mut line);
        match format() {
            LogFormat::Text => {
                let _ = out.write_fmt(text);
                let _ = out.write_char('\n');
            },
            LogFormat::Json => {
                let mut object = JsonObject::new(out);
                object.str("level", self.level().as_str());
                object.str("event", self.name());
                fields(&mut object);
                object.finish();
            },
        }
        sink::write_line(line.as_slice());
    }

    /// The JSON arm for a producer that renders its own text.
    ///
    /// The aligned access line and the styled multi-line banner are richer
    /// than one sentence, so those two branch on the format themselves and
    /// call this inside the JSON arm. No-ops on a text stream, so it cannot
    /// leak an object into one.
    pub(crate) fn emit_json(self, fields: impl FnOnce(&mut JsonObject<'_>)) {
        if format() != LogFormat::Json {
            return;
        }
        self.emit(format_args!(""), fields);
    }
}

/// Escapes as it writes, so a value never has to be buffered to be quoted.
///
/// Everything below 0x20 becomes `\uXXXX`, which is what keeps one record on
/// one line whatever a peer sent.
struct JsonStringWriter<'a> {
    out: &'a mut dyn fmt::Write,
}

impl fmt::Write for JsonStringWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        for ch in value.chars() {
            match ch {
                '"' => self.out.write_str("\\\"")?,
                '\\' => self.out.write_str("\\\\")?,
                '\n' => self.out.write_str("\\n")?,
                '\r' => self.out.write_str("\\r")?,
                '\t' => self.out.write_str("\\t")?,
                c if (c as u32) < 0x20 => write!(self.out, "\\u{:04x}", c as u32)?,
                c => self.out.write_char(c)?,
            }
        }
        Ok(())
    }
}

/// Writes one JSON object, owning the escaping every field needs.
///
/// Request targets, proxy-supplied labels and error text are all
/// attacker-influenced, so no producer is trusted to pre-escape its own
/// values -- that is the single reason this type exists rather than each call
/// site writing its own braces.
pub(crate) struct JsonObject<'a> {
    out: &'a mut dyn fmt::Write,
    empty: bool,
}

impl<'a> JsonObject<'a> {
    fn new(out: &'a mut dyn fmt::Write) -> Self {
        let _ = out.write_char('{');
        Self { out, empty: true }
    }

    fn key(&mut self, name: &str) {
        if !self.empty {
            let _ = self.out.write_char(',');
        }
        self.empty = false;
        write_json_string(self.out, name);
        let _ = self.out.write_char(':');
    }

    /// A quoted, escaped string field.
    pub(crate) fn str(&mut self, name: &str, value: &str) {
        self.key(name);
        write_json_string(self.out, value);
    }

    /// A `Display` value quoted as a string, escaped without buffering it.
    pub(crate) fn text(&mut self, name: &str, value: impl fmt::Display) {
        self.key(name);
        let _ = self.out.write_char('"');
        let _ = write!(JsonStringWriter { out: self.out }, "{value}");
        let _ = self.out.write_char('"');
    }

    /// A `Display` value written as a bare JSON number.
    pub(crate) fn num(&mut self, name: &str, value: impl fmt::Display) {
        self.key(name);
        let _ = write!(self.out, "{value}");
    }

    fn finish(self) {
        let _ = self.out.write_str("}\n");
    }
}

/// Write `value` as a JSON string literal.
fn write_json_string(out: &mut dyn fmt::Write, value: &str) {
    let _ = out.write_char('"');
    let _ = JsonStringWriter { out }.write_str(value);
    let _ = out.write_char('"');
}

pub(crate) fn set_format(format: LogFormat) {
    LOG_FORMAT.store(
        match format {
            LogFormat::Text => 0,
            LogFormat::Json => 1,
        },
        Ordering::Relaxed,
    );
}

fn format() -> LogFormat {
    match LOG_FORMAT.load(Ordering::Relaxed) {
        0 => LogFormat::Text,
        _ => LogFormat::Json,
    }
}

#[cfg(test)]
mod tests {
    use super::write_json_string;

    #[test]
    fn json_escaping_preserves_hostile_text_without_newlines() {
        let mut output = String::new();
        write_json_string(&mut output, "quote\" slash\\ newline\n tab\t nul\u{0}");
        assert_eq!(
            output,
            "\"quote\\\" slash\\\\ newline\\n tab\\t nul\\u0000\""
        );
        assert!(!output.contains('\n'));
    }
}
