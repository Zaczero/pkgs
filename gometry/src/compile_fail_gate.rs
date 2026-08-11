//! Unified compile-fail gate: every `src/**/compile_fail/*.rs` fragment must
//! fail to compile with its registered stable error code **and** a message
//! fragment naming the guarded property, and must `#[path]`-include at least
//! one real production module.
//!
//! One driver for the whole crate — not per-domain substring scanners.
//!
//! # Contract
//!
//! 1. Discovery walks `src/**/compile_fail/*.rs` and matches an explicit
//!    property/code/message-needle manifest (unknown fragments fail the gate).
//! 2. Each fragment is compiled once with `rustc --error-format=json` and
//!    `--emit=metadata,dep-info`.
//! 3. Assertions (all required): compilation failed; unique coded error set is
//!    exactly the registered code; at least one coded diagnostic's combined
//!    text (message + span labels + children) contains the property needle
//!    (so same-code different-message regressions cannot pass); every coded
//!    primary span is in the fixture; every coded diagnostic points to one
//!    source coordinate (multiple diagnostics at one expression allowed);
//!    no code-less diagnostic has a primary source span; rustc's depfile lists
//!    a canonical path under repository `src/` outside every `compile_fail/`
//!    directory and distinct from the fixture.
//!
//! Failure messages name the property, fixture, expected code, needle, actual
//! `(code, file, line, column)` set, observed messages, and production deps.
//!
//! # Fixture shape rule
//!
//! A fixture's named regression must make the fragment **compile**. Passing a
//! value wrong under both the correct and regressed signature leaves an
//! error-code-only gate green while the property is gone (E0308 for
//! `&ProjContext` vs E0308 for `*mut c_void`/`*mut u8`).

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde_json::Value;

/// One fixture: path, property name, expected code, message needle.
struct Fixture {
    /// Path relative to `src/`, e.g. `crs/compile_fail/escaping_context.rs`.
    rel: &'static str,
    property: &'static str,
    code: &'static str,
    /// Substring that must appear in the coded diagnostic's combined text
    /// (top-level message, span labels, child notes). Names the guarded
    /// property so a same-code different-message regression cannot pass.
    message_needle: &'static str,
}

/// Explicit manifest — discovery must match this set exactly.
const MANIFEST: &[Fixture] = &[
    Fixture {
        rel: "crs/compile_fail/escaping_context.rs",
        property: "crs/no_raw_context_escape",
        code: "E0308",
        // Span label / child note: "expected `&ProjContext`, found …"
        message_needle: "&ProjContext",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_c_mutable_pyclass.rs",
        property: "item_c/sealed_immutable_i64_owner",
        code: "E0277",
        message_needle: "Sealed",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_d_foreign_buffer_borrow.rs",
        property: "item_d/no_producer_borrow",
        code: "E0599",
        message_needle: "as_slice",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_e_foreign_buffer_send.rs",
        property: "item_e/not_send",
        code: "E0277",
        // rustc: "`…` cannot be sent between threads safely"
        message_needle: "sent between threads",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_f_foreign_buffer_sync.rs",
        property: "item_f/not_sync",
        code: "E0277",
        // rustc: "`…` cannot be shared between threads safely"
        message_needle: "shared between threads",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_g_foreign_buffer_outlives_owner.rs",
        property: "item_g/owner_lifetime",
        code: "E0515",
        message_needle: "local variable `owner`",
    },
    Fixture {
        rel: "py/arrow_c/compile_fail/item_h_foreign_buffer_snapshot_consumes.rs",
        property: "item_h/snapshot_consumes_foreign_buffer",
        code: "E0382",
        message_needle: "use of moved value: `foreign`",
    },
];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CodedSite {
    code: String,
    file: String,
    line: u64,
    column: u64,
    /// Combined diagnostic text used for needle matching.
    text: String,
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn src_dir() -> PathBuf {
    manifest_dir().join("src")
}

/// Recursively find `compile_fail/*.rs` under `src/`.
fn discover_fragments(src: &Path) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    let mut stack = vec![src.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let is_rs = path.extension().is_some_and(|e| e == "rs");
            let parent_is_compile_fail = path
                .parent()
                .and_then(|p| p.file_name())
                .is_some_and(|n| n == "compile_fail");
            if is_rs && parent_is_compile_fail {
                out.insert(path);
            }
        }
    }
    out
}

fn rel_under_src(path: &Path, src: &Path) -> String {
    path.strip_prefix(src).map_or_else(
        |_| path.to_string_lossy().replace('\\', "/"),
        |p| p.to_string_lossy().replace('\\', "/"),
    )
}

/// Extract the stable rustc error code string from a JSON diagnostic.
fn diagnostic_code(diag: &Value) -> Option<String> {
    let code = diag.get("code")?;
    if code.is_null() {
        return None;
    }
    // Modern rustc: `"code": { "code": "E0599", "explanation": "..." }`
    if let Some(s) = code.get("code").and_then(Value::as_str) {
        return Some(s.to_owned());
    }
    // Older / alternate: `"code": "E0599"`
    code.as_str().map(str::to_owned)
}

fn primary_spans(diag: &Value) -> Vec<(String, u64, u64)> {
    let Some(spans) = diag.get("spans").and_then(Value::as_array) else {
        return Vec::new();
    };
    spans
        .iter()
        .filter(|s| {
            s.get("is_primary")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .filter_map(|s| {
            let file = s.get("file_name")?.as_str()?.to_owned();
            let line = s.get("line_start")?.as_u64()?;
            let column = s.get("column_start")?.as_u64()?;
            Some((file, line, column))
        })
        .collect()
}

/// Concatenate message + span labels + child notes for needle matching.
fn diagnostic_text(diag: &Value) -> String {
    let mut parts = Vec::new();
    if let Some(m) = diag.get("message").and_then(Value::as_str) {
        parts.push(m.to_owned());
    }
    if let Some(spans) = diag.get("spans").and_then(Value::as_array) {
        for span in spans {
            if let Some(label) = span.get("label").and_then(Value::as_str) {
                parts.push(label.to_owned());
            }
        }
    }
    if let Some(children) = diag.get("children").and_then(Value::as_array) {
        for child in children {
            if let Some(m) = child.get("message").and_then(Value::as_str) {
                parts.push(m.to_owned());
            }
            // One level of nested children (rustc notes rarely nest deeper).
            if let Some(grand) = child.get("children").and_then(Value::as_array) {
                for g in grand {
                    if let Some(m) = g.get("message").and_then(Value::as_str) {
                        parts.push(m.to_owned());
                    }
                }
            }
        }
    }
    parts.join("\n")
}

fn parse_coded_sites(stderr: &str) -> (Vec<CodedSite>, Vec<String>) {
    let mut sites = Vec::new();
    let mut codeless_primary = Vec::new();
    for line in stderr.lines() {
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            continue;
        }
        let Ok(diag) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if diag.get("level").and_then(Value::as_str) != Some("error") {
            continue;
        }
        // Skip rustc summary lines (no code, no useful span).
        let message = diag.get("message").and_then(Value::as_str).unwrap_or("");
        if message.starts_with("aborting due to")
            || message.starts_with("For more information about this error")
        {
            continue;
        }
        let code = diagnostic_code(&diag);
        let text = diagnostic_text(&diag);
        let primaries = primary_spans(&diag);
        match code {
            Some(code) => {
                if primaries.is_empty() {
                    sites.push(CodedSite {
                        code,
                        file: String::new(),
                        line: 0,
                        column: 0,
                        text,
                    });
                } else {
                    for (file, line, column) in primaries {
                        sites.push(CodedSite {
                            code: code.clone(),
                            file,
                            line,
                            column,
                            text: text.clone(),
                        });
                    }
                }
            },
            None => {
                for (file, line, column) in primaries {
                    codeless_primary.push(format!("{file}:{line}:{column}"));
                }
            },
        }
    }
    (sites, codeless_primary)
}

fn canonicalize_dep(token: &str) -> PathBuf {
    let path = PathBuf::from(token);
    if let Ok(c) = path.canonicalize() {
        return c;
    }
    let candidate = if path.is_absolute() {
        path
    } else {
        manifest_dir().join(path)
    };
    candidate.canonicalize().unwrap_or(candidate)
}

/// Parse rustc dep-info for production `src/` dependencies.
fn production_deps(dep_path: &Path, src: &Path, fixture: &Path) -> Vec<PathBuf> {
    let Ok(text) = fs::read_to_string(dep_path) else {
        return Vec::new();
    };
    // dep-info format: `target: dep1 dep2 ...` with optional line continuations.
    let body = text.split_once(':').map_or(text.as_str(), |(_, r)| r);
    let Ok(src_canon) = src.canonicalize() else {
        return Vec::new();
    };
    let fixture_canon = fixture
        .canonicalize()
        .unwrap_or_else(|_| fixture.to_path_buf());
    body.split_whitespace()
        .filter(|token| *token != "\\")
        .map(canonicalize_dep)
        .filter(|d| {
            if d == &fixture_canon {
                return false;
            }
            let Ok(rel) = d.strip_prefix(&src_canon) else {
                return false;
            };
            !rel.components().any(|c| c.as_os_str() == "compile_fail")
        })
        .collect()
}

fn paths_share_suffix(a: &str, b: &Path) -> bool {
    let a = a.replace('\\', "/");
    let b = b.to_string_lossy().replace('\\', "/");
    a == b || a.ends_with(&b) || b.ends_with(&a)
}

fn format_sites(sites: &[CodedSite]) -> Vec<String> {
    sites
        .iter()
        .map(|s| {
            format!(
                "({},{},{},{} | {:?})",
                s.code,
                s.file,
                s.line,
                s.column,
                s.text.chars().take(80).collect::<String>()
            )
        })
        .collect()
}

fn rustc_fixture(
    fixture_path: &Path,
    dep_path: &Path,
    meta_path: &Path,
) -> std::io::Result<std::process::Output> {
    Command::new("rustc")
        .args([
            "--edition",
            "2024",
            "--crate-type",
            "bin",
            "--error-format=json",
            "--emit",
            &format!("metadata,dep-info={}", dep_path.display()),
            "-o",
            &meta_path.to_string_lossy(),
            &fixture_path.to_string_lossy(),
        ])
        .output()
}

fn check_coded_sites(fx: &Fixture, sites: &[CodedSite], fixture_path: &Path) -> Result<(), String> {
    let codes: BTreeSet<&str> = sites.iter().map(|s| s.code.as_str()).collect();
    if codes.len() != 1 || !codes.contains(fx.code) {
        return Err(format!(
            "{}: expected {} at one fixture site; got coded set {:?}\n  actual={:?}\n  fixture={}",
            fx.property,
            fx.code,
            codes,
            format_sites(sites),
            fx.rel
        ));
    }

    // Property needle: at least one coded diagnostic must name the guarded
    // concept (not merely share the error code with a different mismatch).
    let needle_hit = sites.iter().any(|s| s.text.contains(fx.message_needle));
    if !needle_hit {
        let texts: Vec<&str> = sites.iter().map(|s| s.text.as_str()).collect();
        return Err(format!(
            "{}: expected {} with message needle {:?}; code matched but needle missing\n  observed_texts={texts:?}\n  fixture={}",
            fx.property, fx.code, fx.message_needle, fx.rel
        ));
    }

    for site in sites {
        if site.file.is_empty() {
            return Err(format!(
                "{}: coded {} has no primary span file\n  fixture={}",
                fx.property, site.code, fx.rel
            ));
        }
        if !paths_share_suffix(&site.file, fixture_path)
            && !paths_share_suffix(&site.file, Path::new(fx.rel))
        {
            return Err(format!(
                "{}: primary span not in fixture: ({},{},{},{})\n  fixture={}",
                fx.property, site.code, site.file, site.line, site.column, fx.rel
            ));
        }
    }
    // One source coordinate for all coded diagnostics (multi-E0277 at one expr OK).
    let coords: HashSet<(u64, u64)> = sites.iter().map(|s| (s.line, s.column)).collect();
    if coords.len() != 1 {
        return Err(format!(
            "{}: expected one failing source coordinate, got {}\n  actual={:?}\n  fixture={}",
            fx.property,
            coords.len(),
            format_sites(sites),
            fx.rel
        ));
    }
    Ok(())
}

fn run_fixture(fx: &Fixture, src: &Path, out_dir: &Path) -> Result<(), String> {
    let fixture_path = src.join(fx.rel);
    if !fixture_path.is_file() {
        return Err(format!(
            "{}: fixture file missing at {}",
            fx.property,
            fixture_path.display()
        ));
    }

    let stem = fixture_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fixture");
    let dep_path = out_dir.join(format!("{stem}.d"));
    let meta_path = out_dir.join(format!("{stem}.rmeta"));

    let output = rustc_fixture(&fixture_path, &dep_path, &meta_path)
        .map_err(|e| format!("{}: failed to spawn rustc: {e}", fx.property))?;

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    if output.status.success() {
        return Err(format!(
            "{}: expected compile failure with {} + needle {:?}, but rustc succeeded\n  fixture={}\n  stdout={stdout}\n  stderr={stderr}",
            fx.property, fx.code, fx.message_needle, fx.rel
        ));
    }

    let (sites, codeless_primary) = parse_coded_sites(&stderr);
    if !codeless_primary.is_empty() {
        return Err(format!(
            "{}: code-less diagnostic(s) with primary source span (not allowed): {}\n  fixture={}\n  stderr={stderr}",
            fx.property,
            codeless_primary.join(", "),
            fx.rel
        ));
    }
    if sites.is_empty() {
        return Err(format!(
            "{}: expected {} + needle {:?}, got no coded errors\n  fixture={}\n  stderr={stderr}",
            fx.property, fx.code, fx.message_needle, fx.rel
        ));
    }
    check_coded_sites(fx, &sites, &fixture_path)?;

    let prod = production_deps(&dep_path, src, &fixture_path);
    if prod.is_empty() {
        return Err(format!(
            "{}: expected at least one production src/ dep outside compile_fail/; dep-info empty or fixture-only\n  fixture={}\n  depfile={}",
            fx.property,
            fx.rel,
            dep_path.display()
        ));
    }
    Ok(())
}

fn failure_with_deps(fx: &Fixture, src: &Path, out_dir: &Path, err: &str) -> String {
    let fixture_path = src.join(fx.rel);
    let stem = fixture_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fixture");
    let dep_path = out_dir.join(format!("{stem}.d"));
    let prod = production_deps(&dep_path, src, &fixture_path);
    let prod_names: Vec<String> = prod
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();
    format!("{err}\n  production deps={prod_names:?}")
}

#[test]
fn compile_fail_fixtures_guard_production() {
    let src = src_dir();
    let discovered = discover_fragments(&src);
    let discovered_rels: BTreeSet<String> =
        discovered.iter().map(|p| rel_under_src(p, &src)).collect();
    let manifest_rels: BTreeSet<String> = MANIFEST.iter().map(|f| f.rel.to_owned()).collect();

    assert_eq!(
        discovered_rels, manifest_rels,
        "compile_fail discovery must match the property/code manifest\n  discovered={discovered_rels:?}\n  manifest={manifest_rels:?}"
    );

    let out_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| manifest_dir().join("target"), PathBuf::from)
        .join("compile_fail_gate");
    fs::create_dir_all(&out_dir).expect("create compile_fail_gate output dir");

    let start = Instant::now();
    let mut failures = Vec::new();
    for fx in MANIFEST {
        if let Err(e) = run_fixture(fx, &src, &out_dir) {
            failures.push(failure_with_deps(fx, &src, &out_dir, &e));
        }
    }
    let elapsed = start.elapsed();
    // Print-only wall-clock (no timing assertion — owner ban).
    eprintln!(
        "compile_fail_gate: {} fixtures in {:.3}s",
        MANIFEST.len(),
        elapsed.as_secs_f64()
    );

    assert!(
        failures.is_empty(),
        "compile_fail_gate failed for {} fixture(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
