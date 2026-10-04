use std::ffi::OsString;
use std::io::{Read as _, Write as _};
use std::os::unix::ffi::OsStringExt as _;
use std::process::{Command, Output, Stdio};

use serde_json::Value;

fn fast(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fast"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").expect("test PATH"))
        .env("HOME", "/proc/fast-unwritable")
        .env("TERM", "dumb")
        .args(args)
        .output()
        .expect("run fast")
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("fast stdout must be one JSON document")
}

#[test]
fn collector_setup_failure_is_not_a_usage_error() {
    use std::os::unix::process::CommandExt as _;

    let mut command = Command::new(env!("CARGO_BIN_EXE_fast"));
    command
        .env_clear()
        .args(["--once", "--", env!("CARGO_BIN_EXE_fast"), "--version"]);
    // SAFETY: the child only sets its descriptor limit before exec.
    unsafe {
        command.pre_exec(|| {
            let limit = libc::rlimit {
                rlim_cur: 4,
                rlim_max: 4,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &raw const limit) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
    let output = command.output().expect("limited collector");
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(!output.stderr.is_empty());
    assert!(output.stdout.is_empty());
}

#[test]
fn exact_arguments_preserve_spaces_and_shell_metacharacters() {
    let result = fast(&[
        "--once",
        "--json",
        "--",
        "sh",
        "-c",
        "test \"$1\" = 'a b;$HOME'",
        "fixture",
        "a b;$HOME",
    ]);
    assert!(result.status.success(), "{result:?}");
    assert_eq!(json(&result)["execution"], "exact_argv");
    assert_eq!(
        json(&result)["invocations"]
            .as_array()
            .expect("array")
            .len(),
        1
    );
}

#[test]
fn command_arguments_stop_fast_option_parsing_and_preserve_double_dash() {
    for explicit_end in [false, true] {
        let mut args = vec!["--once", "--json"];
        if explicit_end {
            args.push("--");
        }
        args.extend([
            "sh", "-c",
            "test \"$#\" -eq 7 && test \"$1\" = --json && test \"$5\" = -- && test \"$6\" = --separator && test -z \"$7\"",
            "fixture", "--json", "--runs", "7", "--help", "--", "--separator", "",
            ":::", "true", "embedded:::marker",
        ]);
        let output = fast(&args);
        assert!(output.status.success(), "{output:?}");
        let result = json(&output);
        assert_eq!(result["candidates"].as_array().expect("commands").len(), 2);
        assert_eq!(result["invocations"].as_array().expect("runs").len(), 2);
        assert_eq!(
            result["candidates"][1]["argv"][1]["value"],
            "embedded:::marker"
        );
    }
}

#[test]
fn custom_separator_preserves_literal_default_marker() {
    let output = fast(&[
        "--once",
        "--json",
        "--separator",
        "+++",
        "sh",
        "-c",
        "test \"$1\" = :::",
        "fixture",
        ":::",
        "+++",
        "true",
    ]);
    assert!(output.status.success(), "{output:?}");
    let result = json(&output);
    assert_eq!(result["candidates"].as_array().expect("commands").len(), 2);
    assert_eq!(result["candidates"][0]["argv"][4]["value"], ":::");
}

#[test]
fn custom_separator_can_be_non_utf8() {
    let marker = OsString::from_vec(vec![0xFF]);
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .args(["--once", "--json", "--separator"])
        .arg(&marker)
        .args(["true", ":::"])
        .arg(&marker)
        .arg("true")
        .output()
        .expect("custom byte separator");
    assert!(output.status.success(), "{output:?}");
    let result = json(&output);
    assert_eq!(result["candidates"].as_array().expect("commands").len(), 2);
    assert_eq!(result["candidates"][0]["argv"][1]["value"], ":::");
}

#[test]
fn empty_command_groups_are_rejected_before_hooks_or_execution() {
    for commands in [
        vec![":::", "true"],
        vec!["true", ":::"],
        vec!["true", ":::", ":::", "true"],
        vec!["true", ":::", ""],
    ] {
        let mut args = vec!["--once", "--json", "--prepare", "exit 77"];
        args.extend(commands);
        let output = fast(&args);
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("nonempty executable"));
    }
    let output = fast(&["--once", "--separator", "", "true"]);
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("--separator must not be empty"));
}

#[test]
fn direct_commands_do_not_require_a_shell_on_path() {
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .env_clear()
        .args(["--once", "--json", env!("CARGO_BIN_EXE_fast"), "--version"])
        .output()
        .expect("direct command without PATH");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        json(&output)["candidates"][0]["argv"]
            .as_array()
            .expect("argv")
            .len(),
        2
    );
}

#[test]
fn whole_command_strings_are_not_implicitly_reparsed() {
    let executable = "/proc/no such fast command";
    let output = fast(&["--once", "--json", executable]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let result = json(&output);
    assert_eq!(result["candidates"][0]["argv"][0]["value"], executable);
    assert_eq!(
        result["invocations"][0]["observation"]["outcome"]["kind"],
        "exec_error"
    );
}

#[test]
fn non_utf8_arguments_are_losslessly_reported() {
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .args(["--once", "--json", "--", "true"])
        .arg(OsString::from_vec(vec![0xFF, b'x']))
        .output()
        .expect("run");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        json(&output)["candidates"][0]["argv"][1]["encoding"],
        "bytes"
    );
    assert_eq!(
        json(&output)["candidates"][0]["argv"][1]["value"],
        serde_json::json!([255, 120])
    );
}

#[test]
fn explicit_shell_commands_use_shell_semantics() {
    let output = fast(&[
        "--once",
        "--json",
        "sh",
        "-c",
        "v='hello world'; test \"$v\" = 'hello world'",
        ":::",
        "sh",
        "-c",
        "printf hidden",
    ]);
    assert!(output.status.success(), "{output:?}");
    let value = json(&output);
    assert_eq!(value["invocations"].as_array().expect("array").len(), 2);
    assert_eq!(value["execution"], "exact_argv");
    assert_eq!(value["candidates"][0]["argv"][0]["value"], "sh");
}

#[test]
fn failures_abort_without_replacement_and_retain_diagnostics() {
    let output = fast(&[
        "--runs",
        "7",
        "--json",
        "sh",
        "-c",
        "printf failure >&2; exit 23",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let result = json(&output);
    assert_eq!(result["invocations"].as_array().expect("array").len(), 1);
    assert_eq!(
        result["invocations"][0]["observation"]["outcome"]["code"],
        23
    );
    assert_eq!(result["completed"], false);
}

#[test]
fn stderr_is_drained_after_capture_fills() {
    let output = fast(&[
        "--once",
        "--json",
        "sh",
        "-c",
        "head -c 262144 /dev/zero >&2; exit 9",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let result = json(&output);
    let diagnostics = &result["invocations"][0]["observation"]["diagnostics"];
    assert_eq!(
        diagnostics["first"].as_array().expect("head").len(),
        32 * 1024
    );
    assert_eq!(
        diagnostics["last"].as_array().expect("tail").len(),
        32 * 1024
    );
    assert_eq!(diagnostics["omitted_bytes"], 196_608);
}

#[test]
fn timeout_is_not_a_successful_timing_sample() {
    let output = fast(&["--once", "--json", "--timeout", "20ms", "sleep", "30"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json(&output)["invocations"][0]["observation"]["outcome"]["kind"],
        "timeout"
    );
}

#[test]
fn verify_runs_once_and_failure_aborts() {
    let output = fast(&[
        "--runs",
        "3",
        "--json",
        "--verify",
        "test \"$FAST_COMMAND_ID:$FAST_PHASE:$FAST_RUN_INDEX\" = 'A:production:1'; exit 7",
        "true",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let result = json(&output);
    assert_eq!(result["invocations"].as_array().expect("array").len(), 2);
    assert_eq!(result["invocations"][1]["role"], "verify");
}

#[test]
fn explicit_count_has_no_pilot_and_balances_candidates() {
    let output = fast(&[
        "--runs", "3", "--json", "--seed", "badc0ffe", "true", ":::", "true", ":::", "true",
    ]);
    assert!(output.status.success(), "{output:?}");
    let result = json(&output);
    let invocations = result["invocations"].as_array().expect("array");
    assert_eq!(invocations.len(), 9);
    for block in 0..3 {
        let mut candidates: Vec<_> = invocations
            .iter()
            .filter(|i| i["block"] == block)
            .map(|i| i["candidate"].as_u64().expect("index"))
            .collect();
        candidates.sort_unstable();
        assert_eq!(candidates, [0, 1, 2]);
    }
}

#[test]
fn stdin_is_null_even_when_fast_receives_a_pipe() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fast"))
        .args(["--once", "--json", "sh", "-c", "test -z \"$(cat)\""])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(b"not workload input")
        .expect("feed");
    let output = child.wait_with_output().expect("wait");
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn default_does_not_need_writable_working_or_home_directories() {
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .env_clear()
        .env("PATH", std::env::var_os("PATH").expect("PATH"))
        .env("HOME", "/proc")
        .env("TMPDIR", "/proc")
        .current_dir("/proc")
        .args(["--once", "--json", "--", "true"])
        .output()
        .expect("run");
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn check_with_insufficient_data_is_inconclusive() {
    let output = fast(&[
        "--runs", "2", "--check", "1%", "--json", "true", ":::", "true",
    ]);
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(json(&output)["exit_code"], 5);
}

#[test]
fn invalid_effort_and_options_are_usage_errors() {
    for args in [
        vec!["--once", "--runs", "3", "true"],
        vec!["--once", "--warmup", "1", "true"],
        vec!["--time", "nan", "true"],
        vec!["--margin", "NaN", "true"],
        vec!["--unknown", "true"],
    ] {
        assert_eq!(fast(&args).status.code(), Some(2), "{args:?}");
    }
}

#[test]
fn broken_pipe_does_not_panic() {
    let (read, write) = std::io::pipe().expect("output pipe");
    drop(read);
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .args(["--once", "--json", "--", "true"])
        .stdout(Stdio::from(write))
        .stderr(Stdio::piped())
        .output()
        .expect("closed output pipe");
    assert_eq!(output.status.code(), Some(141));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
}

#[test]
fn control_characters_are_escaped_in_human_output() {
    let output = fast(&[
        "--once",
        "--color",
        "never",
        "--",
        "true",
        "\u{1b}[2J\nfake",
    ]);
    assert!(output.status.success(), "{output:?}");
    assert!(!output.stdout.contains(&0x1B));
}

#[test]
fn interruption_returns_partial_json_after_cleanup() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fast"))
        .args([
            "--runs",
            "3",
            "--json",
            "sh",
            "-c",
            "kill -INT \"$PPID\"; sleep 30",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut text = String::new();
    child
        .stdout
        .take()
        .expect("stdout")
        .read_to_string(&mut text)
        .expect("read");
    let status = child.wait().expect("wait");
    assert_eq!(status.code(), Some(130));
    let result: Value = serde_json::from_str(&text).expect("partial JSON");
    assert_eq!(result["completed"], false);
}

#[test]
fn inherited_counters_include_synchronous_child_work() {
    let output = fast(&[
        "--once",
        "--json",
        "sh",
        "-c",
        "(:) & wait",
        ":::",
        "sh",
        "-c",
        "(i=0; while [ \"$i\" -lt 10000 ]; do i=$((i+1)); done) & wait",
    ]);
    assert!(output.status.success(), "{output:?}");
    let value = json(&output);
    let counter = |candidate: u64, name: &str| {
        value["invocations"]
            .as_array()
            .expect("invocations")
            .iter()
            .find(|invocation| invocation["candidate"] == candidate)
            .expect("candidate")["observation"]["counters"]
            .as_array()
            .expect("counters")
            .iter()
            .find(|counter| counter["name"] == name)
            .expect("event")
    };
    for name in ["cycles", "instructions"] {
        let baseline = counter(0, name);
        let child_work = counter(1, name);
        if baseline["raw"].is_null() || child_work["raw"].is_null() {
            eprintln!("{name} unavailable on this host: {baseline} {child_work}");
            continue;
        }
        let baseline_count = baseline["raw"]
            .as_str()
            .expect("count")
            .parse::<u64>()
            .expect("integer");
        let child_count = child_work["raw"]
            .as_str()
            .expect("count")
            .parse::<u64>()
            .expect("integer");
        assert!(baseline_count > 0);
        assert!(
            child_count > baseline_count.saturating_mul(2),
            "fork-without-exec work missing from {name}: {baseline} {child_work}"
        );
        assert_ne!(child_work["running_ns"], "0");
    }
}

#[test]
fn counters_exclude_prior_invocations_and_hooks() {
    let baseline = fast(&["--runs", "3", "--json", "--", "true"]);
    let prepared = fast(&[
        "--runs",
        "3",
        "--json",
        "--prepare",
        "i=0; while [ \"$i\" -lt 10000 ]; do i=$((i+1)); done",
        "--",
        "true",
    ]);
    assert!(baseline.status.success(), "{baseline:?}");
    assert!(prepared.status.success(), "{prepared:?}");
    let counts = |output: &Output| -> Option<Vec<u64>> {
        json(output)["invocations"]
            .as_array()?
            .iter()
            .filter(|invocation| invocation["role"] == "command")
            .map(|invocation| {
                invocation["observation"]["counters"]
                    .as_array()?
                    .iter()
                    .find(|counter| counter["name"] == "instructions")?["raw"]
                    .as_str()?
                    .parse()
                    .ok()
            })
            .collect()
    };
    let (Some(baseline), Some(prepared)) = (counts(&baseline), counts(&prepared)) else {
        eprintln!("Instruction counting unavailable; invocation-isolation check not exercised");
        return;
    };
    assert_eq!(baseline.len(), 3);
    assert_eq!(prepared.len(), 3);
    let smallest = *baseline.iter().min().expect("baseline runs");
    let largest = *baseline
        .iter()
        .chain(&prepared)
        .max()
        .expect("measured runs");
    assert!(smallest > 0);
    assert!(
        largest < smallest.saturating_mul(2),
        "counter includes earlier commands or the much larger preparation hook: {baseline:?} {prepared:?}"
    );
}

#[test]
fn input_file_is_reopened_for_each_invocation() {
    let output = fast(&[
        "--runs",
        "3",
        "--json",
        "--input",
        "/proc/version",
        "sh",
        "-c",
        "read -r first rest; test \"$first\" = Linux",
    ]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(json(&output)["analysis"]["blocks"], 3);
}

#[test]
fn no_enoexec_shell_fallback_for_exact_argv() {
    let output = fast(&["--once", "--json", "--", "/proc/version"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json(&output)["invocations"][0]["observation"]["outcome"]["kind"],
        "exec_error"
    );
}

#[test]
fn command_signal_death_remains_distinct_from_exit_status() {
    let output = fast(&["--once", "--json", "sh", "-c", "kill -KILL $$"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json(&output)["invocations"][0]["observation"]["outcome"]["signal"],
        9
    );
}

#[test]
fn surviving_descendant_aborts_repetition_without_waiting_for_stderr_eof() {
    let output = fast(&[
        "--runs",
        "3",
        "--json",
        "sh",
        "-c",
        "sleep 30 & echo $! >&2",
    ]);
    let result = json(&output);
    let diagnostic = &result["invocations"][0]["observation"]["diagnostics"];
    let bytes: Vec<u8> = diagnostic["first"]
        .as_array()
        .expect("capture")
        .iter()
        .map(|v| v.as_u64().expect("byte") as u8)
        .collect();
    let pid: i32 = String::from_utf8(bytes)
        .expect("PID text")
        .trim()
        .parse()
        .expect("descendant PID");
    // SAFETY: the fixture supplied this still-running descendant's PID; SIGKILL
    // cleans up the intentionally persistent test workload after inspecting fast.
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(result["invocations"].as_array().expect("array").len(), 1);
    assert_eq!(
        result["invocations"][0]["observation"]["outcome"]["kind"],
        "background_descendants"
    );
    assert_eq!(diagnostic["pipe_still_open"], true);
}

#[test]
fn details_does_not_change_the_number_of_executions() {
    let plain = fast(&[
        "--runs", "2", "--json", "--seed", "1234", "true", ":::", "true",
    ]);
    let detailed = fast(&[
        "--runs",
        "2",
        "--json",
        "--details",
        "--seed",
        "1234",
        "true",
        ":::",
        "true",
    ]);
    let order = |output: &Output| {
        json(output)["invocations"]
            .as_array()
            .expect("array")
            .iter()
            .map(|i| (i["block"].clone(), i["candidate"].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(order(&plain), order(&detailed));
    assert_eq!(order(&plain).len(), 4);
}

#[test]
fn prepare_stdout_is_captured_as_diagnostics() {
    let output = fast(&[
        "--once",
        "--json",
        "--prepare",
        "printf hook_failure; exit 8",
        "true",
    ]);
    assert_eq!(output.status.code(), Some(1));
    let result = json(&output);
    let bytes: Vec<_> = result["invocations"][0]["observation"]["diagnostics"]["first"]
        .as_array()
        .expect("capture")
        .iter()
        .map(|v| v.as_u64().expect("byte") as u8)
        .collect();
    assert_eq!(bytes, b"hook_failure");
    assert_eq!(result["invocations"].as_array().expect("array").len(), 1);
}

#[test]
fn stopped_command_is_killed_by_timeout() {
    let output = fast(&[
        "--once",
        "--json",
        "--timeout",
        "10ms",
        "sh",
        "-c",
        "kill -STOP $$",
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json(&output)["invocations"][0]["observation"]["outcome"]["kind"],
        "timeout"
    );
}

#[test]
fn explicit_color_overrides_no_color_but_json_remains_plain() {
    for (format, expect_color) in [("--details", true), ("--json", false)] {
        let output = Command::new(env!("CARGO_BIN_EXE_fast"))
            .env("NO_COLOR", "1")
            .env("TERM", "dumb")
            .args(["--once", "--color", "always", format, "true"])
            .output()
            .expect("run");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stdout.contains(&0x1B), expect_color);
    }
}

#[test]
fn explicit_color_applies_to_help_without_interpreting_option_payloads() {
    for (args, colored) in [
        (vec!["--help", "--color", "always"], true),
        (vec!["--color=always", "--help"], true),
        (vec!["--color", "never", "--help"], false),
        (vec!["--prepare=--color=always", "--help"], false),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_fast"))
            .env("NO_COLOR", "1")
            .args(args)
            .output()
            .expect("help");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stdout.contains(&0x1B), colored);
    }
}

#[test]
fn color_environment_forces_piped_color_unless_disabled() {
    for (args, no_color, colored) in [
        (vec!["--once", "true"], false, true),
        (vec!["--help"], false, true),
        (vec!["--once", "true"], true, false),
        (vec!["--once", "--color", "never", "true"], false, false),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fast"));
        command
            .env("FORCE_COLOR", "1")
            .env("TERM", "dumb")
            .env_remove("CLICOLOR_FORCE")
            .env_remove("NO_COLOR");
        if no_color {
            command.env("NO_COLOR", "1");
        }
        let output = command.args(&args).output().expect("run");
        assert!(output.status.success(), "{output:?}");
        assert_eq!(output.stdout.contains(&0x1B), colored, "{args:?}");
    }
}

#[test]
fn legend_wraps_between_arguments() {
    let long = "x".repeat(60);
    let output = fast(&["--once", "--color", "never", "true", &long, &long]);
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 report");
    let quoted = format!("'{long}'");
    assert_eq!(
        stdout.lines().filter(|line| line.contains(&quoted)).count(),
        2,
        "{stdout}"
    );
    assert!(!stdout.contains("0.000 ns"), "{stdout}");
}

#[test]
fn malformed_jobserver_is_rejected_before_workload() {
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .env("MAKEFLAGS", "--jobserver-auth=999998,999999")
        .args(["--once", "--json", "--", "true"])
        .output()
        .expect("run");
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("jobserver"));
}

#[test]
fn inherited_jobserver_descriptors_keep_their_numbers() {
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    let mut fds = [0; 2];
    // SAFETY: pipe writes two new inheritable descriptors, which OwnedFd then owns.
    let (read, write) = unsafe {
        assert_eq!(libc::pipe(fds.as_mut_ptr()), 0, "jobserver pipe");
        (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1]))
    };
    let auth = format!(
        "--jobserver-auth={},{}",
        read.as_raw_fd(),
        write.as_raw_fd()
    );
    let expression = format!(
        "test -p /proc/self/fd/{} && test -p /proc/self/fd/{}",
        read.as_raw_fd(),
        write.as_raw_fd()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_fast"))
        .env("MAKEFLAGS", auth)
        .args(["--once", "--json", "sh", "-c", &expression])
        .output()
        .expect("run");
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn activity_is_attached_to_one_measured_invocation() {
    let output = fast(&["--runs", "2", "--json", "sh", "-c", "sleep 0.3 & wait"]);
    assert!(output.status.success(), "{output:?}");
    let result = json(&output);
    let invocations = result["invocations"].as_array().expect("observations");
    let first = &invocations[0]["observation"]["activity"];
    assert!(!first["tasks"].as_array().expect("tasks").is_empty());
    assert!(first["clock_ticks_per_second"].as_u64().expect("ticks") > 0);
    assert!(
        invocations[1]["observation"]["activity"]["tasks"]
            .as_array()
            .expect("tasks")
            .is_empty()
    );
    for task in first["tasks"].as_array().expect("tasks") {
        assert!(task["start_ticks"].as_u64().expect("start") > 0);
        if task["pid"] != task["tid"] {
            assert!(task["maximum_observed_rss_bytes"].is_null());
        }
    }
}

#[test]
fn signal_mask_and_sigpipe_are_restored_for_the_workload() {
    let output = fast(&["--once", "--json", "sh", "-c", "kill -PIPE $$; exit 17"]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        json(&output)["invocations"][0]["observation"]["outcome"]["signal"],
        libc::SIGPIPE
    );
}

#[test]
fn automatic_planning_reports_pilot_only_without_extra_execution() {
    let output = fast(&["--time", "1ns", "--json", "true"]);
    assert!(output.status.success(), "{output:?}");
    let result = json(&output);
    assert_eq!(
        result["invocations"]
            .as_array()
            .expect("observations")
            .len(),
        1
    );
    assert_eq!(result["invocations"][0]["phase"], "pilot");
    assert_eq!(result["stop_reason"], "pilot_only");
    assert_eq!(result["planned_blocks"], 0);
}

#[test]
fn pilot_only_report_keeps_the_resource_overview() {
    let output = fast(&["--time", "1ns", "--", "true"]);
    assert!(output.status.success(), "{output:?}");
    let report = String::from_utf8(output.stdout).expect("report");
    assert!(report.contains("Pilot resources"), "{report}");
    assert!(report.contains("CPU time"), "{report}");
    assert!(report.contains("RSS peak/launch"), "{report}");
}

#[test]
fn closed_stdin_does_not_alias_internal_descriptors() {
    use std::os::unix::process::CommandExt as _;
    let mut command = Command::new(env!("CARGO_BIN_EXE_fast"));
    command.args(["--once", "--json", "sh", "-c", "test -z \"$(cat)\""]);
    // SAFETY: the child only closes a descriptor before exec; no allocation or locks.
    unsafe {
        command.pre_exec(|| {
            libc::close(0);
            Ok(())
        });
    }
    let output = command.output().expect("run with closed stdin");
    assert!(output.status.success(), "{output:?}");
}
