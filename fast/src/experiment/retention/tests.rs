use std::collections::VecDeque;

use clap::Parser as _;

use super::{Storage, bounded_text};
use crate::cli::Cli;
use crate::experiment::{Collected, Invocation, Phase, Role};
use crate::runner::{Activity, Counter, Diagnostics, Observation, Outcome, Sample, Task};

fn observation() -> Observation {
    Observation {
        elapsed_ns: 123,
        release_ns: 1,
        post_exit_drain_ns: 2,
        descendants_survived: false,
        cleanup_incomplete: false,
        user_ns: 3,
        system_ns: 4,
        max_rss_bytes: 5,
        launch_max_rss_bytes: Some(6),
        minor_faults: 7,
        major_faults: 8,
        voluntary_switches: 9,
        involuntary_switches: 10,
        outcome: Outcome::Success,
        counters: std::iter::repeat_with(|| Counter {
            name: "fixture",
            raw: None,
            enabled_ns: None,
            running_ns: None,
            unavailable: Some("e".repeat(256)),
        })
        .take(4)
        .collect(),
        activity: Activity {
            pid_namespace: Some("n".repeat(256)),
            ..Activity::default()
        },
        diagnostics: Diagnostics::default(),
    }
}

fn invocation(run: usize, outcome: Collected) -> Invocation {
    Invocation {
        candidate: 0,
        phase: Phase::Production,
        run,
        position: 0,
        role: Role::Command,
        started_ns: 321,
        outcome,
    }
}

#[test]
fn admitted_plan_keeps_failure_when_task_detail_exhausts_allowance() {
    let automatic = Cli::parse_from(["fast", "true"]);
    let runs = Storage::new(&automatic, 1, 4)
        .expect("automatic admission")
        .runs();
    let fixed = Cli::parse_from(["fast", "--runs", &runs.to_string(), "true"]);
    let mut storage = Storage::new(&fixed, 1, 4).expect("largest fixed plan");
    let too_large = Cli::parse_from(["fast", "--runs", &(runs + 1).to_string(), "true"]);
    assert!(Storage::new(&too_large, 1, 4).is_err());
    let mut records = Vec::new();
    storage.reserve(&mut records, runs);
    let mut sampled = Vec::new();
    let task_name_capacity = storage.detail_remaining / 3;
    for run in [0, 1, runs - 1] {
        let mut reading = observation();
        if run < 2 {
            reading.activity.tasks = (0..2)
                .map(|tid| Task {
                    pid: 1,
                    tid,
                    process_start_ticks: 11,
                    start_ticks: 12,
                    parent_pid: 2,
                    last_parent_pid: 2,
                    name: {
                        let mut name = String::with_capacity(task_name_capacity);
                        name.push('t');
                        name
                    },
                    name_changes: 0,
                    observations: 1,
                    maximum_observed_rss_bytes: Some(4096),
                    samples: vec![Sample {
                        elapsed_ns: 13,
                        cpu_ticks: 14,
                    }],
                })
                .collect();
        }
        if run == runs - 1 {
            reading.outcome = Outcome::Exit { code: 23 };
            reading.diagnostics.first = vec![b'h'; 32 * 1024];
            reading.diagnostics.last = VecDeque::from(vec![b't'; 32 * 1024]);
            reading.diagnostics.omitted_bytes = 17;
        }
        storage.retain(&mut reading);
        if run < 2 {
            sampled.push(reading.activity.tasks.len());
        }
        records.push(invocation(run, Collected::Observation(reading)));
    }
    let summary = storage.summary(&records, &"exit 23".into());
    eprintln!(
        "inline invocation={} counter={} task={} sample={}; runs={runs}; task counts={sampled:?}; retained={} allowance={}",
        size_of::<Invocation>(),
        size_of::<Counter>(),
        size_of::<Task>(),
        size_of::<Sample>(),
        summary.retained_bytes,
        summary.allowance_bytes,
    );
    assert_eq!(summary.allowance_bytes, 128 * 1024 * 1024);
    assert!(summary.retained_bytes <= 128 * 1024 * 1024);
    assert!(summary.task_detail_omitted);
    assert_eq!(sampled[0], 2);
    assert!(sampled[1] < 2);
    assert_eq!(records.len(), 3);
    for (run, record) in [0, 1, runs - 1].into_iter().zip(&records) {
        assert_eq!(record.run, run);
    }
    let last = records
        .last()
        .expect("terminal record")
        .observation()
        .expect("collected");
    assert!(matches!(last.outcome, Outcome::Exit { code: 23 }));
    assert_eq!(last.diagnostics.first, vec![b'h'; 32 * 1024]);
    assert_eq!(last.diagnostics.last, VecDeque::from(vec![b't'; 32 * 1024]));
    assert_eq!(last.diagnostics.omitted_bytes, 17);
}

#[test]
fn bounded_text_preserves_utf8_and_discloses_truncation() {
    let text = bounded_text("界".repeat(1000));
    assert!(text.ends_with(" [truncated]"));
    assert!(text.starts_with("界界"));
    assert!(text.capacity() <= 256);
    let mut short = String::with_capacity(4096);
    short.push_str("permission denied");
    let text = bounded_text(short);
    assert_eq!(text, "permission denied");
    assert_eq!(text.capacity(), text.len());
}

#[test]
fn completed_outcomes_preserve_schema_two_fields() {
    for outcome in [
        Collected::Observation(observation()),
        Collected::CollectorError("collector unavailable".into()),
    ] {
        let record = invocation(19, outcome);
        let value = serde_json::to_value(&record).expect("invocation JSON");
        assert_eq!(value["run"], 19);
        assert_eq!(value["started_ns"], "321");
        match record.outcome {
            Collected::Observation(_) => {
                assert_eq!(value["observation"]["elapsed_ns"], "123");
                assert!(value["error"].is_null());
            },
            Collected::CollectorError(_) => {
                assert!(value["observation"].is_null());
                assert_eq!(value["error"], "collector unavailable");
            },
        }
    }
}
