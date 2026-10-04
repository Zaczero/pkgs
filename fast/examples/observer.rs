use std::ffi::OsString;
use std::io;
use std::time::Duration;

use clap::Parser;
use fast::random::Random;
use fast::{runner, statistics};
use serde_json::json;

#[derive(Parser)]
#[command(about = "Paired observer on/off experiment using fast's production runner")]
struct Options {
    #[arg(long, default_value_t = 100)]
    runs: usize,
    #[arg(last = true, required = true)]
    command: Vec<OsString>,
}

fn main() -> io::Result<()> {
    let options = Options::parse();
    runner::check_jobserver()?;
    let command = runner::Command {
        argv: options.command,
    };
    let mut counter_policy = None;
    let mut random = Random::new(42);
    let mut rows = Vec::with_capacity(options.runs);
    let mut observations = Vec::with_capacity(options.runs.wrapping_mul(2));
    for _ in 0..options.runs {
        let mut row = [0; 2];
        let order = if random.next_u64() >> 63 == 1 {
            [false, true]
        } else {
            [true, false]
        };
        for enabled in order {
            let mut runner = runner::Runner::new(enabled)?;
            if enabled && counter_policy.is_none() {
                counter_policy =
                    Some(serde_json::to_value(runner.counter_policy()).map_err(io::Error::other)?);
            }
            let observation = runner.invoke(
                &command,
                None,
                Some(Duration::from_secs(30)),
                &[],
                runner::InvocationKind::Command,
            )?;
            if !observation.outcome.success() {
                return Err(io::Error::other(format!("{:?}", observation.outcome)));
            }
            if enabled {
                row[1] = observation.elapsed_ns;
            } else {
                row[0] = observation.elapsed_ns;
            }
            observations.push((enabled, observation));
        }
        rows.push(row.to_vec());
    }
    let analysis = statistics::analyze(&rows, None);
    let recommendation = statistics::precision_count(&rows);
    println!(
        "{}",
        json!({
            "analysis": analysis,
            "counter_policy":counter_policy,
            "observations":observations,
            "precision_count":recommendation,
        })
    );
    Ok(())
}
