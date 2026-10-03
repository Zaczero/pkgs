use std::hint::black_box;
use std::io::{self, Write as _};
use std::process::{Command, ExitCode};
use std::thread;
use std::time::Duration;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Native fixtures for fast's observer measurements")]
struct Options {
    #[arg(long, default_value_t = 0)]
    iterations: u64,
    #[arg(long, default_value_t = 1)]
    threads: usize,
    #[arg(long, default_value_t = 0)]
    children: usize,
    #[arg(long, default_value_t = 0)]
    sleep_ms: u64,
    #[arg(long, default_value_t = 0)]
    stderr_bytes: usize,
}

fn main() -> io::Result<ExitCode> {
    let options = Options::parse();
    let mut children = Vec::new();
    for _ in 0..options.children {
        children.push(
            Command::new(std::env::current_exe()?)
                .args([
                    "--iterations",
                    &options.iterations.to_string(),
                    "--threads",
                    &options.threads.to_string(),
                ])
                .spawn()?,
        );
    }
    thread::scope(|scope| {
        for _ in 0..options.threads {
            scope.spawn(|| {
                let mut value = black_box(0x1234_5678_9ABC_DEF0_u64);
                for _ in 0..options.iterations {
                    value = value
                        .wrapping_mul(0xD134_2543_DE82_EF95)
                        .rotate_left(7)
                        .wrapping_add(1);
                }
                black_box(value);
                thread::sleep(Duration::from_millis(options.sleep_ms));
            });
        }
    });
    let bytes = [b'x'; 8192];
    let mut remaining = options.stderr_bytes;
    while remaining > 0 {
        let count = remaining.min(bytes.len());
        io::stderr().write_all(bytes.get(..count).expect("bounded write"))?;
        remaining = remaining.wrapping_sub(count);
    }
    for mut child in children {
        if !child.wait()?.success() {
            return Ok(ExitCode::FAILURE);
        }
    }
    Ok(ExitCode::SUCCESS)
}
