#[path = "src/cli.rs"]
mod cli;

use std::env;
use std::fs::File;
use std::path::PathBuf;

use clap::CommandFactory as _;
use clap_complete::{Shell, generate_to};

fn main() {
    println!("cargo:rerun-if-changed=src/cli.rs");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo supplies OUT_DIR"));
    let mut command = cli::Cli::command();
    for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
        generate_to(shell, &mut command, "fast", &output).expect("write completion");
    }
    clap_mangen::Man::new(command)
        .render(&mut File::create(output.join("fast.1")).expect("create manual"))
        .expect("write manual");
}
