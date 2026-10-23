mod cli;
mod experiment;
mod format;
mod report;
use std::ffi::OsString;
use std::io;
use std::process::ExitCode;

use clap::{CommandFactory as _, FromArgMatches as _};
use fast::{random, runner, statistics};

fn main() -> ExitCode {
    let cli = {
        let raw: Vec<_> = std::env::args_os().collect();
        match parse_args(&raw) {
            Ok(cli) => cli,
            Err(error) => return output_status(error.print(), error.exit_code() as u8),
        }
    };
    match experiment::run(cli) {
        Ok((result, options)) => {
            let code = result.exit_code;
            output_status(report::print(&result, &options), code)
        },
        Err(error) => {
            let (code, error) = match error {
                experiment::Failure::Usage(error) => (2, error),
                experiment::Failure::Collector(error) => (3, error),
            };
            eprintln!("fast: {error}");
            ExitCode::from(code)
        },
    }
}

fn parse_args(raw: &[OsString]) -> Result<cli::Cli, clap::Error> {
    let mut command = cli::Cli::command();
    match command.try_get_matches_from_mut(raw) {
        Ok(matches) => cli::Cli::from_arg_matches(&matches),
        Err(error) => {
            // Parse presentation options without help/version terminating parsing.
            let matches = command
                .clone()
                .mut_arg("help", |arg| arg.action(clap::ArgAction::SetTrue))
                .mut_arg("version", |arg| arg.action(clap::ArgAction::SetTrue))
                .ignore_errors(true)
                .try_get_matches_from(raw)?;
            let color = match report::resolve_color(
                matches
                    .get_one::<cli::Color>("color")
                    .copied()
                    .unwrap_or(cli::Color::Auto),
            ) {
                cli::Color::Always => clap::ColorChoice::Always,
                cli::Color::Never => clap::ColorChoice::Never,
                cli::Color::Auto => clap::ColorChoice::Auto,
            };
            Err(error.with_cmd(&command.color(color)))
        },
    }
}

fn output_status(output: io::Result<()>, code: u8) -> ExitCode {
    match output {
        Ok(()) => ExitCode::from(code),
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => ExitCode::from(141),
        Err(error) => {
            eprintln!("fast: cannot write output: {error}");
            ExitCode::from(3)
        },
    }
}
