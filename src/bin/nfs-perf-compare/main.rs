mod cli;
mod stats;

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    match cli::parse_args(env::args().skip(1)) {
        Ok(config) => {
            println!("{}", config.suite.name());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}
