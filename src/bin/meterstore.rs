//! The `meterstore` command-line tool.
//!
//! Everything is in [`meterstore::cli`]; this is the entry point that runs it,
//! so the commands themselves stay unit-testable rather than living in a binary
//! nothing can call.

fn main() -> std::process::ExitCode {
    meterstore::cli::main()
}
