//! continuum-runtime CLI.
//!
//! Subcommands:
//! - `run`      — enrol patches, run the trigger loop to quiescence
//! - `patch`    — child mode: run one patch process (spawned by `run`)
//! - `verify`   — positional re-derivation over a run dir
//! - `playback` — diff two runs' streams event for event
//!
//! Exit codes: 0 success; 1 the operation found a failure (a verify
//! finding, a playback divergence, a refused enrolment); 2 usage/IO
//! trouble.

use std::path::PathBuf;
use std::process::ExitCode;

use continuum_runtime::engine::r#loop::{self as engine_loop, LoopConfig};
use continuum_runtime::engine::playback;
use continuum_runtime::nodes::patch;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        usage();
        return ExitCode::from(2);
    };
    let rest = &args[1..];
    let result = match command.as_str() {
        "run" => cmd_run(rest),
        "patch" => cmd_patch(rest),
        "verify" => cmd_verify(rest),
        "playback" => cmd_playback(rest),
        _ => {
            eprintln!("unknown command: {command}");
            usage();
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(code) => code,
        Err(message) => {
            eprintln!("continuum-runtime: {message}");
            ExitCode::from(2)
        }
    }
}

fn usage() {
    eprintln!(
        "usage:\n  \
           continuum-runtime run --run-dir DIR --input FILE --manifest-dir DIR [--step-delay-ms N] [--delivery-timeout-ms N]\n  \
           continuum-runtime patch --id ID --run-dir DIR [--step-delay-ms N]\n  \
           continuum-runtime verify --run-dir DIR [--input FILE]\n  \
           continuum-runtime playback --a DIR --b DIR"
    );
}

/// Minimal `--flag value` parser: returns `Some(value)` for the first
/// occurrence of the flag. Hand-rolled — the CLI is four fixed shapes
/// and a parser crate would be the only reason to depend on one.
fn flag(rest: &[String], name: &str) -> Option<String> {
    rest.windows(2)
        .find(|w| w[0] == name)
        .map(|w| w[1].clone())
}

fn require(rest: &[String], name: &str) -> Result<String, String> {
    flag(rest, name).ok_or_else(|| format!("missing required flag {name}"))
}

fn cmd_run(rest: &[String]) -> Result<ExitCode, String> {
    let run_dir = PathBuf::from(require(rest, "--run-dir")?);
    let input = PathBuf::from(require(rest, "--input")?);
    let manifest_dir = PathBuf::from(require(rest, "--manifest-dir")?);
    let config = LoopConfig {
        run_dir,
        input_path: input,
        manifest_dir,
        step_delay_ms: flag(rest, "--step-delay-ms").and_then(|v| v.parse().ok()).unwrap_or(0),
        delivery_timeout_ms: flag(rest, "--delivery-timeout-ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000),
    };
    let summary = engine_loop::run(&config)?;
    engine_loop::print_summary(&summary);
    Ok(ExitCode::SUCCESS)
}

fn cmd_patch(rest: &[String]) -> Result<ExitCode, String> {
    let id = require(rest, "--id")?;
    let run_dir = PathBuf::from(require(rest, "--run-dir")?);
    let step_delay_ms = flag(rest, "--step-delay-ms").and_then(|v| v.parse().ok()).unwrap_or(0);
    let code = patch::run_patch(&id, &run_dir, step_delay_ms);
    match code {
        0 => Ok(ExitCode::SUCCESS),
        n => Ok(ExitCode::from(n as u8)),
    }
}

fn cmd_verify(rest: &[String]) -> Result<ExitCode, String> {
    let run_dir = PathBuf::from(require(rest, "--run-dir")?);
    let input = flag(rest, "--input").map(PathBuf::from);
    let report = playback::verify(&run_dir, input.as_deref())?;
    println!("verify: {} records checked", report.records);
    if report.ok() {
        println!("verify: PASS (no findings)");
        Ok(ExitCode::SUCCESS)
    } else {
        for finding in &report.findings {
            match finding.pos {
                Some(pos) => println!(
                    "verify: FAIL [{}] position {}: {}",
                    finding.check, pos, finding.detail
                ),
                None => println!("verify: FAIL [{}]: {}", finding.check, finding.detail),
            }
        }
        println!("verify: FAIL ({} findings)", report.findings.len());
        Ok(ExitCode::from(1))
    }
}

fn cmd_playback(rest: &[String]) -> Result<ExitCode, String> {
    let a = PathBuf::from(require(rest, "--a")?);
    let b = PathBuf::from(require(rest, "--b")?);
    let report = playback::diff_streams(&a, &b)?;
    if report.identical {
        println!(
            "playback: identical, {} records (A={}, B={})",
            report.records_a, report.records_a, report.records_b
        );
        Ok(ExitCode::SUCCESS)
    } else {
        println!(
            "playback: DIVERGE at position {} (A={}, B={})",
            report.first_divergence.unwrap_or(0),
            report.records_a,
            report.records_b
        );
        if let Some(detail) = report.detail {
            println!("playback: {detail}");
        }
        Ok(ExitCode::from(1))
    }
}
