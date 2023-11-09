use std::{env, process::Command};

use anyhow::{Context, Result};

fn main() {
    if let Err(e) = try_main() {
        eprintln!("{:?}", e);
        std::process::exit(-1);
    }
}

fn try_main() -> Result<()> {
    let task = env::args().nth(1);
    let arg = env::args().nth(2).unwrap_or("".to_string());
    match task.as_deref() {
        Some("build") => build()?,
        Some("build-wasm") => build_wasm(&arg)?,
        Some("sim-tests") => sim_tests(&arg)?,
        Some("test") => run_tests(&arg)?,
        _ => print_help(),
    }
    Ok(())
}

fn print_help() {
    eprintln!(
        "Tasks:

build                  builds all languages 
build-wasm             builds the wasm components in wasm32-unknown-unknown
tests                  runs the entire libsql test suite using nextest
sim-tests <test name>  runs the libsql-server simulation test suite
"
    )
}

fn build_wasm(_arg: &str) -> Result<()> {
    run_cargo(&[
        "check",
        "-p",
        "libsql",
        "--target",
        "wasm32-unknown-unknown",
        "--no-default-features",
        "--features",
        "cloudflare",
    ])?;

    Ok(())
}

fn run_tests(arg: &str) -> Result<()> {
    println!("installing nextest");
    run_cargo(&["install", "cargo-nextest"])?;
    println!("running nextest run");
    run_cargo(&["nextest", "run", arg])?;

    Ok(())
}

fn sim_tests(arg: &str) -> Result<()> {
    run_cargo(&["test", "--test", "tests", arg])?;

    Ok(())
}

fn build() -> Result<()> {
    run_libsql_sqlite3("./configure")?;
    run_libsql_sqlite3("make")?;

    Ok(())
}

fn run_cargo(cmd: &[&str]) -> Result<()> {
    let mut out = Command::new("cargo").args(cmd).spawn().context("spawn")?;

    let exit = out.wait().context("wait")?;

    if !exit.success() {
        anyhow::bail!("non 0 exit code: {}", exit);
    }

    Ok(())
}

fn run_libsql_sqlite3(cmd: &str) -> Result<()> {
    let mut out = Command::new(cmd).current_dir("libsql-sqlite3").spawn()?;

    let exit = out.wait()?;

    if !exit.success() {
        anyhow::bail!("non 0 exit code: {}", exit);
    }

    Ok(())
}
