use std::path::PathBuf;

use chrono::{DateTime, Local};
use clap::Parser;

use terminusdb_10_to_11_escape_fixup::convert_store::convert_store;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    from: String,
    to: String,
    date: String,
    /// The workdir to store mappings in
    #[arg(short = 'w', long = "workdir")]
    workdir: Option<String>,
    /// Convert the store assuming all values are strings
    /// Keep going with other layers if a layer does not convert
    #[arg(short = 'c', long = "continue")]
    keep_going: bool,
    /// Verbose reporting
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
    /// Replace original directory with converted directory
    #[arg(short = 'r', long = "replace")]
    replace: bool,
    /// Cleanup work directory after successful run
    #[arg(short = 'k', long = "clean")]
    clean: bool,
}


#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let Cli{from, to, workdir, keep_going, verbose, replace, clean, ..} = Cli::parse();
    //let date_converted = DateTime::parse_from_rfc3339(&cli.date).unwrap().naive_local().and_local_timezone(Local).unwrap();
    let default_workdir = format!("{to}/.workdir");
    convert_store(
        &from,
        &to,
        workdir.as_deref().unwrap_or(&default_workdir),
        keep_going,
        verbose,
        replace,
        clean,
    )
        .await.unwrap();

}
