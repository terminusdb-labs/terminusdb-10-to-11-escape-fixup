use std::path::PathBuf;

use chrono::{DateTime, Local};
use clap::Parser;
use terminusdb_10_to_11_escape_fixup::convert_store;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    from: String,
    to: String,
    date: String
}


#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();
    let date_converted = DateTime::parse_from_rfc3339(&cli.date).unwrap().naive_local().and_local_timezone(Local).unwrap();

    convert_store(PathBuf::from(cli.from), PathBuf::from(cli.to), date_converted).await.unwrap();
}
