// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Forwards one systemd journal instance into Sparrow.
//!
//! Usage: `sparrow-journal --config FILE system|user`.

use sparrow_client::Console;
use sparrow_server::journal_forward::{Config, Instance, run};
use std::{error::Error, ffi::OsString, path::PathBuf};

const USAGE: &str = "usage: sparrow-journal --config FILE system|user";

/// The configuration path and the instance named on the command line.
fn arguments(
    arguments: impl Iterator<Item = OsString>,
) -> Result<(PathBuf, Instance), Box<dyn Error>> {
    let mut config = None;
    let mut instance = None;
    let mut arguments = arguments;
    while let Some(argument) = arguments.next() {
        if argument == "--config" {
            config = Some(PathBuf::from(arguments.next().ok_or(USAGE)?));
        } else {
            let text = argument.to_str().ok_or(USAGE)?;
            instance = Some(Instance::parse(text)?);
        }
    }
    match (config, instance) {
        (Some(config), Some(instance)) => Ok((config, instance)),
        _ => Err(USAGE.into()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (path, instance) = arguments(std::env::args_os().skip(1))?;
    let _console = Console::install_auto("sparrow.journal").await?;
    let config = Config::load(&path)?;
    run(&config, instance).await?;
    Ok(())
}
