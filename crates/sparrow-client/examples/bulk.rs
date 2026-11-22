// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Import a completed log file, or read stdin incrementally when FILE is omitted.
use sparrow_client::{
    Endpoint, Format,
    bulk::{BulkConfig, BulkImporter},
};
use std::{io, path::PathBuf};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [endpoint, directory, job_id, rest @ ..] = args.as_slice() else {
        return Err("usage: bulk ENDPOINT ABSOLUTE_SPOOL JOB_ID [FILE]".into());
    };
    let bulk = BulkImporter::new(BulkConfig::new(
        Endpoint::parse(endpoint)?,
        PathBuf::from(directory),
    ))?;
    let mut job = bulk.open(job_id, "import", Format::Log).await?;
    match rest {
        [file] => job.stage_file(file).await?,
        [] => {
            let p = job.progress().borrow().clone();
            if !p.eof {
                if p.staged > 0 {
                    return Err(io::Error::other(format!("resume stdin from source offset {} with stage_stream, or pass the original file",p.staged)).into());
                }
                job.stage_stream(0, tokio::io::stdin()).await?;
            }
        }
        _ => return Err("bulk accepts at most one completed file".into()),
    }
    let result = job.finish().await?;
    println!(
        "{} {} {} bytes",
        result.store_id, result.artifact_id, result.bytes
    );
    Ok(())
}
