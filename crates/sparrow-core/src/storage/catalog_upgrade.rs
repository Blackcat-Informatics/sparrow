// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One bounded, durable catalog pager for startup index migrations.

use super::{Catalog, Error, json, parse, read_field};
use rocksdb::{DB, Direction, IteratorMode, WriteBatch, WriteOptions};

type Publish = fn(&DB, &str, &[u8], usize, &mut WriteBatch) -> Result<(), Error>;
type PageAdmission<'a> = Option<&'a dyn Fn(&WriteBatch) -> Result<(), Error>>;

/// Encoded input plus the shared 64x catalog JSON decoding allowance.
pub(super) fn json_charge(bytes: &[u8], maximum: usize) -> Result<usize, Error> {
    let charge = bytes.len().saturating_mul(65);
    if charge > maximum {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "catalog JSON exceeds migration admission",
        ));
    }
    Ok(charge)
}

pub(super) struct Job {
    pub checkpoint: &'static str,
    pub source: &'static str,
    pub target: &'static [u8],
    pub page_bytes: usize,
    pub page_entries: usize,
    pub charge: fn(&[u8]) -> Result<usize, Error>,
    pub publish: Publish,
}

#[derive(Default)]
pub(super) struct Progress {
    pub after: Option<String>,
    pub complete: bool,
}

impl Catalog for Progress {
    fn encode(&self) -> json::Value {
        json::Object::new()
            .with("after", self.after.clone())
            .with("complete", self.complete)
            .into()
    }

    fn decode(value: &json::Value) -> Result<Self, Error> {
        let progress = Self {
            after: read_field(value, "after")?,
            complete: read_field(value, "complete")?,
        };
        if progress.after.as_ref().is_some_and(|key| {
            (![
                "artifact:",
                "record:",
                "rdf:graph:artifact:",
                "rdf:graph:record:",
            ]
            .iter()
            .any(|prefix| key.starts_with(prefix)))
                || key.len() > 128
        }) || (progress.complete && progress.after.is_some())
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid catalog index upgrade checkpoint",
            ));
        }
        Ok(progress)
    }
}

pub(super) fn run(db: &DB, job: &Job, admit: PageAdmission<'_>) -> Result<(), Error> {
    loop {
        let bytes = db.get(job.checkpoint)?.ok_or_else(|| {
            Error::new(
                "STORAGE_FAILURE",
                format!(
                    "required {} state missing; restore the original catalog",
                    job.checkpoint
                ),
            )
        })?;
        if bytes.len() > 1024 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "catalog upgrade checkpoint exceeds its bound",
            ));
        }
        let progress = Progress::decode(&parse(&bytes)?)?;
        if progress.complete {
            return Ok(());
        }
        step(db, job, progress, job.page_entries, admit)?;
    }
}

pub(super) fn seed(db: &DB, job: &Job, batch: &mut WriteBatch) -> Result<(), Error> {
    let present = match db.prefix_iterator(job.target).next() {
        Some(row) => row?.0.starts_with(job.target),
        None => false,
    };
    if db.get(job.checkpoint)?.is_some() || present {
        return Err(Error::new(
            "STORAGE_FAILURE",
            format!("legacy store contains unfenced {} state", job.checkpoint),
        ));
    }
    super::put(batch, job.checkpoint, &Progress::default());
    Ok(())
}

pub(super) fn commit(db: &DB, batch: WriteBatch) -> Result<(), Error> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    db.write_opt(batch, &options)?;
    Ok(())
}

pub(super) fn step(
    db: &DB,
    job: &Job,
    mut progress: Progress,
    maximum: usize,
    admit: PageAdmission<'_>,
) -> Result<(), Error> {
    if progress
        .after
        .as_ref()
        .is_some_and(|key| !key.starts_with(job.source))
    {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "catalog checkpoint belongs to another index",
        ));
    }
    let start = progress.after.clone();
    let key = start.as_deref().unwrap_or(job.source);
    let mut batch = WriteBatch::default();
    let mut remaining = job.page_bytes;
    let mut visited = 0;
    progress.complete = true;
    for entry in db.iterator(IteratorMode::From(key.as_bytes(), Direction::Forward)) {
        let (key, bytes) = entry?;
        if !key.starts_with(job.source.as_bytes()) {
            break;
        }
        if start
            .as_ref()
            .is_some_and(|after| key.as_ref() == after.as_bytes())
        {
            continue;
        }
        if visited == maximum {
            progress.complete = false;
            break;
        }
        let identity = std::str::from_utf8(&key).map_err(|_| {
            Error::new(
                "STORAGE_FAILURE",
                "invalid catalog key during index upgrade",
            )
        })?;
        if identity.len() > 128 {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "catalog key exceeds index upgrade bound",
            ));
        }
        if bytes.len() > job.page_bytes {
            return Err(Error::new(
                "RESOURCE_LIMIT",
                "catalog row exceeds index upgrade input admission",
            ));
        }
        let charge = (job.charge)(&bytes)?;
        if charge > remaining {
            if visited == 0 {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "one catalog row exceeds index upgrade page admission",
                ));
            }
            progress.complete = false;
            break;
        }
        remaining -= charge;
        (job.publish)(db, identity, &bytes, charge, &mut batch)?;
        progress.after = Some(identity.to_owned());
        visited += 1;
    }
    if progress.complete {
        progress.after = None;
    }
    super::put(&mut batch, job.checkpoint, &progress);
    if let Some(admit) = admit {
        admit(&batch)?;
    }
    commit(db, batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    const JOB: Job = Job {
        checkpoint: "test-upgrade",
        source: "record:",
        target: b"test-range:",
        page_bytes: 4,
        page_entries: 512,
        charge: |bytes| Ok(bytes.len()),
        publish: |_, identity, bytes, _, batch| {
            if bytes == b"bad" {
                return Err(Error::new("STORAGE_FAILURE", "invalid test catalog row"));
            }
            batch.put(format!("test-range:{identity}"), bytes);
            Ok(())
        },
    };

    #[test]
    fn byte_frontiers_resume_after_reopen_and_bad_pages_publish_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        db.put("record:a", b"aa").unwrap();
        db.put("record:b", b"bbb").unwrap();
        let mut batch = WriteBatch::default();
        seed(&db, &JOB, &mut batch).unwrap();
        commit(&db, batch).unwrap();
        step(&db, &JOB, Progress::default(), 512, None).unwrap();
        assert_eq!(db.get("test-range:record:a").unwrap().unwrap(), b"aa");
        assert!(db.get("test-range:record:b").unwrap().is_none());
        let progress =
            Progress::decode(&parse(&db.get(JOB.checkpoint).unwrap().unwrap()).unwrap()).unwrap();
        assert_eq!(progress.after.as_deref(), Some("record:a"));
        assert!(!progress.complete);
        drop(db);
        let db = DB::open_default(dir.path()).unwrap();
        run(&db, &JOB, None).unwrap();
        assert_eq!(db.get("test-range:record:b").unwrap().unwrap(), b"bbb");
        assert!(seed(&db, &JOB, &mut WriteBatch::default()).is_err());
        let wrong = Progress {
            after: Some("artifact:a".into()),
            complete: false,
        };
        assert!(step(&db, &JOB, wrong, 512, None).is_err());
        db.delete("test-range:record:a").unwrap();
        db.put("record:b", b"bad").unwrap();
        let mut batch = WriteBatch::default();
        super::super::put(&mut batch, JOB.checkpoint, &Progress::default());
        commit(&db, batch).unwrap();
        let mut wide = JOB;
        wide.page_bytes = 8;
        assert!(step(&db, &wide, Progress::default(), 512, None).is_err());
        assert!(db.get("test-range:record:a").unwrap().is_none());
        assert!(
            Progress::decode(&parse(&db.get(JOB.checkpoint).unwrap().unwrap()).unwrap())
                .unwrap()
                .after
                .is_none()
        );
        db.put("record:a", b"oversized").unwrap();
        assert_eq!(
            step(&db, &JOB, Progress::default(), 512, None)
                .unwrap_err()
                .code,
            "RESOURCE_LIMIT"
        );
    }
}
