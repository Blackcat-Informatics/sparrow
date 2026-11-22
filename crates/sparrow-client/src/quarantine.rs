// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The client quarantine directory: bytes the daemon cannot take are kept, never reduced to a
//! count. One file per batch holds a header line (reason code, reason, counts, identity)
//! followed by the batch's canonical event lines, exactly the bytes a daemon would have been
//! given. The worker never deletes or expires a quarantine file. [`crate::replay_quarantine`]
//! resends one and removes it once the store acknowledges it; a refusal that recurs is recorded
//! in the file's header and the file stays.

use crate::EventBatch;
use purrdf_lex::json::{self, Object};
use std::{
    fs::File,
    io::Write,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

/// Directory used when the configuration names none: `sparrow-client/quarantine` under an
/// absolute `XDG_STATE_HOME`, else under `$HOME/.local/state`. Empty when neither is usable:
/// configuration validation then fails with an actionable error. There is no volatile fallback,
/// because this directory holds the only copy of a refused log.
pub(crate) fn default_dir() -> PathBuf {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .map(|home| home.join(".local/state"))
        });
    state.map_or_else(PathBuf::new, |state| {
        state.join("sparrow-client").join("quarantine")
    })
}

/// A save that did not complete. `created` names the quarantine file this call had already
/// linked into place when it failed (its scratch name could not be removed), so the caller
/// counts that file once even though the save is reported failed and retried.
#[derive(Debug)]
pub(crate) struct SaveFailure {
    pub(crate) error: std::io::Error,
    pub(crate) created: Option<PathBuf>,
    pub(crate) published: Option<Published>,
}

/// The complete quarantine file already appeared. Finish only these local effects.
#[derive(Clone, Debug)]
pub(crate) struct Published {
    pub(crate) path: PathBuf,
    pub(crate) scratch: Option<PathBuf>,
}

impl Published {
    pub(crate) fn finish(&self) -> Result<(PathBuf, bool), SaveFailure> {
        let result = (|| {
            let parent = self
                .path
                .parent()
                .ok_or_else(|| std::io::Error::other("quarantine file missing parent"))?;
            let directory = crate::ExclusiveFileLock::acquire(File::open(parent)?)?;
            if let Some(scratch) = &self.scratch {
                match std::fs::remove_file(scratch) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                    Err(e) => return Err(e),
                }
            }
            directory.as_ref().sync_all()
        })();
        result
            .map(|()| (self.path.clone(), false))
            .map_err(|error| SaveFailure {
                error,
                created: None,
                published: Some(self.clone()),
            })
    }
}

impl From<std::io::Error> for SaveFailure {
    fn from(error: std::io::Error) -> Self {
        Self {
            error,
            created: None,
            published: None,
        }
    }
}

/// What is known about publication under the producer's original request identity.
#[derive(Clone, Copy, Default)]
pub(crate) enum Original {
    #[default]
    Unsent,
    Refused,
    Submitted,
}
impl Original {
    pub(crate) fn definitively_refused(error: &crate::Error) -> bool {
        matches!(error, crate::Error::Domain { code, .. } if matches!(code.as_str(),
            "INVALID_ARGUMENT" | "CONFLICT" | "RESOURCE_LIMIT" | "UNAUTHENTICATED" | "PERMISSION_DENIED" | "READ_ONLY" | "SHUTTING_DOWN" | "STORE_MISMATCH"))
    }
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Unsent => "unsent",
            Self::Refused => "refused",
            Self::Submitted => "submitted",
        }
    }
}

/// Durably write one batch with its reason; returns the file path and whether this call
/// created it. The file appears only complete (written and synced under a scratch name, then
/// linked). A name already taken by this batch's own bytes, compared exactly after the header
/// line, is reused, so saving one identity twice writes once; a name taken by different bytes
/// (identities that sanitize to one name) gets a numeric suffix, so one batch is never reported
/// saved on the strength of another's file. Any failure is returned: the caller must keep the
/// batch.
pub(crate) fn save(
    dir: &Path,
    batch: &EventBatch,
    code: &str,
    reason: &str,
    original: Original,
    store: Option<&str>,
) -> Result<(PathBuf, bool), SaveFailure> {
    save_with(dir, batch, code, reason, original, store, |scratch| {
        std::fs::remove_file(scratch)
    })
}

/// [`save`] with the removal of the scratch name supplied, so a test can fail it.
fn save_with(
    dir: &Path,
    batch: &EventBatch,
    code: &str,
    reason: &str,
    original: Original,
    store: Option<&str>,
    remove_scratch: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<(PathBuf, bool), SaveFailure> {
    let (Some(first), Some(last)) = (batch.events.first(), batch.events.last()) else {
        return Err(std::io::Error::other("quarantine of an empty batch").into());
    };
    create_dir(dir)?;
    let directory = crate::ExclusiveFileLock::acquire(File::open(dir)?)?;
    let payload = batch.quarantine_bytes();
    let header = Object::new()
        .with("profile", "sparrow.client.quarantine/2")
        .with("original", original.label())
        .with(
            "store",
            store.map_or(json::Value::Null, |id| id.to_owned().into()),
        )
        .with("reason_code", code)
        .with("reason", reason)
        .with("request_id", batch.request_id.clone())
        .with("source", batch.source.clone())
        .with(
            "client_instance",
            purrdf_hash::hex::encode(&batch.client_instance),
        )
        .with("events", batch.events.len() as u64)
        .with("first_sequence", first.sequence)
        .with("last_sequence", last.sequence)
        .with("payload_bytes", payload.len() as u64);
    let mut bytes = String::new();
    json::write_into(&mut bytes, &header.into(), json::Format::COMPACT);
    bytes.push('\n');
    let mut bytes = bytes.into_bytes();
    bytes.extend_from_slice(&payload);

    let name = filename_stem(&batch.request_id);
    let scratch = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    // A scratch name left by an earlier failed cleanup is a second link to a quarantine file:
    // opening it for writing would rewrite that file. Drop the name first; the file stays.
    match std::fs::remove_file(&scratch) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&scratch)?;
    let written = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    let linked = written.map_err(SaveFailure::from).and_then(|()| {
        for attempt in 0..=u64::MAX {
            let path = if attempt == 0 {
                dir.join(format!("{name}.quarantine.jsonl"))
            } else {
                dir.join(format!("{name}-{attempt}.quarantine.jsonl"))
            };
            match std::fs::hard_link(&scratch, &path) {
                Ok(()) => return Ok((path, true)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if holds_payload(&std::fs::read(&path)?, &payload, batch) {
                        return Ok((path, false));
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(std::io::Error::other("every quarantine file name for this batch is taken").into())
    });
    // The scratch name is this function's own temporary; the quarantine file is never removed.
    let cleaned = remove_scratch(&scratch);
    let (path, created) = linked?;
    if let Err(error) = cleaned {
        return Err(SaveFailure {
            error,
            created: created.then(|| path.clone()),
            published: Some(Published {
                path,
                scratch: Some(scratch),
            }),
        });
    }
    directory.as_ref().sync_all().map_err(|error| SaveFailure {
        error,
        created: created.then(|| path.clone()),
        published: Some(Published {
            path: path.clone(),
            scratch: None,
        }),
    })?;
    Ok((path, created))
}

fn filename_stem(request: &str) -> String {
    request
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// The writer published a complete file but died before unlinking its temporary
/// hard link. Under directory ownership, establish the final entry's durability
/// before removing the matching scratch name. Other aliases require inspection.
pub(crate) fn recover_scratch(
    path: &Path,
    batch: &EventBatch,
    metadata: &std::fs::Metadata,
    directory: &File,
) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let invalid = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "quarantine file has a second link outside its writer scratch protocol",
        )
    };
    let stem = filename_stem(&batch.request_id);
    let Some(published) = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".quarantine.jsonl"))
        .and_then(|name| name.strip_prefix(&stem))
    else {
        return Err(invalid());
    };
    if !published.is_empty()
        && !published
            .strip_prefix('-')
            .is_some_and(|s| s.parse::<u64>().is_ok())
    {
        return Err(invalid());
    }
    let prefix = format!(".{stem}.");
    for entry in std::fs::read_dir(path.parent().ok_or_else(invalid)?)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|s| s.strip_prefix(&prefix))
            .and_then(|s| s.strip_suffix(".tmp"))
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == 0 {
            continue;
        }
        let candidate = entry.metadata()?;
        if candidate.is_file()
            && candidate.dev() == metadata.dev()
            && candidate.ino() == metadata.ino()
        {
            directory.sync_all()?;
            std::fs::remove_file(entry.path())?;
            return directory.sync_all();
        }
    }
    Err(invalid())
}

/// Whether a quarantine file holds exactly this payload after its header line.
fn holds_payload(file: &[u8], payload: &[u8], batch: &EventBatch) -> bool {
    let Some(end) = file.iter().position(|byte| *byte == b'\n') else {
        return false;
    };
    let Ok(header) = json::read_slice(&file[..end], json::Limits::default()) else {
        return false;
    };
    header.get("request_id").and_then(json::Value::as_str) == Some(batch.request_id.as_str())
        && header.get("source").and_then(json::Value::as_str) == Some(batch.source.as_str())
        && header.get("payload_bytes").and_then(json::Value::as_u64) == Some(payload.len() as u64)
        && file[end + 1..].get(..payload.len()) == Some(payload)
}

/// Create the directory with mode 0700 and make every directory entry this call created
/// durable, so a crash after the first save cannot lose the directory that holds it.
pub(crate) fn create_dir(dir: &Path) -> std::io::Result<()> {
    let created: Vec<&Path> = dir.ancestors().take_while(|path| !path.exists()).collect();
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    for path in &created {
        std::fs::File::open(path)?.sync_all()?;
    }
    if let Some(parent) = created.last().and_then(|path| path.parent()) {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventKind, EventLevel, ProducerEvent};

    fn batch(request: &str, sequences: &[u64]) -> EventBatch {
        EventBatch {
            request_id: request.into(),
            source: "app".into(),
            client_instance: [7; 16],
            events: sequences
                .iter()
                .map(|sequence| ProducerEvent {
                    sequence: *sequence,
                    event_time: 1000,
                    kind: EventKind::Log,
                    level: EventLevel::Info,
                    name: format!("event {sequence}"),
                    target: String::new(),
                    fields: vec![],
                    context: vec![],
                    span: None,
                    follows_from: None,
                })
                .collect(),
        }
    }

    /// INVARIANT: a quarantine file carries the refusal reason, the event count and the
    /// batch's own event lines, so the bytes someone sent are recoverable from it alone; the
    /// old behaviour kept only a number.
    #[test]
    fn file_holds_reason_count_and_every_event_line() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch("events:abc:1", &[3, 4]);
        let (path, created) = save(
            dir.path(),
            &batch,
            "INVALID_ARGUMENT",
            "too odd",
            Original::Unsent,
            None,
        )
        .unwrap();
        assert!(created);
        let text = std::fs::read_to_string(&path).unwrap();
        let mut lines = text.lines();
        let header = lines.next().unwrap();
        for needle in [
            "\"reason_code\":\"INVALID_ARGUMENT\"",
            "\"reason\":\"too odd\"",
            "\"events\":2",
            "\"first_sequence\":3",
            "\"last_sequence\":4",
        ] {
            assert!(header.contains(needle), "{needle} missing from {header}");
        }
        let rest: Vec<_> = lines.collect();
        assert_eq!(rest.len(), 2);
        assert!(rest[0].contains("event 3") && rest[1].contains("event 4"));
        assert_eq!(
            text.as_bytes()[header.len() + 1..],
            batch.quarantine_bytes()[..]
        );
    }

    /// INVARIANT: saving the same batch identity twice neither fails nor rewrites the file,
    /// and no scratch file is left; the client never replaces bytes it already preserved.
    #[test]
    fn saving_the_same_batch_again_keeps_the_first_file_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch("events:abc:2", &[1]);
        let (first, made) = save(
            dir.path(),
            &batch,
            "CONFLICT",
            "first",
            Original::Unsent,
            None,
        )
        .unwrap();
        let (second, again) = save(
            dir.path(),
            &batch,
            "CONFLICT",
            "second",
            Original::Unsent,
            None,
        )
        .unwrap();
        assert_eq!((first.clone(), made, again), (second, true, false));
        assert!(std::fs::read_to_string(&first).unwrap().contains("first"));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// INVARIANT: a quarantine that cannot be written is an error the caller must act on
    /// (keep the batch); it is never reported as saved.
    #[test]
    fn unwritable_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("file");
        std::fs::write(&blocker, b"x").unwrap();
        assert!(
            save(
                &blocker,
                &batch("events:abc:3", &[1]),
                "CONFLICT",
                "r",
                Original::Unsent,
                None
            )
            .is_err()
        );
    }

    /// INVARIANT: two different batches whose identities sanitize to one file name each get
    /// their own file; the second is never reported saved on the strength of the first's file.
    #[test]
    fn a_name_collision_with_different_bytes_gets_its_own_file() {
        let dir = tempfile::tempdir().unwrap();
        let (one, _) = save(
            dir.path(),
            &batch("events:abc:4", &[1]),
            "CONFLICT",
            "r",
            Original::Unsent,
            None,
        )
        .unwrap();
        let (two, made) = save(
            dir.path(),
            &batch("events-abc-4", &[2]),
            "CONFLICT",
            "r",
            Original::Unsent,
            None,
        )
        .unwrap();
        assert!(made && one != two);
        assert!(std::fs::read_to_string(&two).unwrap().contains("event 2"));
        assert!(std::fs::read_to_string(&one).unwrap().contains("event 1"));
    }

    /// INVARIANT: the quarantine directory is private (0700), created with its missing
    /// ancestors.
    #[test]
    fn directory_is_created_private_with_ancestors() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let leaf = dir.path().join("a/b");
        save(
            &leaf,
            &batch("events:abc:5", &[1]),
            "CONFLICT",
            "r",
            Original::Unsent,
            None,
        )
        .unwrap();
        let mode = std::fs::metadata(&leaf).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    /// INVARIANT: a name is reused only when the file holds exactly this batch's payload after
    /// its header line. A batch whose event lines merely equal the tail of another batch's file
    /// (here the second event of a two-event batch) is a different batch and gets its own file;
    /// the old tail comparison reported it saved on the strength of the other's file.
    #[test]
    fn a_payload_equal_to_the_tail_of_another_file_is_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let (whole, _) = save(
            dir.path(),
            &batch("events:abc:6", &[1, 2]),
            "CONFLICT",
            "r",
            Original::Unsent,
            None,
        )
        .unwrap();
        let (tail, made) = save(
            dir.path(),
            &batch("events-abc-6", &[2]),
            "CONFLICT",
            "r",
            Original::Unsent,
            None,
        )
        .unwrap();
        assert!(made && whole != tail);
        let kept = std::fs::read_to_string(&tail).unwrap();
        assert!(kept.contains("event 2") && !kept.contains("event 1"));
    }

    /// INVARIANT: when the scratch name cannot be removed after the file was linked, the save is
    /// reported failed (so the batch is retried) but names the file it created, so the caller
    /// counts it once; the retry reuses the file without rewriting it and clears the scratch
    /// name. The old code returned a bare error, so the retry saw `created == false` and the
    /// file was never counted.
    #[test]
    fn a_failed_scratch_cleanup_after_the_link_still_names_the_created_file() {
        let dir = tempfile::tempdir().unwrap();
        let batch = batch("events:abc:7", &[1]);
        let failure = save_with(
            dir.path(),
            &batch,
            "CONFLICT",
            "first",
            Original::Unsent,
            None,
            |_| Err(std::io::Error::other("scratch removal refused")),
        )
        .unwrap_err();
        let created = failure.created.expect("the linked file is named");
        assert!(
            failure
                .error
                .to_string()
                .contains("scratch removal refused")
        );
        let (again, made) = save(
            dir.path(),
            &batch,
            "CONFLICT",
            "second",
            Original::Unsent,
            None,
        )
        .unwrap();
        assert_eq!((again, made), (created.clone(), false));
        let text = std::fs::read_to_string(&created).unwrap();
        assert!(text.contains("\"reason\":\"first\""), "{text}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
