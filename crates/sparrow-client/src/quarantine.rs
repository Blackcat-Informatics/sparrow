// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The client quarantine directory: bytes the daemon cannot take are kept, never reduced to a
//! count. One file per batch holds a header line (reason code, reason, counts, identity)
//! followed by the batch's canonical event lines, exactly the bytes a daemon would have been
//! given. The client never deletes, rewrites or expires a quarantine file; an operator or a
//! later replay owns it.

use crate::EventBatch;
use purrdf_lex::json::{self, Object};
use std::{
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

/// Directory used when the configuration names none: the user's state directory, else the
/// system temporary directory.
pub(crate) fn default_dir() -> PathBuf {
    let state = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .map(|home| home.join(".local/state"))
        })
        .unwrap_or_else(std::env::temp_dir);
    state.join("sparrow-client").join("quarantine")
}

/// Durably write one batch with its reason; returns the file path. The file appears only
/// complete (written and synced under a scratch name, then linked), and a batch identity that
/// was already written is not written twice. Any failure is returned: the caller must keep the
/// batch.
pub(crate) fn save(
    dir: &Path,
    batch: &EventBatch,
    code: &str,
    reason: &str,
) -> std::io::Result<PathBuf> {
    let (Some(first), Some(last)) = (batch.events.first(), batch.events.last()) else {
        return Err(std::io::Error::other("quarantine of an empty batch"));
    };
    std::fs::create_dir_all(dir)?;
    let payload = batch.quarantine_bytes();
    let header = Object::new()
        .with("profile", "sparrow.client.quarantine/1")
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

    let name: String = batch
        .request_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let path = dir.join(format!("{name}.quarantine.jsonl"));
    let scratch = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&scratch)?;
    let written = file.write_all(&bytes).and_then(|()| file.sync_all());
    drop(file);
    let linked = written.and_then(|()| match std::fs::hard_link(&scratch, &path) {
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        other => other,
    });
    // The scratch name is this function's own temporary; the quarantine file is never removed.
    let cleaned = std::fs::remove_file(&scratch);
    linked?;
    cleaned?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(path)
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
        let path = save(dir.path(), &batch, "INVALID_ARGUMENT", "too odd").unwrap();
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
        let first = save(dir.path(), &batch, "CONFLICT", "first").unwrap();
        let second = save(dir.path(), &batch, "CONFLICT", "second").unwrap();
        assert_eq!(first, second);
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
        assert!(save(&blocker, &batch("events:abc:3", &[1]), "CONFLICT", "r").is_err());
    }
}
