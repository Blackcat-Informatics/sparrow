// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Archive-before-delete: the durable watermark, the one deletion gate and its writer fence.
//!
//! Positions are eviction-order suffixes, `{received_at:020}:{id}`, shared by the
//! `time:` record index and the `empty-time:` recordless artifact index. Every
//! published artifact and record has one, so a single byte comparison decides
//! whether its evidence is covered by a committed archive segment.

use super::{Error, Store, Writer, catalog_upgrade, recordless, writer_format};
use rocksdb::{DB, WriteBatch};

const THROUGH_KEY: &str = "archive:through";
const INTENT_KEY: &str = "archive:intent";
const VERSION: u8 = 1;
const ID_BYTES: usize = 128;
const POSITION_BYTES: usize = 20 + 1 + ID_BYTES;

/// Default early start: archive once three quarters of the retention window or budget is used.
pub const ARCHIVE_START_FRACTION: f64 = 0.75;
/// Default closing size of one segment's payload.
pub const ARCHIVE_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// Largest admitted segment payload, below the columnar codec's 256 MiB table bound.
pub const ARCHIVE_MAX_SEGMENT_BYTES: u64 = 192 * 1024 * 1024;
/// Smallest admitted segment closing size. An entry larger than the target forms
/// its own segment and streams its bytes outside the codec tables.
pub const ARCHIVE_MIN_SEGMENT_BYTES: u64 = 1024 * 1024;
/// Default closing record count of one segment.
pub const ARCHIVE_SEGMENT_RECORDS: u64 = 50_000;

/// Store-side archive policy. Its presence makes the store archive before it deletes.
#[derive(Clone, Debug, PartialEq)]
pub struct ArchivePolicy {
    /// Fraction of the retention window or data budget that starts archiving, 0.5..=0.95.
    pub start_fraction: f64,
    /// Payload bytes that close a segment, from 1 MiB to 192 MiB. A closing target,
    /// not a capacity: an entry larger than it is archived in a segment of its own.
    pub max_segment_bytes: u64,
    /// Eviction entries that close a segment; at least one.
    pub max_segment_records: u64,
}

impl Default for ArchivePolicy {
    fn default() -> Self {
        Self {
            start_fraction: ARCHIVE_START_FRACTION,
            max_segment_bytes: ARCHIVE_SEGMENT_BYTES,
            max_segment_records: ARCHIVE_SEGMENT_RECORDS,
        }
    }
}

impl ArchivePolicy {
    /// Refuse a policy outside the start window, the segment size range or the record floor.
    pub fn validate(&self) -> Result<(), Error> {
        if !(0.5..=0.95).contains(&self.start_fraction) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "archive start_fraction must be 0.5..0.95",
            ));
        }
        if !(ARCHIVE_MIN_SEGMENT_BYTES..=ARCHIVE_MAX_SEGMENT_BYTES)
            .contains(&self.max_segment_bytes)
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "archive max_segment_bytes must be 1 MiB..192 MiB",
            ));
        }
        if self.max_segment_records == 0 {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "archive max_segment_records must be positive",
            ));
        }
        Ok(())
    }
}

/// A segment whose encoding is complete; its archive write may begin once this is durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveIntent {
    /// Segment identity: BLAKE3 over the framed format name and table digests.
    pub segment_id: [u8; 32],
    /// First covered eviction position; the first entry after the watermark.
    pub first: String,
    /// Last covered eviction position, inclusive.
    pub last: String,
    /// Record index entries in the range.
    pub records: u64,
    /// Recordless artifact index entries in the range.
    pub artifacts: u64,
}

/// Durable archive progress of an archiving store.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ArchiveState {
    /// Last eviction position covered by a committed segment; absent before the first.
    pub through: Option<String>,
    /// The segment being written, when one is in flight.
    pub intent: Option<ArchiveIntent>,
}

/// Error code of a publication at or behind the archive watermark. Typed apart from
/// `STORAGE_FAILURE` so a caller that stamped the publication with an earlier instant (a UDP
/// syslog datagram stamped when it was read) can offer it once more with the commit instant.
pub const ARCHIVE_WATERMARK: &str = "ARCHIVE_WATERMARK";

impl ArchiveState {
    /// The one deletion gate: evidence retires only once a committed segment covers it.
    fn archived(&self, position: &[u8]) -> bool {
        self.through
            .as_deref()
            .is_some_and(|through| position <= through.as_bytes())
    }

    /// Publication behind the covered or in-flight range would never be archived.
    fn admit_publication(&self, earliest: &[u8]) -> Result<(), Error> {
        let fence = self
            .through
            .as_deref()
            .into_iter()
            .chain(self.intent.as_ref().map(|intent| intent.last.as_str()))
            .max();
        match fence {
            Some(fence) if earliest <= fence.as_bytes() => Err(Error::new(
                ARCHIVE_WATERMARK,
                format!(
                    "publication at {} is at or before the archive watermark {fence}; the server clock moved backwards or an import outlived the archive horizon; correct the clock and retry",
                    String::from_utf8_lossy(earliest)
                ),
            )),
            _ => Ok(()),
        }
    }
}

/// The eviction-order position of one published artifact or record.
pub(super) fn position(received_at: u64, id: &str) -> String {
    format!("{received_at:020}:{id}")
}

fn valid_position(position: &[u8]) -> bool {
    position.len() <= POSITION_BYTES
        && position.len() > 21
        && position[..20].iter().all(u8::is_ascii_digit)
        && position[20] == b':'
        && std::str::from_utf8(&position[21..]).is_ok()
}

fn corrupt(message: &str) -> Error {
    Error::new("STORAGE_FAILURE", format!("archive {message}"))
}

fn encode_through(through: Option<&str>) -> Vec<u8> {
    let mut bytes = vec![VERSION];
    bytes.extend_from_slice(through.unwrap_or_default().as_bytes());
    bytes
}

fn decode_through(bytes: &[u8]) -> Result<Option<String>, Error> {
    let Some((&VERSION, position)) = bytes.split_first() else {
        return Err(corrupt("watermark has an unsupported encoding"));
    };
    if position.is_empty() {
        return Ok(None);
    }
    if !valid_position(position) {
        return Err(corrupt("watermark position is malformed"));
    }
    String::from_utf8(position.to_vec())
        .map(Some)
        .map_err(|_| corrupt("watermark position is not UTF-8"))
}

fn encode_intent(intent: &ArchiveIntent) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + 32 + 2 + 2 * POSITION_BYTES + 16);
    bytes.push(VERSION);
    bytes.extend_from_slice(&intent.segment_id);
    for position in [&intent.first, &intent.last] {
        // INVARIANT: validated positions are at most POSITION_BYTES (149) long.
        bytes.push(position.len() as u8);
        bytes.extend_from_slice(position.as_bytes());
    }
    bytes.extend_from_slice(&intent.records.to_be_bytes());
    bytes.extend_from_slice(&intent.artifacts.to_be_bytes());
    bytes
}

fn decode_intent(bytes: &[u8]) -> Result<ArchiveIntent, Error> {
    fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], Error> {
        if bytes.len() < count {
            return Err(corrupt("intent is truncated"));
        }
        let (head, tail) = bytes.split_at(count);
        *bytes = tail;
        Ok(head)
    }
    fn position(bytes: &mut &[u8]) -> Result<String, Error> {
        let length = usize::from(take(bytes, 1)?[0]);
        let raw = take(bytes, length)?;
        if !valid_position(raw) {
            return Err(corrupt("intent position is malformed"));
        }
        String::from_utf8(raw.to_vec()).map_err(|_| corrupt("intent position is not UTF-8"))
    }
    fn count(bytes: &mut &[u8]) -> Result<u64, Error> {
        let raw = take(bytes, 8)?;
        Ok(u64::from_be_bytes(
            raw.try_into().map_err(|_| corrupt("intent count"))?,
        ))
    }
    let mut rest = bytes;
    if take(&mut rest, 1)? != [VERSION] {
        return Err(corrupt("intent has an unsupported encoding"));
    }
    let segment_id = take(&mut rest, 32)?
        .try_into()
        .map_err(|_| corrupt("intent segment identity"))?;
    let intent = ArchiveIntent {
        segment_id,
        first: position(&mut rest)?,
        last: position(&mut rest)?,
        records: count(&mut rest)?,
        artifacts: count(&mut rest)?,
    };
    if !rest.is_empty() || intent.first > intent.last {
        return Err(corrupt("intent range is malformed"));
    }
    Ok(intent)
}

/// Load archive progress at open, fencing a newly configured store first.
///
/// A store fenced for archiving cannot be opened without a policy: retiring
/// unarchived evidence would silently break the archive's completeness.
pub(super) fn open(db: &DB, policy: Option<&ArchivePolicy>) -> Result<Option<ArchiveState>, Error> {
    let format = db
        .get("store_format")?
        .ok_or_else(|| corrupt("writer format missing"))?;
    let fenced = writer_format::has_archive(&format);
    let through = db.get(THROUGH_KEY)?;
    let intent = db.get(INTENT_KEY)?;
    if fenced != through.is_some() || (intent.is_some() && !fenced) {
        return Err(corrupt(
            "watermark and writer fence disagree; restore the original catalog",
        ));
    }
    match (policy, fenced) {
        (None, false) => return Ok(None),
        (None, true) => {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "this store archives evidence before deleting it; configure the [archive] table to open it",
            ));
        }
        (Some(_), false) => {
            let mut batch = WriteBatch::default();
            batch.put("store_format", writer_format::enable_archive(&format)?);
            // The archive family includes the log time law; keep that fence coherent.
            if db.get(super::source_time::LOG_FENCE)?.is_none() {
                batch.put(super::source_time::LOG_FENCE, super::log_time::LAW);
            }
            // Nothing is archived yet: the whole retained backlog waits for its first segment.
            batch.put(THROUGH_KEY, encode_through(None));
            catalog_upgrade::commit(db, batch)?;
            return Ok(Some(ArchiveState::default()));
        }
        (Some(_), true) => {}
    }
    Ok(Some(ArchiveState {
        through: through
            .as_deref()
            .map(decode_through)
            .transpose()?
            .flatten(),
        intent: intent.as_deref().map(decode_intent).transpose()?,
    }))
}

impl Writer {
    /// The deletion gate shared by expiry, eviction and retirement.
    /// Stores without an archive policy retire exactly as before.
    pub(super) fn archived(&self, received_at: u64, id: &str) -> bool {
        self.archive.as_ref().is_none_or(|state| {
            state
                .borrow()
                .archived(position(received_at, id).as_bytes())
        })
    }

    /// Retirement's own assertion of the gate; reaching it unarchived is a broken invariant.
    pub(super) fn require_archived(&self, received_at: u64, id: &str) -> Result<(), Error> {
        if self.archived(received_at, id) {
            Ok(())
        } else {
            Err(Error::new(
                "STORAGE_FAILURE",
                format!("retirement reached unarchived evidence {id}; no evidence was deleted"),
            ))
        }
    }

    /// Clock-regression guard for every publication batch.
    pub(super) fn admit_archive_publication(&self, earliest: Option<&[u8]>) -> Result<(), Error> {
        match (&self.archive, earliest) {
            (Some(state), Some(earliest)) => state.borrow().admit_publication(earliest),
            _ => Ok(()),
        }
    }

    fn archive_state(&self) -> Result<&std::cell::RefCell<ArchiveState>, Error> {
        self.archive
            .as_ref()
            .ok_or_else(|| Error::new("UNSUPPORTED", "archiving is not configured for this store"))
    }

    /// Count the contiguous eviction range an intent claims, from the first unarchived entry.
    fn archive_range(&self, intent: &ArchiveIntent, through: Option<&str>) -> Result<(), Error> {
        let maximum = self
            .config
            .archive
            .as_ref()
            .ok_or_else(|| corrupt("state is loaded without its policy"))?
            .max_segment_records;
        let (mut records, mut artifacts) = (0_u64, 0_u64);
        let mut reached = false;
        for entry in self.eviction_entries(through.map(str::as_bytes)) {
            let (key, _) = entry?;
            let (empty, position) = match key.strip_prefix(recordless::PREFIX) {
                Some(position) => (true, position),
                None => (false, &key[b"time:".len()..]),
            };
            if through.is_some_and(|through| position <= through.as_bytes()) {
                continue;
            }
            if records + artifacts == 0 && position != intent.first.as_bytes() {
                return Err(Error::new(
                    "CONFLICT",
                    "archive intent must begin at the first unarchived entry",
                ));
            }
            if records + artifacts == maximum {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "archive intent exceeds max_segment_records",
                ));
            }
            if empty {
                artifacts += 1;
            } else {
                records += 1;
            }
            if position == intent.last.as_bytes() {
                reached = true;
                break;
            }
        }
        if !reached || records != intent.records || artifacts != intent.artifacts {
            return Err(Error::new(
                "CONFLICT",
                "archive intent range or counts disagree with the retained catalog",
            ));
        }
        Ok(())
    }
}

impl Store {
    /// Durable archive progress, or `None` when the store does not archive.
    pub fn archive_state(&self) -> Result<Option<ArchiveState>, Error> {
        Ok(self
            .lock()?
            .archive
            .as_ref()
            .map(|state| state.borrow().clone()))
    }

    /// Durably record the segment about to be written. Identical retries are idempotent.
    ///
    /// The range must start at the first entry after the watermark and its counts
    /// must match the retained catalog. Publication behind its end is refused from
    /// now on, so the range cannot change while the segment is written.
    pub fn archive_intend(&self, intent: &ArchiveIntent) -> Result<(), Error> {
        if !valid_position(intent.first.as_bytes())
            || !valid_position(intent.last.as_bytes())
            || intent.first > intent.last
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "archive intent requires an ordered pair of eviction positions",
            ));
        }
        let writer = self.lock()?;
        let state = writer.archive_state()?;
        let current = state.borrow().clone();
        match &current.intent {
            Some(existing) if existing == intent => return Ok(()),
            Some(_) => {
                return Err(Error::new(
                    "CONFLICT",
                    "another archive segment is in flight; commit it first",
                ));
            }
            None => {}
        }
        writer.archive_range(intent, current.through.as_deref())?;
        let mut batch = WriteBatch::default();
        batch.put(INTENT_KEY, encode_intent(intent));
        writer.commit(batch)?;
        state.borrow_mut().intent = Some(intent.clone());
        Ok(())
    }

    /// Advance the watermark over a written and verified segment, in one synced batch.
    /// Retirement of the covered evidence may proceed from the next maintenance pass.
    pub fn archive_commit(&self, segment_id: &[u8; 32]) -> Result<(), Error> {
        let writer = self.lock()?;
        let state = writer.archive_state()?;
        let Some(intent) = state.borrow().intent.clone() else {
            return Err(Error::new("NOT_FOUND", "no archive segment is in flight"));
        };
        if intent.segment_id != *segment_id {
            return Err(Error::new(
                "CONFLICT",
                "archive commit names a different segment than the one in flight",
            ));
        }
        let mut batch = WriteBatch::default();
        batch.put(THROUGH_KEY, encode_through(Some(&intent.last)));
        batch.delete(INTENT_KEY);
        writer.commit(batch)?;
        *state.borrow_mut() = ArchiveState {
            through: Some(intent.last),
            intent: None,
        };
        // Pressure stopped at the old watermark; let the next pass revisit that entry.
        writer.eviction.borrow_mut().reset();
        self.wake_retention();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// INVARIANT: the durable encodings round-trip and refuse truncation or foreign versions.
    #[test]
    fn archive_markers_round_trip_and_refuse_malformed_bytes() {
        let first = position(5, "record:a");
        let last = position(7, "artifact:b");
        assert_eq!(decode_through(&encode_through(None)).unwrap(), None);
        assert_eq!(
            decode_through(&encode_through(Some(&last))).unwrap(),
            Some(last.clone())
        );
        assert!(decode_through(b"\x02").is_err());
        assert!(decode_through(b"\x01short").is_err());
        let intent = ArchiveIntent {
            segment_id: [7; 32],
            first,
            last,
            records: 3,
            artifacts: 1,
        };
        let bytes = encode_intent(&intent);
        assert_eq!(decode_intent(&bytes).unwrap(), intent);
        assert!(decode_intent(&bytes[..bytes.len() - 1]).is_err());
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(decode_intent(&longer).is_err());
    }
}
