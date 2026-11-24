// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded producer sequence windows committed with their record occurrences.

use super::{Catalog, DAY, Error, Store, Writer, integer, random_id, read_field};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

/// Maximum simultaneously retained producer windows; expired windows can be replaced.
pub const MAX_PRODUCER_STREAMS: usize = 64;
/// Exact recent gap intervals retained per window; aggregate counts remain exact.
pub const MAX_PRODUCER_GAPS: usize = 16;
/// Persistent encoded registry ceiling, admitted before native decoding.
pub const MAX_PRODUCER_REGISTRY_BYTES: usize = 128 * 1024;
pub(super) const KEY: &str = "producer-stream-registry";

/// Validated producer envelope accompanying already-normalized event bytes.
#[derive(Clone, Debug)]
pub struct ProducerBatch {
    /// Producer process identity, preserved across reconnects.
    pub instance: [u8; 16],
    /// One nonzero increasing sequence per normalized record.
    pub sequences: Vec<u64>,
}

impl ProducerBatch {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.sequences.is_empty()
            || self.sequences.len() > 256
            || self.sequences[0] == 0
            || self.sequences.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "producer batch requires 1..256 nonzero increasing sequences",
            ));
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Value {
        Object::new()
            .with("instance", super::hex::encode(&self.instance))
            .with(
                "sequences",
                self.sequences
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>(),
            )
            .into()
    }
}

/// Immutable record position; the finite stream summary can expire independently.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerPosition {
    /// Opaque window identity, never reused after expiry.
    pub stream_id: String,
    /// Producer sequence, distinct from the daemon WAL fence.
    pub sequence: u64,
}

impl Catalog for ProducerPosition {
    fn encode(&self) -> Value {
        Object::new()
            .with("stream_id", self.stream_id.clone())
            .with("sequence", self.sequence.to_string())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let position = Self {
            stream_id: read_field(value, "stream_id")?,
            sequence: integer(value, "sequence")?,
        };
        if !position.stream_id.starts_with("producer-stream:")
            || position.stream_id.len() > 128
            || position.sequence == 0
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid producer record position",
            ));
        }
        Ok(position)
    }
}

/// Inclusive unobserved sequence interval, with no inferred cause of loss.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerGap {
    /// First unobserved sequence.
    pub first: u64,
    /// Last unobserved sequence.
    pub last: u64,
}

/// Finite accepted producer window. Prefix and trailing continuity remain unknown.
#[derive(Clone, Debug)]
pub struct ProducerStream {
    /// Opaque store-local identity.
    pub id: String,
    /// Exact source label.
    pub source: String,
    /// Lowercase hex producer instance bytes.
    pub instance: String,
    /// First observed position; no assumption about earlier generation positions.
    pub first_sequence: u64,
    /// Last accepted position.
    pub last_sequence: u64,
    /// Count of accepted independent record occurrences.
    pub accepted_events: u64,
    /// Count of positions absent between accepted events.
    pub unobserved_sequences: u64,
    /// Recent exact inclusive gap intervals in ascending order.
    pub gaps: Vec<ProducerGap>,
    /// Whether these intervals cover every observed gap in this window.
    pub gaps_complete: bool,
    /// First server acceptance instant.
    pub first_received_at: u64,
    /// Most recent server acceptance instant.
    pub last_received_at: u64,
    /// Finite metadata deadline, independent of pins on records.
    pub expires_at: u64,
}

impl Catalog for ProducerStream {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("instance", self.instance.clone())
            .with("first_sequence", self.first_sequence.to_string())
            .with("last_sequence", self.last_sequence.to_string())
            .with("accepted_events", self.accepted_events.to_string())
            .with(
                "unobserved_sequences",
                self.unobserved_sequences.to_string(),
            )
            .with(
                "gaps",
                self.gaps
                    .iter()
                    .map(|gap| {
                        Object::new()
                            .with("first", gap.first.to_string())
                            .with("last", gap.last.to_string())
                            .into()
                    })
                    .collect::<Vec<Value>>(),
            )
            .with("gaps_complete", self.gaps_complete)
            .with("first_received_at", self.first_received_at.to_string())
            .with("last_received_at", self.last_received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let rows = value
            .get("gaps")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "producer gaps missing"))?;
        if rows.len() > MAX_PRODUCER_GAPS {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "producer gap suffix exceeds its bound",
            ));
        }
        let state = Self {
            id: read_field(value, "id")?,
            source: read_field(value, "source")?,
            instance: read_field(value, "instance")?,
            first_sequence: integer(value, "first_sequence")?,
            last_sequence: integer(value, "last_sequence")?,
            accepted_events: integer(value, "accepted_events")?,
            unobserved_sequences: integer(value, "unobserved_sequences")?,
            gaps: rows
                .iter()
                .map(|gap| {
                    Ok(ProducerGap {
                        first: integer(gap, "first")?,
                        last: integer(gap, "last")?,
                    })
                })
                .collect::<Result<_, Error>>()?,
            gaps_complete: read_field(value, "gaps_complete")?,
            first_received_at: integer(value, "first_received_at")?,
            last_received_at: integer(value, "last_received_at")?,
            expires_at: integer(value, "expires_at")?,
        };
        let mut previous = state.first_sequence;
        let mut count = 0_u64;
        for gap in &state.gaps {
            if gap.first <= previous || gap.first > gap.last || gap.last >= state.last_sequence {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "producer gap intervals inconsistent",
                ));
            }
            previous = gap
                .last
                .checked_add(1)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "producer gap overflow"))?;
            count = count
                .checked_add(gap.last - gap.first + 1)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "producer gap count overflow"))?;
        }
        if !state.id.starts_with("producer-stream:")
            || state.id.len() > 128
            || state.source.is_empty()
            || state.source.len() > 512
            || state.instance.len() != 32
            || !state
                .instance
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || state.first_sequence == 0
            || state.last_sequence < state.first_sequence
            || state.accepted_events == 0
            || state
                .last_sequence
                .checked_sub(state.first_sequence)
                .and_then(|n| n.checked_add(1))
                .and_then(|n| n.checked_sub(state.unobserved_sequences))
                != Some(state.accepted_events)
            || count > state.unobserved_sequences
            || (state.gaps_complete && count != state.unobserved_sequences)
            || state.last_received_at.checked_add(30 * DAY) != Some(state.expires_at)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "producer stream invariants inconsistent",
            ));
        }
        Ok(state)
    }
}

pub(super) fn registry(writer: &Writer) -> Result<Vec<ProducerStream>, Error> {
    super::sequence_window::registry(
        writer,
        KEY,
        MAX_PRODUCER_REGISTRY_BYTES,
        MAX_PRODUCER_STREAMS,
        |stream: &ProducerStream| {
            (
                stream.id.clone(),
                (stream.source.clone(), stream.instance.clone()),
            )
        },
    )
}

pub(super) struct PreparedStream {
    pub id: String,
    encoded: String,
}

impl PreparedStream {
    pub(super) fn publish(self, batch: &mut WriteBatch) {
        batch.put(KEY, self.encoded);
    }
}

/// Prepare the stream window for one batch. `None` means the registry cannot hold a new
/// window (full, or past its byte bound): the batch is still accepted, without positions,
/// and the caller counts the lost gap evidence (#215).
pub(super) fn prepare(
    writer: &Writer,
    source: &str,
    input: &ProducerBatch,
    now: u64,
) -> Result<Option<PreparedStream>, Error> {
    prepare_within(writer, source, input, now, MAX_PRODUCER_REGISTRY_BYTES)
}

/// [`prepare`] with an explicit byte bound, so a test can reach the byte-bound corner.
fn prepare_within(
    writer: &Writer,
    source: &str,
    input: &ProducerBatch,
    now: u64,
    max_bytes: usize,
) -> Result<Option<PreparedStream>, Error> {
    input.validate()?;
    let mut streams = registry(writer)?;
    streams.retain(|stream| stream.expires_at > now);
    let instance = super::hex::encode(&input.instance);
    let mut is_new = false;
    let index = match streams
        .iter()
        .position(|stream| stream.source == source && stream.instance == instance)
    {
        Some(index) => {
            if input.sequences[0] <= streams[index].last_sequence {
                return Err(Error::new(
                    "CONFLICT",
                    "producer sequences overlap accepted positions; retry the original request or use a new producer instance",
                ));
            }
            index
        }
        None => {
            if streams.len() == MAX_PRODUCER_STREAMS {
                return Ok(None);
            }
            is_new = true;
            streams.push(ProducerStream {
                id: random_id("producer-stream")?,
                source: source.to_owned(),
                instance,
                first_sequence: input.sequences[0],
                last_sequence: input.sequences[0] - 1,
                accepted_events: 0,
                unobserved_sequences: 0,
                gaps: Vec::new(),
                gaps_complete: true,
                first_received_at: now,
                last_received_at: now,
                expires_at: 0,
            });
            streams.len() - 1
        }
    };
    let state = &mut streams[index];
    for sequence in &input.sequences {
        let missing = sequence - state.last_sequence - 1;
        if missing > 0 {
            super::sequence_window::record_gap(
                &mut state.unobserved_sequences,
                &mut state.gaps,
                &mut state.gaps_complete,
                state.last_sequence + 1,
                sequence - 1,
            )?;
        }
        state.last_sequence = *sequence;
        state.accepted_events = state
            .accepted_events
            .checked_add(1)
            .ok_or_else(|| Error::new("RESOURCE_LIMIT", "producer accepted count overflow"))?;
    }
    state.last_received_at = now;
    state.expires_at = now
        .checked_add(30 * DAY)
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "producer metadata deadline overflow"))?;
    let id = state.id.clone();
    let Some(encoded) =
        super::sequence_window::encode_bounded(&mut streams, index, max_bytes, |stream| {
            // A new window never displaces recorded evidence; an existing one sheds its own
            // oldest intervals first.
            if is_new || stream.gaps.is_empty() {
                return false;
            }
            stream.gaps.remove(0);
            stream.gaps_complete = false;
            true
        })
    else {
        // Nothing left to shed: take the batch without a window update. The caller counts
        // the lost evidence; a later batch of this stream may then show those positions as
        // a gap. A log is never refused for bookkeeping (#215).
        return Ok(None);
    };
    Ok(Some(PreparedStream { id, encoded }))
}

impl super::sequence_window::Window for ProducerStream {
    const PREFIX: &'static str = "producer-stream:";
    fn metadata(&self) -> (&str, u64) {
        (&self.id, self.expires_at)
    }
}

impl Store {
    /// Read one retained producer window; record positions survive summary expiry.
    pub fn producer_stream(&self, id: &str, now: u64) -> Result<ProducerStream, Error> {
        super::sequence_window::retained::<ProducerStream>(self, id, now, registry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    /// A sealed producer can submit the missing prefix after reopening the store. Once
    /// GC retires its exact receipt, a backward clock can never admit it as new again.
    #[test]
    fn sealed_event_watermark_survives_restart_gc_and_clock_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparrow_test_support::storage_config!(Config);
        let now = 10_000;
        let deadline = Store::sealed_event_deadline(now);
        let producer = ProducerBatch {
            instance: [42; 16],
            sequences: vec![1],
        };
        let submit = |store: &Store, id, t, deadline, batch: &ProducerBatch| {
            store.ingest_events_before(
                id,
                "source",
                b"{}\n",
                batch,
                None,
                t,
                crate::Durability::Durable,
                Some(deadline),
            )
        };
        let store = Store::open(dir.path(), config.clone()).unwrap();
        drop(store);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        let original =
            |store: &Store, t| submit(store, "events:sealed:test", t, deadline, &producer);
        let first = original(&store, now).unwrap();
        drop(store);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        assert_eq!(original(&store, now + 1).unwrap().id, first.id);
        let later = now + super::super::RETRY_WINDOW + 10;
        assert_eq!(
            original(&store, later).unwrap_err().code,
            "UNCERTAIN_DELIVERY"
        );
        store.expire(later).unwrap();
        assert!(
            store
                .lock()
                .unwrap()
                .db
                .get("request:ingest:events:sealed:test")
                .unwrap()
                .is_none()
        );
        drop(store);
        let store = Store::open(dir.path(), config.clone()).unwrap();
        assert_eq!(
            original(&store, now + 1).unwrap_err().code,
            "UNCERTAIN_DELIVERY"
        );
        assert_eq!(store.artifacts(now + 1).unwrap().len(), 1);
        let next = ProducerBatch {
            instance: producer.instance,
            sequences: vec![2],
        };
        submit(
            &store,
            "events:sealed:next",
            later,
            Store::sealed_event_deadline(later),
            &next,
        )
        .unwrap();
        assert_eq!(store.artifacts(later).unwrap().len(), 2);
        // An upgraded writer cannot make a fresh assumption about missing GC evidence.
        store
            .lock()
            .unwrap()
            .db
            .delete(super::super::writer_format::RETRY_GC)
            .unwrap();
        drop(store);
        assert_eq!(
            Store::open(dir.path(), config).err().unwrap().code,
            "STORAGE_FAILURE"
        );
    }

    /// INVARIANT (#215): a registry at its byte bound never refuses a batch. An existing
    /// window first sheds its own oldest gap intervals; with none left, the batch is taken
    /// without a window update (`None`, counted by the caller). The old code answered
    /// RESOURCE_LIMIT in the second case.
    #[test]
    fn byte_bound_sheds_the_windows_gaps_then_takes_the_batch_without_a_window() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), sparrow_test_support::storage_config!(Config)).unwrap();
        let batch = ProducerBatch {
            instance: [7; 16],
            sequences: vec![2, 4, 6, 8],
        };
        store
            .ingest_producer(
                "seed",
                "source",
                &b"{}\n".repeat(4),
                &batch,
                None,
                1,
                crate::Durability::Durable,
            )
            .unwrap();
        let next = ProducerBatch {
            instance: [7; 16],
            sequences: vec![20],
        };
        let writer = store.lock().unwrap();
        let full = prepare_within(&writer, "source", &next, 2, MAX_PRODUCER_REGISTRY_BYTES)
            .unwrap()
            .unwrap();
        let tight = full.encoded.len() - 1;
        let shed = prepare_within(&writer, "source", &next, 2, tight)
            .unwrap()
            .unwrap();
        assert!(shed.encoded.len() <= tight);
        assert!(
            prepare_within(&writer, "source", &next, 2, 10)
                .unwrap()
                .is_none()
        );
    }
}
