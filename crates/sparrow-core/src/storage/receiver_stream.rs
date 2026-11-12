// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Finite durable syslog receiver observations, distinct from sender sequences.

use super::{Catalog, DAY, Error, Store, Writer, integer, json, random_id, read_field};
use purrdf_lex::json::{Object, Value};
use rocksdb::WriteBatch;

pub(super) const KEY: &str = "syslog-receiver-registry";
pub(super) const FORMAT: &[u8] = b"sparrow.store/40";
pub(super) const WATCH_FORMAT: &[u8] = b"sparrow.store/41";
/// Maximum retained receiver windows, independent of evidence pins.
pub const MAX_RECEIVER_WINDOWS: usize = 64;
/// Encoded admission before native registry decoding.
pub const MAX_RECEIVER_REGISTRY_BYTES: usize = 128 * 1024;

/// Receiver process/listener identity. A restart creates a fresh instance.
#[derive(Clone, Debug)]
pub struct SyslogReceiver {
    listener: String,
    instance: String,
    transport: String,
}
impl SyslogReceiver {
    /// Create one listener instance with an explicit framing/transport law.
    pub fn new(listener: &str, transport: &str) -> Result<Self, Error> {
        if listener.is_empty() || listener.len() > 128 || !valid_transport(transport) {
            return Err(Error::new(
                "INVALID_ARGUMENT",
                "receiver requires a bounded listener and udp/tcp_octet_counting/tcp_lf/tcp_cr_lf transport",
            ));
        }
        Ok(Self {
            listener: listener.into(),
            instance: random_id("receiver-instance")?,
            transport: transport.into(),
        })
    }
    pub(super) fn encode(&self) -> Value {
        Object::new()
            .with("listener", self.listener.clone())
            .with("instance", self.instance.clone())
            .with("transport", self.transport.clone())
            .into()
    }
}
fn valid_transport(value: &str) -> bool {
    matches!(value, "udp" | "tcp_octet_counting" | "tcp_lf" | "tcp_cr_lf")
}

/// Known local discard classification, not evidence of network or sender loss.
#[derive(Clone, Copy, Debug)]
pub enum ReceiverDiscard {
    /// Complete message refused by local admission/publication.
    Refused,
    /// Bytes of a received packet were cut. The kept prefix is published and this counts
    /// the cut: a TCP line above the listener's `max_message_bytes`, or an octet-counted
    /// frame above the store's syslog ceiling. Oversize that fits the ceiling is kept whole
    /// and is not counted.
    Truncated,
    /// TCP framing failed; no complete packet was asserted.
    FramingFailure,
    /// TCP frame deadline expired; no complete packet was asserted.
    TimedOut,
}

/// Immutable position in durable receiver observation commit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverPosition {
    /// Opaque finite window ID; its summary can expire independently.
    pub window_id: String,
    /// One-based durable observation ordinal, never a sender sequence.
    pub sequence: u64,
}
impl Catalog for ReceiverPosition {
    fn encode(&self) -> Value {
        Object::new()
            .with("window_id", self.window_id.clone())
            .with("sequence", self.sequence.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let result = Self {
            window_id: read_field(value, "window_id")?,
            sequence: integer(value, "sequence")?,
        };
        if !result.window_id.starts_with("syslog-receiver:")
            || result.window_id.len() > 128
            || result.sequence == 0
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "invalid receiver record position",
            ));
        }
        Ok(result)
    }
}

/// Finite committed observation history. Pre-commit and sender continuity remain unknown.
#[derive(Clone, Debug)]
pub struct ReceiverWindow {
    /// Opaque store-local identity.
    pub id: String,
    /// Exact record source.
    pub source: String,
    /// Configured listener ID at observation time.
    pub listener: String,
    /// Fresh listener process identity, preserved only within this runtime.
    pub instance: String,
    /// Explicit UDP or TCP framing law.
    pub transport: String,
    /// Count and final ordinal of durably classified observations.
    pub observations: u64,
    /// Published independent packet occurrences.
    pub accepted: u64,
    /// Classified observations without a published record.
    pub discarded: u64,
    /// Refused complete packets.
    pub refused: u64,
    /// Packets cut before publication: TCP lines above the listener's size, and octet-counted
    /// frames or broken frames above the store's syslog ceiling.
    pub truncated: u64,
    /// TCP framing failures.
    pub framing_failures: u64,
    /// TCP frame deadline failures.
    pub timed_out: u64,
    /// Recent inclusive local discard ranges, in observation order.
    pub gaps: Vec<super::producer_stream::ProducerGap>,
    /// Whether the ranges cover all classified discards.
    pub gaps_complete: bool,
    /// Earliest committed observation timestamp.
    pub first_received_at: u64,
    /// Latest committed observation timestamp.
    pub last_received_at: u64,
    /// Finite summary deadline; record pins never extend it.
    pub expires_at: u64,
}
impl Catalog for ReceiverWindow {
    fn encode(&self) -> Value {
        Object::new()
            .with("id", self.id.clone())
            .with("source", self.source.clone())
            .with("listener", self.listener.clone())
            .with("instance", self.instance.clone())
            .with("transport", self.transport.clone())
            .with("observations", self.observations.to_string())
            .with("accepted", self.accepted.to_string())
            .with("discarded", self.discarded.to_string())
            .with("refused", self.refused.to_string())
            .with("truncated", self.truncated.to_string())
            .with("framing_failures", self.framing_failures.to_string())
            .with("timed_out", self.timed_out.to_string())
            .with(
                "gaps",
                Value::array(self.gaps.iter().map(|gap| {
                    Object::new()
                        .with("first", gap.first.to_string())
                        .with("last", gap.last.to_string())
                })),
            )
            .with("gaps_complete", self.gaps_complete)
            .with("first_received_at", self.first_received_at.to_string())
            .with("last_received_at", self.last_received_at.to_string())
            .with("expires_at", self.expires_at.to_string())
            .into()
    }
    fn decode(value: &Value) -> Result<Self, Error> {
        let rows = super::field(value, "gaps")?
            .as_array()
            .ok_or_else(|| Error::new("STORAGE_FAILURE", "receiver gaps must be an array"))?;
        if rows.len() > super::producer_stream::MAX_PRODUCER_GAPS {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "receiver gap admission exceeded",
            ));
        }
        let gaps = rows
            .iter()
            .map(|row| {
                Ok(super::producer_stream::ProducerGap {
                    first: integer(row, "first")?,
                    last: integer(row, "last")?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let state = Self {
            id: read_field(value, "id")?,
            source: read_field(value, "source")?,
            listener: read_field(value, "listener")?,
            instance: read_field(value, "instance")?,
            transport: read_field(value, "transport")?,
            observations: integer(value, "observations")?,
            accepted: integer(value, "accepted")?,
            discarded: integer(value, "discarded")?,
            refused: integer(value, "refused")?,
            truncated: integer(value, "truncated")?,
            framing_failures: integer(value, "framing_failures")?,
            timed_out: integer(value, "timed_out")?,
            gaps,
            gaps_complete: read_field(value, "gaps_complete")?,
            first_received_at: integer(value, "first_received_at")?,
            last_received_at: integer(value, "last_received_at")?,
            expires_at: integer(value, "expires_at")?,
        };
        let mut previous = 0;
        let mut total = 0_u64;
        for gap in &state.gaps {
            if gap.first <= previous || gap.last < gap.first || gap.last > state.observations {
                return Err(Error::new(
                    "STORAGE_FAILURE",
                    "receiver discard ranges inconsistent",
                ));
            }
            previous = gap.last;
            total = total
                .checked_add(gap.last - gap.first + 1)
                .ok_or_else(|| Error::new("STORAGE_FAILURE", "receiver gap count overflow"))?;
        }
        if !state.id.starts_with("syslog-receiver:")
            || state.id.len() > 128
            || state.source.is_empty()
            || state.source.len() > 512
            || state.listener.is_empty()
            || state.listener.len() > 128
            || !state.instance.starts_with("receiver-instance:")
            || state.instance.len() > 128
            || !valid_transport(&state.transport)
            || state.observations == 0
            || state.accepted.checked_add(state.discarded) != Some(state.observations)
            || state
                .refused
                .checked_add(state.truncated)
                .and_then(|n| n.checked_add(state.framing_failures))
                .and_then(|n| n.checked_add(state.timed_out))
                != Some(state.discarded)
            || total > state.discarded
            || (state.gaps_complete && total != state.discarded)
            || state.first_received_at > state.last_received_at
            || state.last_received_at.checked_add(30 * DAY) != Some(state.expires_at)
        {
            return Err(Error::new(
                "STORAGE_FAILURE",
                "receiver window invariants inconsistent",
            ));
        }
        Ok(state)
    }
}

pub(super) fn registry(writer: &Writer) -> Result<Vec<ReceiverWindow>, Error> {
    let format = super::writer_format::read(&writer.db)?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "receiver store format missing"))?;
    let fenced = super::manifest_retire::features(&writer.db)?
        .unwrap_or(format.as_slice() == FORMAT || format.as_slice() == WATCH_FORMAT);
    match (fenced, writer.db.get(KEY)?.is_some()) {
        (false, false) => Ok(Vec::new()),
        (true, true) => super::sequence_window::registry(
            writer,
            KEY,
            MAX_RECEIVER_REGISTRY_BYTES,
            MAX_RECEIVER_WINDOWS,
            |window: &ReceiverWindow| {
                (
                    window.id.clone(),
                    (window.source.clone(), window.instance.clone()),
                )
            },
        ),
        _ => Err(Error::new(
            "STORAGE_FAILURE",
            "receiver registry and writer fence disagree; restore the catalog",
        )),
    }
}

pub(super) struct PreparedReceiver {
    pub position: ReceiverPosition,
    encoded: String,
}
impl PreparedReceiver {
    pub(super) fn publish(self, batch: &mut WriteBatch) {
        batch.put(KEY, self.encoded);
    }
}
pub(super) fn prepare(
    writer: &Writer,
    source: &str,
    receiver: &SyslogReceiver,
    discard: Option<ReceiverDiscard>,
    now: u64,
) -> Result<PreparedReceiver, Error> {
    if source.is_empty() || source.len() > 512 {
        return Err(Error::new(
            "INVALID_ARGUMENT",
            "receiver source must be 1..512 bytes",
        ));
    }
    let mut windows = registry(writer)?;
    windows.retain(|window| window.expires_at > now);
    let index = match windows
        .iter()
        .position(|window| window.source == source && window.instance == receiver.instance)
    {
        Some(index) => index,
        None => {
            if windows.len() == MAX_RECEIVER_WINDOWS {
                return Err(Error::new(
                    "RESOURCE_LIMIT",
                    "receiver window registry full; wait for finite metadata expiry",
                ));
            }
            windows.push(ReceiverWindow {
                id: random_id("syslog-receiver")?,
                source: source.into(),
                listener: receiver.listener.clone(),
                instance: receiver.instance.clone(),
                transport: receiver.transport.clone(),
                observations: 0,
                accepted: 0,
                discarded: 0,
                refused: 0,
                truncated: 0,
                framing_failures: 0,
                timed_out: 0,
                gaps: Vec::new(),
                gaps_complete: true,
                first_received_at: now,
                last_received_at: now,
                expires_at: 0,
            });
            windows.len() - 1
        }
    };
    let state = &mut windows[index];
    if state.listener != receiver.listener || state.transport != receiver.transport {
        return Err(Error::new("CONFLICT", "receiver instance binding changed"));
    }
    state.observations = state
        .observations
        .checked_add(1)
        .ok_or_else(|| Error::new("RESOURCE_LIMIT", "receiver observation sequence overflow"))?;
    if let Some(discard) = discard {
        let counter = match discard {
            ReceiverDiscard::Refused => &mut state.refused,
            ReceiverDiscard::Truncated => &mut state.truncated,
            ReceiverDiscard::FramingFailure => &mut state.framing_failures,
            ReceiverDiscard::TimedOut => &mut state.timed_out,
        };
        *counter = counter.checked_add(1).ok_or_else(|| {
            Error::new("RESOURCE_LIMIT", "receiver classification count overflow")
        })?;
        super::sequence_window::record_gap(
            &mut state.discarded,
            &mut state.gaps,
            &mut state.gaps_complete,
            state.observations,
            state.observations,
        )?;
    } else {
        state.accepted = state
            .accepted
            .checked_add(1)
            .ok_or_else(|| Error::new("RESOURCE_LIMIT", "receiver publication count overflow"))?;
    }
    state.first_received_at = state.first_received_at.min(now);
    state.last_received_at = state.last_received_at.max(now);
    state.expires_at = state
        .last_received_at
        .checked_add(30 * DAY)
        .ok_or_else(|| Error::new("INVALID_ARGUMENT", "receiver metadata deadline overflow"))?;
    let position = ReceiverPosition {
        window_id: state.id.clone(),
        sequence: state.observations,
    };
    let encoded = json::write_compact(&Value::array(windows.iter().map(Catalog::encode)));
    if encoded.len() > MAX_RECEIVER_REGISTRY_BYTES {
        return Err(Error::new(
            "RESOURCE_LIMIT",
            "receiver registry encoded admission exceeded",
        ));
    }
    Ok(PreparedReceiver { position, encoded })
}

impl super::sequence_window::Window for ReceiverWindow {
    const PREFIX: &'static str = "syslog-receiver:";
    fn metadata(&self) -> (&str, u64) {
        (&self.id, self.expires_at)
    }
}

impl Store {
    /// Read retained receiver history without renewing metadata or evidence lifetime.
    pub fn receiver_window(&self, id: &str, now: u64) -> Result<ReceiverWindow, Error> {
        super::sequence_window::retained::<ReceiverWindow>(self, id, now, registry)
    }
    /// Durably classify one local discard; no raw packet or successful prefix is created.
    pub fn discard_syslog(
        &self,
        source: &str,
        receiver: &SyslogReceiver,
        reason: ReceiverDiscard,
        now: u64,
    ) -> Result<ReceiverPosition, Error> {
        let writer = self.lock()?;
        let prepared = prepare(&writer, source, receiver, Some(reason), now)?;
        let position = prepared.position.clone();
        let mut batch = WriteBatch::default();
        prepared.publish(&mut batch);
        writer.admit_import_batch(&batch)?;
        writer.commit(batch)?;
        Ok(position)
    }
    /// Accept one packet and its receiver observation in the same synchronized batch.
    pub fn ingest_received_syslog(
        &self,
        source: &str,
        receiver: &SyslogReceiver,
        bytes: &[u8],
        now: u64,
    ) -> Result<super::Artifact, Error> {
        self.ingest_input(
            &random_id("syslog-intake")?,
            source,
            "syslog",
            bytes,
            super::ingest::Origin::Receiver(receiver),
            None,
            now,
            super::durability::Durability::Durable,
        )
    }
}
