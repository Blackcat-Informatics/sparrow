// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Bounded private producer log. One append worker and one sender share its short disk transactions.

use crate::{Error, EventBatch, ProducerEvent, spool_capnp as wire};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

/// Limits for one private, exclusively owned spool directory.
#[derive(Clone, Debug)]
pub struct SpoolConfig {
    /// Absolute private directory outside daemon consume roots.
    pub dir: PathBuf,
    /// Maximum total event segment bytes.
    pub max_bytes: u64,
    /// Target segment size. Capped at the total disk budget.
    pub segment_bytes: u64,
    /// Longest interval between synchronization of ordinary fast admissions.
    pub sync_interval: Duration,
    /// Longest wait for a confirmed caller to obtain intake capacity.
    pub max_wait: Duration,
}

impl SpoolConfig {
    /// A 1 GiB spool using 16 MiB segments.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_bytes: 1 << 30,
            segment_bytes: 16 << 20,
            sync_interval: Duration::from_secs(1),
            max_wait: Duration::from_millis(20),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !self.dir.is_absolute()
            || !(1 << 20..=1 << 40).contains(&self.max_bytes)
            || self.segment_bytes < 64 << 10
            || self.sync_interval.is_zero()
            || self.sync_interval > Duration::from_secs(60)
            || self.max_wait > Duration::from_secs(1)
        {
            return Err(Error::InvalidArgument(
                "spool requires an absolute path, 1 MiB..1 TiB budget, at least 64 KiB segments, a positive sync interval <=60 s and max wait <=1 s",
            ));
        }
        Ok(())
    }
}

/// Memory-only delivery or delivery through a private, bounded disk spool.
#[derive(Clone, Debug)]
pub enum DeliveryMode {
    /// Memory queue with exact refusal counters and permanent-input quarantine.
    LossyAspirational,
    /// Nonwaiting, disk-free offers with sequenced losses and explicit store rebinding.
    LossyMemory,
    /// Fast memory admission followed by asynchronous disk intake.
    LossySpooled(SpoolConfig),
    /// Supports both fast memory offers and explicit confirmed disk acceptance.
    LosslessConfirmed(SpoolConfig),
}

impl DeliveryMode {
    pub(crate) fn spool(&self) -> Option<&SpoolConfig> {
        match self {
            Self::LossyAspirational | Self::LossyMemory => None,
            Self::LossySpooled(config) | Self::LosslessConfirmed(config) => Some(config),
        }
    }
}

/// Observable local-disk progress, independent of the network worker's state.
#[derive(Clone, Debug, Default)]
pub struct SpoolStats {
    /// Number of the oldest request awaiting certification, including unsealed input.
    pub next_batch: u64,
    /// The remote confirmation is durable. Local segment reclamation is still due.
    pub cleanup_pending: bool,
    /// Wall clock of the oldest locally accepted event still awaiting a remote receipt.
    pub oldest_spooled_at_ms: Option<u64>,
    /// Number of the current immutable sealed request, if any.
    pub sealed_batch: Option<u64>,
    /// Highest locally appended sequence.
    pub spooled_sequence: u64,
    /// Highest sequence synchronized to the local filesystem.
    pub synced_sequence: u64,
    /// Total bytes occupied by segment files.
    pub bytes: u64,
    /// Number of retained segments.
    pub segments: usize,
    /// Incomplete final entries truncated on this startup.
    pub torn_tail: u64,
    /// Exact sealed requests recovered on startup.
    pub replayed_batches: u64,
    /// Latest accepted cursor, including after cleanup and restart.
    pub accepted_token: Vec<u8>,
    /// Cursor of the most recent remotely confirmed event.
    pub confirmed_token: Vec<u8>,
}

const FRAME_MAX: usize = 128 << 10;
const PREFIX: usize = 44;
const FORMAT: &str = "sparrow-spool/1";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    segment: u64,
    offset: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct Seal {
    pub(crate) last: u64,
    pub(crate) deadline: u64,
}

#[derive(Clone)]
pub(crate) struct Header {
    pub(crate) source: String,
    pub(crate) instance: [u8; 16],
    pub(crate) store: String,
    pub(crate) epoch: u32,
    pub(crate) confirmed: u64,
    token: Vec<u8>,
    batch: u64,
    cursor: Position,
    pub(crate) seal: Option<Seal>,
    artifact: String,
}

struct Entry {
    event: ProducerEvent,
    token: Vec<u8>,
    at: u64,
}

pub(crate) struct Prepared {
    pub(crate) batch: std::sync::Arc<EventBatch>,
    pub(crate) deadline: u64,
    end: Position,
    token: Vec<u8>,
}

pub(crate) struct Log {
    config: SpoolConfig,
    directory: File,
    _lock: crate::ExclusiveFileLock,
    pub(crate) head: Header,
    sizes: BTreeMap<u64, u64>,
    active: Option<(u64, File)>,
    appended_token: Vec<u8>,
    pub(crate) stats: SpoolStats,
}

fn options() -> OpenOptions {
    let mut result = OpenOptions::new();
    result
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    result
}

fn segment(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("seg-{index:020}"))
}

fn message_bytes(message: &capnp::message::Builder<capnp::message::HeapAllocator>) -> Vec<u8> {
    capnp::serialize::write_message_to_words(message)
}

fn decode<T: capnp::traits::Owned, R>(
    bytes: &[u8],
    f: impl for<'a> FnOnce(T::Reader<'a>) -> Result<R, Error>,
) -> Result<R, Error> {
    let mut input = bytes;
    let message = capnp::serialize::read_message(
        &mut input,
        capnp::message::ReaderOptions {
            traversal_limit_in_words: Some(FRAME_MAX),
            nesting_limit: 32,
        },
    )?;
    if !input.is_empty() {
        return Err(Error::Protocol("trailing bytes in spool message"));
    }
    f(message.get_root::<T::Reader<'_>>()?)
}

impl Header {
    fn encode(&self) -> Vec<u8> {
        let mut message = capnp::message::Builder::new_default();
        let mut out = message.init_root::<wire::header::Builder<'_>>();
        out.set_format(FORMAT);
        out.set_source(&self.source);
        out.set_instance(&self.instance);
        out.set_store(&self.store);
        out.set_span_epoch(self.epoch);
        out.set_confirmed(self.confirmed);
        out.set_confirmed_token(&self.token);
        out.set_next_batch(self.batch);
        out.set_artifact(&self.artifact);
        let mut cursor = out.reborrow().init_cursor();
        cursor.set_segment(self.cursor.segment);
        cursor.set_offset(self.cursor.offset);
        if let Some(seal) = &self.seal {
            let mut out = out.init_seal();
            out.set_last_sequence(seal.last);
            out.set_deadline(seal.deadline);
        }
        message_bytes(&message)
    }

    fn read(file: File) -> Result<Self, Error> {
        let mut bytes = Vec::new();
        file.take(8193).read_to_end(&mut bytes)?;
        if bytes.len() > 8192 {
            return Err(Error::Protocol("oversized spool header"));
        }
        let (checksum, body) = bytes
            .split_at_checked(32)
            .ok_or(Error::Protocol("truncated spool checkpoint"))?;
        if purrdf_hash::blake3::hash(body).as_bytes() != checksum {
            return Err(Error::Protocol("corrupt spool checkpoint checksum"));
        }
        decode::<wire::header::Owned, _>(body, |value| {
            if value.get_format()?.to_str()? != FORMAT {
                return Err(Error::Unsupported("unrecognized spool format"));
            }
            let cursor = value.get_cursor()?;
            let seal = value.get_seal()?;
            let store = value.get_store()?.to_str()?.to_owned();
            let source = value.get_source()?.to_str()?;
            let confirmed = value.get_confirmed();
            let batch = value.get_next_batch();
            let token = value.get_confirmed_token()?.to_vec();
            if !(1..=512).contains(&source.len())
                || batch == 0
                || value.get_span_epoch() == 0
                || token.len() > 256
                || value.has_seal()
                    && (seal.get_last_sequence() <= confirmed
                        || seal.get_deadline() == 0
                        || store.is_empty())
            {
                return Err(Error::Protocol("inconsistent spool checkpoint or seal"));
            }
            Ok(Self {
                source: source.to_owned(),
                instance: value
                    .get_instance()?
                    .try_into()
                    .map_err(|_| Error::Protocol("invalid spool instance"))?,
                store,
                epoch: value.get_span_epoch(),
                confirmed,
                token,
                batch,
                artifact: value.get_artifact()?.to_str()?.to_owned(),
                cursor: Position {
                    segment: cursor.get_segment(),
                    offset: cursor.get_offset(),
                },
                seal: value.has_seal().then(|| Seal {
                    last: seal.get_last_sequence(),
                    deadline: seal.get_deadline(),
                }),
            })
        })
    }
}

enum Frame {
    End,
    Torn,
    Entry(Entry, u64),
}

fn read_frame(reader: &mut impl Read) -> Result<Frame, Error> {
    let mut prefix = [0; PREFIX];
    if reader.read(&mut prefix[..1])? == 0 {
        return Ok(Frame::End);
    }
    if let Err(err) = reader.read_exact(&mut prefix[1..]) {
        return if err.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(Frame::Torn)
        } else {
            Err(err.into())
        };
    }
    let size = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
    if &prefix[4..8] != b"SP01"
        || prefix[8..12] != (!(size as u32)).to_le_bytes()
        || !(8..=FRAME_MAX).contains(&size)
    {
        return Err(Error::Protocol("corrupt spool frame prefix"));
    }
    let mut body = vec![0; size];
    if let Err(err) = reader.read_exact(&mut body) {
        return if err.kind() == std::io::ErrorKind::UnexpectedEof {
            Ok(Frame::Torn)
        } else {
            Err(err.into())
        };
    }
    if purrdf_hash::blake3::hash(&body).as_bytes() != &prefix[12..] {
        return Err(Error::Protocol("corrupt spool frame checksum"));
    }
    let entry = decode::<wire::entry::Owned, _>(&body, |value| {
        let token = value.get_token()?;
        if token.len() > 256 {
            return Err(Error::Protocol("oversized spool cursor token"));
        }
        let (event, _) = ProducerEvent::decode(value.get_event()?, crate::MAX_EVENT_BYTES)?;
        event.admission_bytes()?;
        Ok(Entry {
            event,
            token: token.to_vec(),
            at: value.get_at(),
        })
    })?;
    Ok(Frame::Entry(entry, (size + PREFIX) as u64))
}

impl Log {
    fn entry_message(
        event: &ProducerEvent,
        token: &[u8],
        at: u64,
    ) -> capnp::message::Builder<capnp::message::HeapAllocator> {
        let mut msg = capnp::message::Builder::new_default();
        let mut out = msg.init_root::<wire::entry::Builder<'_>>();
        out.set_at(at);
        out.set_token(token);
        event.encode(out.init_event());
        msg
    }

    /// Sequence and timestamp occupy fixed-width slots, so this is also the final append size.
    pub(crate) fn frame_bytes(event: &ProducerEvent, token: &[u8]) -> u64 {
        let msg = Self::entry_message(event, token, 0);
        (PREFIX + 8 * capnp::serialize::compute_serialized_size_in_words(&msg)) as u64
    }
    pub(crate) fn open(
        config: &SpoolConfig,
        source: &str,
        instance: [u8; 16],
    ) -> Result<Self, Error> {
        Self::acquire(config, Some((source, instance)))
    }

    /// Acquire an existing spool using its verified source and persistent identity.
    pub(crate) fn existing(config: &SpoolConfig) -> Result<Self, Error> {
        Self::acquire(config, None)
    }

    fn acquire(config: &SpoolConfig, producer: Option<(&str, [u8; 16])>) -> Result<Self, Error> {
        config.validate()?;
        if producer.is_some() {
            crate::quarantine::create_dir(&config.dir)?;
        }
        let directory = options().read(true).open(&config.dir)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(Error::InvalidArgument(
                "spool directory must be private (mode 0700) and cannot be a symlink",
            ));
        }
        let lock = options()
            .read(true)
            .write(true)
            .create(producer.is_some())
            .truncate(false)
            .open(config.dir.join("LOCK"))?;
        let lock = crate::ExclusiveFileLock::acquire(lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                std::io::Error::new(
                    error.kind(),
                    format!("spool already open: {}", config.dir.display()),
                )
            } else {
                error
            }
        })?;
        let mut sizes = BTreeMap::new();
        for entry in fs::read_dir(&config.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or(Error::Protocol("invalid spool file name"))?;
            if matches!(name, "HEADER" | "LOCK" | "HEADER.new") {
                continue;
            }
            let index = name
                .strip_prefix("seg-")
                .filter(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(Error::Protocol(
                    "unexpected file in private spool directory",
                ))?;
            if !entry.file_type()?.is_file() {
                return Err(Error::Protocol("spool segment is not a regular file"));
            }
            sizes.insert(index, entry.metadata()?.len());
        }
        let mut head = match options().read(true).open(config.dir.join("HEADER")) {
            Ok(file) => Header::read(file)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && sizes.is_empty() => {
                let Some((source, instance)) = producer else {
                    return Err(error.into());
                };
                Header {
                    source: source.to_owned(),
                    instance,
                    store: String::new(),
                    epoch: 0,
                    confirmed: 0,
                    token: vec![],
                    batch: 1,
                    cursor: Position::default(),
                    seal: None,
                    artifact: String::new(),
                }
            }
            Err(error) => return Err(error.into()),
        };
        if producer.is_some_and(|(source, _)| head.source != source) {
            return Err(Error::InvalidArgument(
                "spool belongs to a different source",
            ));
        }
        if producer.is_some() {
            head.epoch = head
                .epoch
                .checked_add(1)
                .ok_or(Error::Protocol("spool span epoch exhausted"))?;
        }
        let stats = SpoolStats {
            next_batch: head.batch,
            spooled_sequence: head.confirmed,
            synced_sequence: head.confirmed,
            sealed_batch: head.seal.as_ref().map(|_| head.batch),
            replayed_batches: u64::from(head.seal.is_some()),
            accepted_token: head.token.clone(),
            confirmed_token: head.token.clone(),
            ..SpoolStats::default()
        };
        let mut log = Self {
            config: config.clone(),
            directory,
            _lock: lock,
            appended_token: head.token.clone(),
            head,
            sizes,
            active: None,
            stats,
        };
        log.recover()?;
        log.checkpoint(&log.head)?;
        log.cleanup()?;
        Ok(log)
    }

    fn checkpoint(&self, head: &Header) -> Result<(), Error> {
        let path = self.config.dir.join("HEADER.new");
        let mut file = options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let body = head.encode();
        file.write_all(purrdf_hash::blake3::hash(&body).as_bytes())?;
        file.write_all(&body)?;
        file.sync_all()?;
        fs::rename(path, self.config.dir.join("HEADER"))?;
        self.directory.sync_all()?;
        Ok(())
    }

    fn recover(&mut self) -> Result<(), Error> {
        let end_segment = self.sizes.last_key_value().map(|(index, _)| *index);
        let mut expected_segment = self.head.cursor.segment;
        let mut saw_cursor = self.head.cursor.offset == 0;
        for (&index, size) in &mut self.sizes {
            if index >= self.head.cursor.segment {
                if index != expected_segment {
                    return Err(Error::Protocol("gap in producer spool segments"));
                }
                expected_segment = index
                    .checked_add(1)
                    .ok_or(Error::Protocol("spool segment index exhausted"))?;
            }
            let file = options()
                .read(true)
                .write(true)
                .open(segment(&self.config.dir, index))?;
            let mut reader = BufReader::new(&file);
            let mut offset = 0;
            loop {
                let pos = Position {
                    segment: index,
                    offset,
                };
                if pos == self.head.cursor {
                    saw_cursor = true;
                }
                match read_frame(&mut reader)? {
                    Frame::End => break,
                    Frame::Torn if Some(index) == end_segment && pos >= self.head.cursor => {
                        file.set_len(offset)?;
                        file.sync_all()?;
                        self.stats.torn_tail += 1;
                        *size = offset;
                        break;
                    }
                    Frame::Torn => {
                        return Err(Error::Protocol("incomplete interior producer spool frame"));
                    }
                    Frame::Entry(entry, length) => {
                        if pos >= self.head.cursor {
                            if self.stats.spooled_sequence.checked_add(1)
                                != Some(entry.event.sequence)
                            {
                                return Err(Error::Protocol(
                                    "nonconsecutive spooled event sequence",
                                ));
                            }
                            self.stats.spooled_sequence = entry.event.sequence;
                            self.stats.oldest_spooled_at_ms.get_or_insert(entry.at);
                            self.appended_token = entry.token;
                        } else if entry.event.sequence > self.head.confirmed {
                            return Err(Error::Protocol(
                                "spool checkpoint skips an unconfirmed event",
                            ));
                        }
                        offset += length;
                    }
                }
            }
            self.stats.bytes = self
                .stats
                .bytes
                .checked_add(*size)
                .ok_or(Error::Protocol("spool length overflow"))?;
            if Some(index) == end_segment && index >= self.head.cursor.segment {
                drop(file);
                self.active = Some((
                    index,
                    options()
                        .append(true)
                        .open(segment(&self.config.dir, index))?,
                ));
            }
        }
        if !saw_cursor
            || self
                .head
                .seal
                .as_ref()
                .is_some_and(|seal| seal.last > self.stats.spooled_sequence)
        {
            return Err(Error::Protocol(
                "spool checkpoint points past recoverable data",
            ));
        }
        self.stats.segments = self.sizes.len();
        self.sync()
    }

    fn cleanup(&mut self) -> Result<(), Error> {
        while let Some((&index, &size)) = self.sizes.first_key_value() {
            if index >= self.head.cursor.segment {
                break;
            }
            if self
                .active
                .as_ref()
                .is_some_and(|(active, _)| *active == index)
            {
                self.active = None;
            }
            fs::remove_file(segment(&self.config.dir, index))?;
            self.sizes.remove(&index);
            self.stats.bytes -= size;
        }
        self.stats.segments = self.sizes.len();
        self.directory.sync_all()?;
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<(), Error> {
        if let Some((_, file)) = &self.active {
            file.sync_data()?;
        }
        self.stats.synced_sequence = self.stats.spooled_sequence;
        self.stats.accepted_token.clone_from(&self.appended_token);
        Ok(())
    }

    pub(crate) fn append(
        &mut self,
        event: &ProducerEvent,
        token: &[u8],
        at: u64,
    ) -> Result<(), Error> {
        if self.stats.spooled_sequence.checked_add(1) != Some(event.sequence) || token.len() > 256 {
            return Err(Error::Protocol("invalid local spool append order or token"));
        }
        let body = message_bytes(&Self::entry_message(event, token, at));
        let size = (PREFIX + body.len()) as u64;
        if body.len() > FRAME_MAX || size > self.config.max_bytes.saturating_sub(self.stats.bytes) {
            return Err(Error::Protocol(
                "admitted spool frame exceeds its reserved disk budget",
            ));
        }
        let mut frame = Vec::with_capacity(size as usize);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(b"SP01");
        frame.extend_from_slice(&(!(body.len() as u32)).to_le_bytes());
        frame.extend_from_slice(purrdf_hash::blake3::hash(&body).as_bytes());
        frame.extend_from_slice(&body);
        let rotate = match self.sizes.last_key_value() {
            Some((_, len)) => {
                *len > 0
                    && len.saturating_add(size)
                        > self.config.segment_bytes.min(self.config.max_bytes)
            }
            None => true,
        };
        if self.active.is_none() || rotate {
            self.sync()?;
            let index = match self.sizes.last_key_value() {
                Some((&i, _)) => i
                    .checked_add(1)
                    .ok_or(Error::Protocol("spool segment index exhausted"))?,
                None => self.head.cursor.segment,
            };
            let file = options()
                .append(true)
                .create_new(true)
                .open(segment(&self.config.dir, index))?;
            self.directory.sync_all()?;
            self.sizes.insert(index, 0);
            self.active = Some((index, file));
        }
        let (index, file) = self
            .active
            .as_mut()
            .ok_or(Error::Protocol("no active spool segment"))?;
        file.write_all(&frame)?;
        *self
            .sizes
            .get_mut(index)
            .ok_or(Error::Protocol("missing active segment length"))? += size;
        self.stats.bytes += size;
        self.stats.segments = self.sizes.len();
        self.stats.spooled_sequence = event.sequence;
        self.stats.oldest_spooled_at_ms.get_or_insert(at);
        self.appended_token.clear();
        self.appended_token.extend_from_slice(token);
        Ok(())
    }

    pub(crate) fn rebind(&mut self, store: &str) -> Result<(), Error> {
        if self.head.seal.is_some() {
            return Err(Error::InvalidArgument(
                "cannot rebind a sealed batch of uncertain delivery to a different store",
            ));
        }
        if !crate::receipt::valid_store_id(store) {
            return Err(Error::InvalidArgument("invalid persistent store id"));
        }
        let mut next = self.head.clone();
        next.store = store.to_owned();
        self.checkpoint(&next)?;
        self.head = next;
        Ok(())
    }

    pub(crate) fn prepare(
        &mut self,
        store: &str,
        deadline: u64,
        limits: crate::events::BatchLimits,
    ) -> Result<Option<Prepared>, Error> {
        if !self.head.store.is_empty() && self.head.store != store {
            return Err(Error::Domain {
                code: "STORE_MISMATCH".into(),
                message: format!(
                    "spool is bound to {}, connected to {store}",
                    self.head.store
                ),
                retryable: false,
                class: crate::FailureClass::Local,
            });
        }
        if self.stats.spooled_sequence == self.head.confirmed {
            return Ok(None);
        }
        if deadline == 0 {
            return Err(Error::Unsupported(
                "configured spool requires conditional sealed event ingestion",
            ));
        }
        let request_id = format!(
            "events:{}:{:020}",
            purrdf_hash::hex::encode(&self.head.instance),
            self.head.batch
        );
        let mut batch = EventBatch {
            request_id,
            source: self.head.source.clone(),
            client_instance: self.head.instance,
            events: Vec::new(),
        };
        let mut charge = 1024 + 6 * (batch.request_id.len() + batch.source.len());
        let mut end = self.head.cursor;
        let mut token = Vec::new();
        let sealed = self.head.seal.as_ref();
        'segments: for (&index, &size) in self.sizes.range(self.head.cursor.segment..) {
            let mut file = options()
                .read(true)
                .open(segment(&self.config.dir, index))?;
            let offset = if index == self.head.cursor.segment {
                self.head.cursor.offset
            } else {
                0
            };
            file.seek(SeekFrom::Start(offset))?;
            let mut reader = BufReader::new(file);
            let mut offset = offset;
            while offset < size {
                let Frame::Entry(entry, len) = read_frame(&mut reader)? else {
                    return Err(Error::Protocol("spool entry lost before delivery"));
                };
                let bytes = entry.event.admission_bytes()?;
                if sealed.is_none()
                    && !batch.events.is_empty()
                    && (batch.events.len() >= limits.events
                        || charge + bytes > limits.payload_bytes.min(limits.bytes))
                {
                    break 'segments;
                }
                let sequence = entry.event.sequence;
                batch.events.push(entry.event);
                if batch.events.len() > crate::MAX_EVENT_BATCH_EVENTS {
                    return Err(Error::Protocol(
                        "sealed spool request exceeds native count ceiling",
                    ));
                }
                token = entry.token;
                charge += bytes;
                offset += len;
                end = Position {
                    segment: index,
                    offset,
                };
                if sealed.is_some_and(|seal| seal.last == sequence) {
                    break 'segments;
                }
            }
        }
        let last = batch
            .events
            .last()
            .ok_or(Error::Protocol("spool checkpoint has no next event"))?
            .sequence;
        if sealed.is_some_and(|seal| seal.last != last) {
            return Err(Error::Protocol("sealed spool range is incomplete"));
        }
        let seal = match sealed {
            Some(seal) => seal.clone(),
            None => {
                // Validate before sealing: unsendable input remains on disk with a visible hold.
                batch.source_bytes_with_limits(limits)?;
                self.sync()?;
                let mut next = self.head.clone();
                next.store = store.to_owned();
                let seal = Seal { last, deadline };
                next.seal = Some(seal.clone());
                self.checkpoint(&next)?;
                self.head = next;
                self.stats.sealed_batch = Some(self.head.batch);
                seal
            }
        };
        Ok(Some(Prepared {
            batch: std::sync::Arc::new(batch),
            deadline: seal.deadline,
            end,
            token,
        }))
    }

    pub(crate) fn confirm(&mut self, prepared: &Prepared, artifact: &str) -> Result<(), Error> {
        let seal = self
            .head
            .seal
            .as_ref()
            .ok_or(Error::Protocol("missing spool confirmation seal"))?;
        let mut next = self.head.clone();
        next.confirmed = seal.last;
        next.token = prepared.token.clone();
        next.artifact = artifact.to_owned();
        next.batch = next
            .batch
            .checked_add(1)
            .ok_or(Error::Protocol("spool batch id exhausted"))?;
        next.cursor = prepared.end;
        if self.sizes.get(&next.cursor.segment) == Some(&next.cursor.offset) {
            next.cursor = Position {
                segment: next
                    .cursor
                    .segment
                    .checked_add(1)
                    .ok_or(Error::Protocol("spool cursor exhausted"))?,
                offset: 0,
            };
        }
        next.seal = None;
        // A failed directory sync can leave the new HEADER visible after rename.
        // Never append behind either recoverable cursor, even when checkpoint returns an error.
        if self
            .active
            .as_ref()
            .is_some_and(|(index, _)| *index < next.cursor.segment)
        {
            self.active = None;
        }
        self.checkpoint(&next)?;
        self.head = next;
        self.stats.confirmed_token = prepared.token.clone();
        self.stats.next_batch = self.head.batch;
        self.stats.sealed_batch = None;
        self.stats.cleanup_pending = true;
        Ok(())
    }

    pub(crate) fn finish_confirmation(&mut self) -> Result<(), Error> {
        self.cleanup()?;
        self.stats.oldest_spooled_at_ms = if self.stats.spooled_sequence == self.head.confirmed {
            None
        } else {
            let mut file = options()
                .read(true)
                .open(segment(&self.config.dir, self.head.cursor.segment))?;
            file.seek(SeekFrom::Start(self.head.cursor.offset))?;
            let Frame::Entry(entry, _) = read_frame(&mut BufReader::new(file))? else {
                return Err(Error::Protocol(
                    "confirmed spool cursor lost the next event",
                ));
            };
            Some(entry.at)
        };
        self.stats.cleanup_pending = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Delivery, DeliveryConfig, Endpoint, EventField, EventKind, EventLevel, FieldValue, Refusal,
        SendError,
    };

    /// A cleanup failure after a durable confirmation cannot replay the accepted batch
    /// or delete an event appended while reclamation is being retried.
    #[test]
    fn failed_confirmation_cleanup_preserves_the_next_append_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = SpoolConfig::new(dir.path().join("private"));
        let store = "store:00000000000000000000000000000001";
        let limits = crate::events::BatchLimits::NATIVE;
        let mut log = Log::open(&config, "app", [1; 16]).unwrap();
        let mut event = ProducerEvent {
            sequence: 1,
            event_time: 1,
            kind: EventKind::Log,
            level: EventLevel::Info,
            name: "accepted".into(),
            target: "app".into(),
            fields: vec![],
            context: vec![],
            span: None,
            follows_from: None,
        };
        log.append(&event, b"first", 1).unwrap();
        let prepared = log.prepare(store, 20, limits).unwrap().unwrap();
        let inherited = log._lock.as_ref().try_clone().unwrap();
        drop(log);
        let mut log = Log::open(&config, "app", [3; 16]).unwrap();
        drop(inherited);
        let replacement = "store:00000000000000000000000000000002";
        assert!(
            matches!(log.prepare(replacement, 30, limits), Err(Error::Domain {code,..}) if code == "STORE_MISMATCH")
        );
        assert!(log.rebind(replacement).is_err());
        let replay = log.prepare(store, 30, limits).unwrap().unwrap();
        assert_eq!(replay.deadline, prepared.deadline);
        assert_eq!(replay.batch, prepared.batch);
        let checkpoint = config.dir.join("HEADER.new");
        fs::create_dir(&checkpoint).unwrap();
        assert!(log.confirm(&prepared, "artifact:first").is_err());
        event.sequence = 2;
        log.append(&event, b"second", 2).unwrap();
        let second = event.clone();
        fs::remove_dir(checkpoint).unwrap();
        log.confirm(&prepared, "artifact:first").unwrap();
        let retired = segment(&config.dir, 0);
        let saved = dir.path().join("saved");
        fs::rename(&retired, &saved).unwrap();
        fs::create_dir(&retired).unwrap();
        assert!(log.finish_confirmation().is_err());
        assert!(log.stats.cleanup_pending);
        event.sequence = 3;
        log.append(&event, b"third", 3).unwrap();
        fs::remove_dir(&retired).unwrap();
        fs::rename(&saved, &retired).unwrap();
        log.finish_confirmation().unwrap();
        assert!(!retired.exists());
        assert_eq!(log.stats.oldest_spooled_at_ms, Some(2));
        drop(log);
        let mut log = Log::open(&config, "app", [2; 16]).unwrap();
        assert_eq!(log.stats.confirmed_token, b"first");
        assert_eq!(log.stats.accepted_token, b"third");
        let next = log.prepare(store, 30, limits).unwrap().unwrap();
        assert_eq!(next.batch.events.as_slice(), [second, event]);
        assert_ne!(next.batch.request_id, prepared.batch.request_id);
    }

    /// Full disk refuses further confirmed input without consuming its cursor. A crash tear
    /// is repairable only at the tail. A checksum failure inside a complete frame is fatal.
    #[tokio::test]
    async fn bounded_offline_spool_distinguishes_capacity_torn_tail_and_corrupt_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = SpoolConfig::new(dir.path().join("private"));
        spool.max_bytes = 1 << 20;
        spool.segment_bytes = 256 << 10;
        let config = DeliveryConfig::lossless(
            Endpoint::Unix(dir.path().join("absent.sock")),
            "bounded",
            spool.clone(),
        );
        let delivery = Delivery::start_async(config.clone()).await.unwrap();
        assert!(
            matches!(Delivery::start_async(config.clone()).await, Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::WouldBlock)
        );
        let mut count = 0u64;
        loop {
            let event = ProducerEvent {
                sequence: 0,
                event_time: 1,
                kind: EventKind::Log,
                level: EventLevel::Info,
                name: "offline".into(),
                target: "sdk".into(),
                fields: vec![EventField {
                    name: "message".into(),
                    value: FieldValue::Text("x".repeat(5000)),
                }],
                context: vec![],
                span: None,
                follows_from: None,
            };
            match delivery
                .send_with_token(event, &(count + 1).to_le_bytes())
                .await
            {
                Ok(receipt) => {
                    count += 1;
                    assert_eq!(count, receipt.sequence);
                }
                Err(SendError::Refused {
                    reason: Refusal::SpoolFull,
                    event,
                }) => {
                    assert_eq!(event.name, "offline");
                    assert_eq!(
                        delivery.stats().unwrap().dropped_by_reason
                            [crate::DropReason::Full as usize],
                        0
                    );
                    // A tracing or console callback has no upstream owner to reclaim the refusal.
                    let mut event = *event;
                    event.name.push_str(" captured");
                    delivery.observe(event);
                    break;
                }
                other => panic!("unexpected offline admission {other:?}"),
            }
        }
        assert!(count > 1 && delivery.stats().unwrap().spool.unwrap().bytes <= spool.max_bytes);
        assert_eq!(
            delivery.stats().unwrap().refused_by_reason[Refusal::SpoolFull as usize],
            2
        );
        assert_eq!(
            delivery.stats().unwrap().dropped_by_reason[crate::DropReason::Full as usize],
            1
        );
        assert_eq!(
            delivery.close_local(Duration::from_secs(5)).await.unwrap(),
            count
        );
        let mut segments: Vec<_> = fs::read_dir(&spool.dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("seg-"))
            .collect();
        segments.sort();
        let tail = segments.last().unwrap();
        let old = tail.metadata().unwrap().len();
        options()
            .append(true)
            .open(tail)
            .unwrap()
            .write_all(&[8, 0])
            .unwrap();
        let recovered = Delivery::start_async(config.clone()).await.unwrap();
        assert_eq!(recovered.stats().unwrap().spool.unwrap().torn_tail, 1);
        assert_eq!(recovered.accepted_token().unwrap(), count.to_le_bytes());
        assert_eq!(tail.metadata().unwrap().len(), old);
        recovered.close_local(Duration::from_secs(5)).await.unwrap();
        let mut file = options().write(true).open(&segments[0]).unwrap();
        file.seek(SeekFrom::Start(12)).unwrap();
        file.write_all(&[0; 32]).unwrap();
        assert!(matches!(
            Delivery::start_async(config).await,
            Err(Error::Protocol("corrupt spool frame checksum"))
        ));
    }
}
