// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The payload lookup index, `DIR/payload.index`: derived data rebuilt from the segments alone.
//!
//! The index names, for each payload, the artifact and segment that hold it, and the digest and
//! length of every segment it covers. A lookup uses it only when the index is valid, was built
//! for the root's current store identity, and covers exactly the segment files on disk. For each
//! covered segment the lookup reads the whole file and compares its length and digest with the
//! index: a match trusts the index entries for that segment, and a mismatch decodes the segment
//! in full, so a damaged segment fails the lookup by name exactly as in the scan. Any other
//! condition (no index, a damaged index, another store, an uncovered or missing segment) makes
//! the lookup a full scan and says why.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use purrdf::gts::writer::digest_string;

use crate::error::ArchiveError;
use crate::payload::{PayloadId, PayloadMatch, find_payload};
use crate::segment::{Segment, io, list_segments, store_binding};

/// The index file's name, directly under the archive root.
pub const INDEX_FILE: &str = "payload.index";
const TEMP_FILE: &str = "payload.index.tmp";
const MAGIC: &[u8; 8] = b"SPAPIDX1";
const FORMAT_VERSION: u32 = 1;
/// `blake3:` and 64 hex digits, the length of a digest string.
const DIGEST_LEN: usize = 71;

/// The outcome of one lookup and how it was reached.
#[derive(Debug)]
pub struct Lookup {
    /// Every artifact whose metadata names the payload, in segment path order.
    pub matches: Vec<PayloadMatch>,
    /// How the answer was reached.
    pub method: Method,
}

/// How a lookup reached its answer.
#[derive(Debug, PartialEq, Eq)]
pub enum Method {
    /// The index was valid and covered exactly the segments on disk. `decoded` counts covered
    /// segments whose bytes no longer matched the index, which were decoded in full.
    Index {
        /// Covered segments decoded because their length or digest changed.
        decoded: usize,
    },
    /// The lookup decoded every segment, for the stated reason.
    Scan {
        /// Why the index was not used.
        reason: String,
    },
}

/// Counts reported by [`build_index`].
#[derive(Debug, PartialEq, Eq)]
pub struct BuildSummary {
    /// Segments covered by the index.
    pub segments: usize,
    /// Entries, one per artifact per segment that names it.
    pub entries: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Covered {
    name: String,
    length: u64,
    digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    payload_id: String,
    artifact_id: String,
    segment: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Index {
    /// The `STORE_ID` the root named when the index was built, or `None` for an unbound root.
    store: Option<String>,
    covered: Vec<Covered>,
    entries: Vec<Entry>,
}

/// A segment's name relative to the archive root, the form the index stores.
fn relative_name(root: &Path, path: &Path) -> Result<String, ArchiveError> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| ArchiveError::Incomplete {
            reason: format!("segment name {} is not UTF-8", relative.display()),
        })
}

/// Write `DIR/payload.index` from the segments under `root`. Every segment is decoded first, so
/// a damaged segment refuses the build and nothing is written. The index is written to a
/// temporary file and renamed over any previous index.
///
/// # Errors
/// A segment that cannot be read or decoded, a segment name that is not UTF-8, or a write that
/// fails. On error the previous index, if any, is left as it was.
pub fn build_index(root: &Path) -> Result<BuildSummary, ArchiveError> {
    let paths = list_segments(root)?;
    let store = store_binding(root)?;
    let mut covered = Vec::with_capacity(paths.len());
    let mut entries = Vec::new();
    for path in &paths {
        let bytes = fs::read(path).map_err(io(path))?;
        let segment = Segment::from_bytes(path, &bytes)?;
        let name = relative_name(root, path)?;
        for artifact in segment.artifacts() {
            entries.push(Entry {
                payload_id: artifact.payload_id.clone(),
                artifact_id: artifact.id.clone(),
                segment: name.clone(),
            });
        }
        covered.push(Covered {
            name,
            length: bytes.len() as u64,
            digest: segment.digest().to_owned(),
        });
    }
    entries.sort();
    let summary = BuildSummary {
        segments: covered.len(),
        entries: entries.len(),
    };
    let encoded = encode(&Index {
        store,
        covered,
        entries,
    });
    let temp = root.join(TEMP_FILE);
    fs::write(&temp, &encoded).map_err(io(&temp))?;
    let target = root.join(INDEX_FILE);
    if let Err(source) = fs::rename(&temp, &target) {
        let _ = fs::remove_file(&temp);
        return Err(ArchiveError::Io {
            path: target,
            source,
        });
    }
    Ok(summary)
}

/// Every artifact among the segments under `root` whose payload identity is `payload_id`, using
/// the index where it is valid, was built for the root's store identity, and covers the segments
/// on disk. Nothing is written.
///
/// What is read: the root's `STORE_ID`, the index file in full, and the directory listing. For each covered segment the
/// whole file is read and hashed, because damage anywhere in a covered segment must fail the
/// lookup, including damage in a record the index does not name. The index saves the decoding
/// of intact segments, not their reads. A segment whose length or digest differs from the index
/// is decoded in full, and a decode failure fails the lookup naming that segment.
///
/// # Errors
/// A `STORE_ID` that cannot be read or is not a store identity, a segment that cannot be read,
/// or a segment that fails to decode when the index does not cover it or disagrees with it.
pub fn lookup_payload(root: &Path, payload_id: &PayloadId) -> Result<Lookup, ArchiveError> {
    let paths = list_segments(root)?;
    let store = store_binding(root)?;
    let index = match read_index(root) {
        Ok(index) => index,
        Err(reason) => return scan(&paths, payload_id, reason),
    };
    if index.store != store {
        return scan(
            &paths,
            payload_id,
            format!(
                "the index was built for {}, and the root now names {}",
                describe_store(index.store.as_deref()),
                describe_store(store.as_deref()),
            ),
        );
    }
    let on_disk: BTreeSet<String> = paths
        .iter()
        .map(|path| relative_name(root, path))
        .collect::<Result<_, _>>()?;
    let covered: BTreeSet<String> = index.covered.iter().map(|c| c.name.clone()).collect();
    if on_disk != covered {
        return scan(
            &paths,
            payload_id,
            "the index does not cover exactly the segments on disk".to_owned(),
        );
    }
    let mut matches = Vec::new();
    let mut decoded = 0;
    for (path, cover) in paths.iter().zip(&index.covered) {
        let bytes = fs::read(path).map_err(io(path))?;
        let intact = bytes.len() as u64 == cover.length && digest_string(&bytes) == cover.digest;
        if intact {
            let range = index
                .entries
                .partition_point(|entry| entry.payload_id.as_str() < payload_id.as_str());
            for entry in index.entries[range..]
                .iter()
                .take_while(|entry| entry.payload_id == payload_id.as_str())
                .filter(|entry| entry.segment == cover.name)
            {
                matches.push(PayloadMatch {
                    artifact_id: entry.artifact_id.clone(),
                    path: path.clone(),
                });
            }
        } else {
            decoded += 1;
            let segment = Segment::from_bytes(path, &bytes)?;
            matches.extend(
                segment
                    .artifacts()
                    .iter()
                    .filter(|artifact| artifact.payload_id == payload_id.as_str())
                    .map(|artifact| PayloadMatch {
                        artifact_id: artifact.id.clone(),
                        path: path.clone(),
                    }),
            );
        }
    }
    matches.sort_by(|a, b| (&a.path, &a.artifact_id).cmp(&(&b.path, &b.artifact_id)));
    Ok(Lookup {
        matches,
        method: Method::Index { decoded },
    })
}

fn describe_store(store: Option<&str>) -> &str {
    store.unwrap_or("no STORE_ID")
}

fn scan(paths: &[PathBuf], payload_id: &PayloadId, reason: String) -> Result<Lookup, ArchiveError> {
    let matches = find_payload(paths, payload_id)?;
    Ok(Lookup {
        matches,
        method: Method::Scan { reason },
    })
}

/// Read and verify the index. The error names why the index cannot be used.
fn read_index(root: &Path) -> Result<Index, String> {
    let path = root.join(INDEX_FILE);
    let bytes = fs::read(&path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => {
            "no payload index exists; run `sparrow-archive index DIR`".to_owned()
        }
        _ => format!("cannot read the payload index: {error}"),
    })?;
    decode(&bytes).ok_or_else(|| {
        "the payload index is damaged or of another format; run `sparrow-archive index DIR`"
            .to_owned()
    })
}

fn encode(index: &Index) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    put_str(&mut body, index.store.as_deref().unwrap_or(""));
    put_len(&mut body, index.covered.len());
    for cover in &index.covered {
        put_str(&mut body, &cover.name);
        body.extend_from_slice(&cover.length.to_le_bytes());
        put_str(&mut body, &cover.digest);
    }
    put_len(&mut body, index.entries.len());
    for entry in &index.entries {
        put_str(&mut body, &entry.payload_id);
        put_str(&mut body, &entry.artifact_id);
        put_str(&mut body, &entry.segment);
    }
    let checksum = digest_string(&body);
    body.extend_from_slice(checksum.as_bytes());
    body
}

fn decode(bytes: &[u8]) -> Option<Index> {
    let split = bytes.len().checked_sub(DIGEST_LEN)?;
    let (body, checksum) = bytes.split_at(split);
    if digest_string(body).as_bytes() != checksum {
        return None;
    }
    let mut reader = Reader { bytes: body, at: 0 };
    if reader.take(MAGIC.len())? != MAGIC {
        return None;
    }
    if reader.u32()? != FORMAT_VERSION {
        return None;
    }
    let store = reader.str()?;
    let store = (!store.is_empty()).then_some(store);
    let covered_count = reader.u32()? as usize;
    let mut covered = Vec::new();
    for _ in 0..covered_count {
        covered.push(Covered {
            name: reader.str()?,
            length: reader.u64()?,
            digest: reader.str()?,
        });
    }
    let entry_count = reader.u32()? as usize;
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        let entry = Entry {
            payload_id: reader.str()?,
            artifact_id: reader.str()?,
            segment: reader.str()?,
        };
        PayloadId::parse(&entry.payload_id).ok()?;
        entries.push(entry);
    }
    if reader.at != body.len() || !entries.windows(2).all(|pair| pair[0] <= pair[1]) {
        return None;
    }
    Some(Index {
        store,
        covered,
        entries,
    })
}

fn put_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&u32::try_from(len).unwrap_or(u32::MAX).to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_len(out, value.len());
    out.extend_from_slice(value.as_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let slice = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn str(&mut self) -> Option<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).ok()
    }
}
