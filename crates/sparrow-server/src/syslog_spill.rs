// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The spill file of one syslog TCP listener: frames it took off a connection and could not
//! store before it stopped. The listener publishes them, in order, before it accepts a
//! connection at its next start, so a stop under storage pressure loses no frame the listener
//! has read.
//!
//! A record is a little-endian `u32` source length, the source, a little-endian `u64` frame
//! length, and the frame. A failed append is cut back to the length it had before. A record cut
//! off by a crash was never synced, so the stop that wrote it never reported success and the
//! read drops it. Publication keeps every frame the store has not yet taken and removes the
//! file once the store has taken them all, so delivery across a crash during publication is at
//! least once.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// One spilled frame: the source it was offered under and its bytes.
pub(crate) struct Frame {
    pub(crate) source: String,
    pub(crate) bytes: Arc<[u8]>,
}

/// The frames of a spill file and the bytes of a torn final record that were dropped.
pub(crate) struct Spilled {
    pub(crate) frames: Vec<Frame>,
    pub(crate) torn_bytes: usize,
}

/// The spill file of one listener, in the spill directory beside the store.
pub(crate) struct Spill {
    path: PathBuf,
    /// Serializes appends from the connections of one listener that stop together.
    append: Mutex<()>,
}

impl Spill {
    /// The spill file of the listener `id` in `dir`. The id is hex-encoded in the file name, so
    /// no id can name a path outside `dir` or collide with another listener's file.
    pub(crate) fn new(dir: &Path, id: &str) -> Self {
        Self {
            path: dir.join(format!("{}.spill", file_stem(id))),
            append: Mutex::new(()),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The scratch file a rewrite is built in before it is renamed over the spill file.
    pub(crate) fn partial_path(&self) -> PathBuf {
        self.path.with_extension("spill.partial")
    }

    /// Append one frame and sync it before returning. An error means the frame is not kept, and
    /// the file is cut back to its earlier length so no partial record stays behind it: the
    /// caller must report the stop as failed.
    pub(crate) fn append(&self, source: &str, bytes: &[u8]) -> io::Result<()> {
        let _guard = self
            .append
            .lock()
            .map_err(|_| io::Error::other("syslog spill lock poisoned"))?;
        let Some(dir) = self.path.parent() else {
            return Err(io::Error::other(format!(
                "syslog spill {} has no directory",
                self.path.display()
            )));
        };
        fs::create_dir_all(dir).map_err(|error| self.error("create the spill directory", error))?;
        let source_len = u32::try_from(source.len()).map_err(|_| {
            io::Error::other(format!(
                "syslog spill {}: source name of {} bytes does not fit a record",
                self.path.display(),
                source.len()
            ))
        })?;
        let mut record = Vec::with_capacity(12 + source.len() + bytes.len());
        record.extend_from_slice(&source_len.to_le_bytes());
        record.extend_from_slice(source.as_bytes());
        record.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        record.extend_from_slice(bytes);
        let created = !self.path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| self.error("open", error))?;
        let before = file
            .metadata()
            .map_err(|error| self.error("stat", error))?
            .len();
        let written = file.write_all(&record).and_then(|()| file.sync_all());
        if let Err(error) = written {
            return Err(match file.set_len(before) {
                Ok(()) => self.error("write and sync", error),
                Err(cut) => io::Error::other(format!(
                    "syslog spill {}: write and sync: {error}; cutting the partial record back failed too: {cut}",
                    self.path.display()
                )),
            });
        }
        if created {
            fs::File::open(dir)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| self.error("sync the spill directory", error))?;
        }
        Ok(())
    }

    /// Every spilled frame in the file, in the order they were appended. A missing file holds
    /// none. A record cut off at the end is dropped and its bytes counted; any other damage is an
    /// error, because frames past it cannot be found.
    pub(crate) fn read(&self) -> io::Result<Spilled> {
        // A scratch file survives only an interrupted rewrite. The spill file is replaced by
        // rename, so until that rename the spill file still holds every frame the scratch does.
        match fs::remove_file(self.partial_path()) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(self.error("remove the scratch file", error)),
        }
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Spilled {
                    frames: Vec::new(),
                    torn_bytes: 0,
                });
            }
            Err(error) => return Err(self.error("read", error)),
        };
        let mut frames = Vec::new();
        let mut rest = bytes.as_slice();
        while !rest.is_empty() {
            let Some((frame, after)) = self.record(rest)? else {
                return Ok(Spilled {
                    frames,
                    torn_bytes: rest.len(),
                });
            };
            frames.push(frame);
            rest = after;
        }
        Ok(Spilled {
            frames,
            torn_bytes: 0,
        })
    }

    /// Replace the file with `frames`, written beside it, synced and renamed into place, so a
    /// crash leaves either the old file or the new one. An empty list removes the file.
    pub(crate) fn replace(&self, frames: &[Frame]) -> io::Result<()> {
        if frames.is_empty() {
            return self.remove();
        }
        let partial = self.partial_path();
        let mut file = fs::File::create(&partial).map_err(|error| self.error("create", error))?;
        for frame in frames {
            let source_len = u32::try_from(frame.source.len()).map_err(|_| {
                io::Error::other(format!(
                    "syslog spill {}: source name of {} bytes does not fit a record",
                    self.path.display(),
                    frame.source.len()
                ))
            })?;
            file.write_all(&source_len.to_le_bytes())
                .and_then(|()| file.write_all(frame.source.as_bytes()))
                .and_then(|()| file.write_all(&(frame.bytes.len() as u64).to_le_bytes()))
                .and_then(|()| file.write_all(&frame.bytes))
                .map_err(|error| self.error("write", error))?;
        }
        file.sync_all().map_err(|error| self.error("sync", error))?;
        fs::rename(&partial, &self.path).map_err(|error| self.error("rename into place", error))?;
        self.sync_dir()
    }

    /// Remove the file once every frame in it has been stored. A missing file is already empty.
    pub(crate) fn remove(&self) -> io::Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(self.error("remove", error)),
        }
        self.sync_dir()
    }

    /// One record off the front of `bytes`: the frame and what follows it, or `None` when the
    /// record is cut off by the end of `bytes`.
    fn record<'a>(&self, bytes: &'a [u8]) -> io::Result<Option<(Frame, &'a [u8])>> {
        let Some((source_len, after)) = take(bytes, 4) else {
            return Ok(None);
        };
        let source_len = u32::from_le_bytes(source_len.try_into().map_err(|_| self.corrupt())?);
        let Some((source, after)) = take(after, source_len as usize) else {
            return Ok(None);
        };
        let source = std::str::from_utf8(source)
            .map_err(|error| {
                io::Error::other(format!(
                    "syslog spill {}: source name is not UTF-8: {error}",
                    self.path.display()
                ))
            })?
            .to_owned();
        let Some((frame_len, after)) = take(after, 8) else {
            return Ok(None);
        };
        let frame_len = u64::from_le_bytes(frame_len.try_into().map_err(|_| self.corrupt())?);
        let Ok(frame_len) = usize::try_from(frame_len) else {
            return Ok(None);
        };
        let Some((frame, after)) = take(after, frame_len) else {
            return Ok(None);
        };
        Ok(Some((
            Frame {
                source,
                bytes: Arc::from(frame),
            },
            after,
        )))
    }

    fn sync_dir(&self) -> io::Result<()> {
        let Some(dir) = self.path.parent() else {
            return Err(io::Error::other(format!(
                "syslog spill {} has no directory",
                self.path.display()
            )));
        };
        fs::File::open(dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| self.error("sync the spill directory", error))
    }

    fn error(&self, action: &str, error: io::Error) -> io::Error {
        io::Error::new(
            error.kind(),
            format!("syslog spill {}: {action}: {error}", self.path.display()),
        )
    }

    fn corrupt(&self) -> io::Error {
        io::Error::other(format!(
            "syslog spill {}: a length field is not readable",
            self.path.display()
        ))
    }
}

/// The name of a listener's spill file without its extension: the hex of the id, so the name is
/// a safe file name of twice the id's length.
pub(crate) fn file_stem(id: &str) -> String {
    id.bytes().map(|byte| format!("{byte:02x}")).collect()
}

/// The first `len` bytes of `bytes` and the rest, or `None` when fewer than `len` remain.
fn take(bytes: &[u8], len: usize) -> Option<(&[u8], &[u8])> {
    (bytes.len() >= len).then(|| bytes.split_at(len))
}
