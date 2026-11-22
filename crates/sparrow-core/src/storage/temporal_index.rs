// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Borrow native receipt-index entries until their admitted catalog lookup finishes.

use super::Error;
use rocksdb::{DB, DBRawIteratorWithThreadMode, ReadOptions};

pub(super) const ID_BYTES: usize = 128;

/// A prospective index-row allowance, independent of its catalog body's size.
pub(super) const fn row_bound(prefix: &str) -> u64 {
    (prefix.len() + 21 + 2 * ID_BYTES) as u64
}

/// A borrowed index key and its identity value.
pub(super) type Entry<'a> = (&'a [u8], &'a [u8]);

pub(super) struct TemporalRows<'a> {
    cursor: DBRawIteratorWithThreadMode<'a, DB>,
    prefix: &'a str,
    advance: bool,
    finished: bool,
}

impl<'a> TemporalRows<'a> {
    pub(super) fn new(
        prefix: &'a str,
        from: u64,
        before: u64,
        scan: impl FnOnce(ReadOptions) -> DBRawIteratorWithThreadMode<'a, DB>,
    ) -> Self {
        Self::after(prefix, from, None, before, scan)
    }

    /// Start at `from`, or just past the entry for `after` received at `from`.
    pub(super) fn after(
        prefix: &'a str,
        from: u64,
        after: Option<&str>,
        before: u64,
        scan: impl FnOnce(ReadOptions) -> DBRawIteratorWithThreadMode<'a, DB>,
    ) -> Self {
        let mut options = ReadOptions::default();
        options.set_iterate_upper_bound(format!("{prefix}{before:020}:").into_bytes());
        let mut cursor = scan(options);
        let mut start = format!("{prefix}{from:020}:").into_bytes();
        if let Some(after) = after {
            // The least key greater than the entry itself.
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        cursor.seek(&start);
        Self {
            cursor,
            prefix,
            advance: false,
            finished: false,
        }
    }

    /// Check borrowed key/value widths before copying an identity or loading a body.
    /// Domain readers retain their own catalog/index identity and timestamp law.
    pub(super) fn read_next<T>(
        &mut self,
        read: impl FnOnce(&[u8], &[u8]) -> Result<T, Error>,
    ) -> Option<Result<T, Error>> {
        let result = match self.peek()? {
            Ok((key, value)) => read(key, value),
            Err(error) => return Some(Err(error)),
        };
        self.advance = true;
        if result.is_err() {
            self.finished = true;
        }
        Some(result)
    }

    /// Borrow the width-checked head entry without consuming it.
    pub(super) fn peek(&mut self) -> Option<Result<Entry<'_>, Error>> {
        if self.finished {
            return None;
        }
        if self.advance {
            self.cursor.next();
            self.advance = false;
        }
        if !self.cursor.valid() {
            self.finished = true;
            return self.cursor.status().err().map(|error| Err(error.into()));
        }
        let state = match self.cursor.key().zip(self.cursor.value()) {
            None => Err(Error::new(
                "STORAGE_FAILURE",
                "native temporal cursor lost its borrowed entry",
            )),
            Some((key, _)) if !key.starts_with(self.prefix.as_bytes()) => Ok(false),
            Some((key, value)) => check_width(self.prefix, key, value).map(|()| true),
        };
        match state {
            Ok(true) => self.cursor.key().zip(self.cursor.value()).map(Ok),
            Ok(false) => {
                self.finished = true;
                None
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

pub(super) fn check_width(prefix: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
    if key.len() > prefix.len() + 21 + ID_BYTES || value.is_empty() || value.len() > ID_BYTES {
        return Err(Error::new(
            "STORAGE_FAILURE",
            "temporal index exceeds its bounded key/identity contract",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_width_guards_precede_callbacks_and_respect_half_open_bounds() {
        for long_key in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            db.put("time:00000000000000000010:record:a", b"record:a")
                .unwrap();
            let key = if long_key {
                format!("time:{:020}:{}", 20, "x".repeat(ID_BYTES + 1))
            } else {
                "time:00000000000000000020:record:b".into()
            };
            let value = if long_key {
                b"record:b".to_vec()
            } else {
                vec![b'x'; ID_BYTES + 1]
            };
            db.put(key, value).unwrap();
            let mut bounded =
                TemporalRows::new("time:", 10, 20, |options| db.raw_iterator_opt(options));
            assert_eq!(
                bounded.read_next(|_, id| Ok(id.to_vec())).unwrap().unwrap(),
                b"record:a"
            );
            assert!(
                bounded
                    .read_next::<()>(|_, _| panic!("upper bound must stop before corrupt row"))
                    .is_none()
            );
            let mut corrupt =
                TemporalRows::new("time:", 20, 30, |options| db.raw_iterator_opt(options));
            assert_eq!(
                corrupt
                    .read_next::<()>(|_, _| panic!(
                        "oversized borrowed entries must not reach lookup"
                    ))
                    .unwrap()
                    .unwrap_err()
                    .code,
                "STORAGE_FAILURE"
            );
            assert!(
                corrupt
                    .read_next::<()>(|_, _| panic!("a failed cursor must remain terminal"))
                    .is_none()
            );
        }
    }
}
