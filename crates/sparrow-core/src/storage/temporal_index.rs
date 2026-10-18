// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Borrow native receipt-index entries until their admitted catalog lookup finishes.

use super::Error;
use rocksdb::{DB, DBRawIteratorWithThreadMode, ReadOptions};

pub(super) const ID_BYTES: usize = 128;

/// A prospective index-row allowance, independent of its catalog body's size.
pub(super) const fn row_bound(prefix: &str) -> u64 {
    (prefix.len() + 21 + 2 * ID_BYTES) as u64
}

pub(super) struct TemporalRows<'a> {
    cursor: DBRawIteratorWithThreadMode<'a, DB>,
    prefix: &'static str,
    advance: bool,
    finished: bool,
}

impl<'a> TemporalRows<'a> {
    pub(super) fn new(
        prefix: &'static str,
        from: u64,
        before: u64,
        scan: impl FnOnce(ReadOptions) -> DBRawIteratorWithThreadMode<'a, DB>,
    ) -> Self {
        let mut options = ReadOptions::default();
        options.set_iterate_upper_bound(format!("{prefix}{before:020}:").into_bytes());
        let mut cursor = scan(options);
        cursor.seek(format!("{prefix}{from:020}:").as_bytes());
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
        if self.finished {
            return None;
        }
        if self.advance {
            self.cursor.next();
        }
        self.advance = true;
        if !self.cursor.valid() {
            self.finished = true;
            return self.cursor.status().err().map(|error| Err(error.into()));
        }
        let entry = self.cursor.key().zip(self.cursor.value());
        let Some((key, value)) = entry else {
            self.finished = true;
            return Some(Err(Error::new(
                "STORAGE_FAILURE",
                "native temporal cursor lost its borrowed entry",
            )));
        };
        if !key.starts_with(self.prefix.as_bytes()) {
            self.finished = true;
            return None;
        }
        if let Err(error) = check_width(self.prefix, key, value) {
            self.finished = true;
            return Some(Err(error));
        }
        let result = read(key, value);
        if result.is_err() {
            self.finished = true;
        }
        Some(result)
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
