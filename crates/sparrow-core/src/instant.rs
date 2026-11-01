// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The native RFC 3339 codec at Sparrow's millisecond clock boundary.

use crate::Error;
use purrdf::xsd::rfc3339::{self, LeapSecond, Separator};

/// Parse a millisecond-exact RFC 3339 instant through PurRDF.
/// This storage generation does not admit pre-epoch or sub-millisecond bounds.
pub fn parse_instant(text: &str) -> Result<u64, Error> {
    let (seconds, nanos) = rfc3339::parse(text, Separator::Rfc3339Abnf, LeapSecond::MonthEnd)
        .map_err(|e| Error {
            code: "INVALID_ARGUMENT",
            message: e.to_string(),
        })?;
    if seconds < 0 || nanos % 1_000_000 != 0 {
        return Err(Error {
            code: "INVALID_ARGUMENT",
            message:
                "require an instant at or after the Unix epoch with millisecond-exact precision"
                    .into(),
        });
    }
    (seconds as u64)
        .checked_mul(1000)
        .and_then(|base| base.checked_add(u64::from(nanos / 1_000_000)))
        .ok_or_else(|| Error {
            code: "INVALID_ARGUMENT",
            message: "instant overflow".into(),
        })
}

/// Format the stored millisecond clock as canonical RFC 3339 UTC through PurRDF.
pub fn format_instant(millis: u64) -> Result<String, Error> {
    let seconds = i64::try_from(millis / 1000).map_err(|_| Error {
        code: "INVALID_ARGUMENT",
        message: "instant overflow".into(),
    })?;
    rfc3339::format(seconds, ((millis % 1000) * 1_000_000) as u32).map_err(|e| Error {
        code: "INVALID_ARGUMENT",
        message: e.into(),
    })
}
