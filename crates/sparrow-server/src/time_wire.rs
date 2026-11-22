// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared bounded log-capture controls for definitions and returned profiles.
use crate::sparrow_capnp as wire;
use sparrow_core::LogTimeCapture;

/// Decode the log time capture descriptor.
pub fn capture(value: wire::log_time_capture::Reader<'_>) -> capnp::Result<LogTimeCapture> {
    let pattern = value.get_pattern()?;
    let flags = value.get_flags()?;
    if pattern.len() > 4096 || flags.len() > 2 || value.get_group() > 32 {
        return Err(capnp::Error::failed(
            "log capture exceeds bounded pattern/group/flag controls".into(),
        ));
    }
    Ok(LogTimeCapture {
        pattern: pattern.to_str()?.to_owned(),
        group: value.get_group(),
        flags: flags.to_str()?.to_owned(),
    })
}

/// Encode the log time capture descriptor.
pub fn encode(mut out: wire::log_time_capture::Builder<'_>, value: &LogTimeCapture) {
    out.set_pattern(&value.pattern);
    out.set_group(value.group);
    out.set_flags(&value.flags);
}
