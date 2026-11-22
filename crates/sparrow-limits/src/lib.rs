// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Fixed admission limits shared by Sparrow storage and producers.

#![no_std]

/// Largest raw message admitted as a single syslog packet.
pub const SYSLOG_MESSAGE_BYTES: usize = 64 * 1024;
