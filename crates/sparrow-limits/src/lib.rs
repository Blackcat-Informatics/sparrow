// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Fixed admission limits shared by Sparrow storage and producers.

#![no_std]

/// Largest raw message admitted as a single syslog packet.
pub const SYSLOG_MESSAGE_BYTES: usize = 64 * 1024;

/// Most distinct records an atomic source-context update admits. The store reserves
/// 1 KiB of metadata per record from a 32 MiB allowance. Staged imports enforce this
/// ceiling during layout, before preparing any records.
pub const MAX_PUBLICATION_RECORDS: usize = 32_768;
