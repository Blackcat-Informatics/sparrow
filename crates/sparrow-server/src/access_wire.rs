// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! Shared access codecs for typed queries, status and watch definitions.

pub use crate::correlation_wire::decode as decode_correlation;
pub use crate::correlation_wire::encode_relation as encode_correlation_relation;
pub use crate::correlation_wire::encode_selector as encode_correlation_selector;
pub use crate::diagnostics::dimension as diagnostic_dimension;
pub use crate::diagnostics::encode_mappings as encode_diagnostic_mappings;
pub use crate::diagnostics::render_profile as render_diagnostic_profile;
pub use crate::diagnostics::render_snapshot as render_diagnostic_snapshot;
pub use crate::diagnostics::wire_dimension as wire_diagnostic_dimension;
pub use crate::pressure::decode as render_pressure;
pub use crate::rdf_wire::MAX_BYTES as RDF_MAX_BYTES;
pub use crate::rdf_wire::MAX_NODES as RDF_MAX_NODES;
pub use crate::rdf_wire::decode as decode_rdf;
pub use crate::rdf_wire::diagnostic::render as render_native_diagnostic;
pub use crate::rdf_wire::encode as encode_rdf;
pub use crate::syslog_wire::listener as render_syslog_listener;
pub use crate::syslog_wire::listeners as render_syslog_listeners;
pub use crate::syslog_wire::render as render_syslog;
pub use crate::text_wire::encode_relations as encode_text_relations;
pub use crate::time_wire::capture as decode_time_capture;
pub use crate::time_wire::encode as encode_time_capture;
pub use crate::watch_admin::decode_definition as decode_watch_definition;
pub use crate::watch_admin::encode_definition as encode_watch_definition;

/// Encode an optional text field for typed request and status messages.
pub fn optional(field: crate::sparrow_capnp::optional_text::Builder<'_>, value: &Option<String>) {
    sparrow_client::encode_optional(
        field,
        value.as_deref(),
        |mut field, value| field.set_value(value),
        |mut field| field.set_absent(()),
    );
}
