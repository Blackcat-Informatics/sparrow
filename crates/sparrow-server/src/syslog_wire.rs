// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! One typed protocol snapshot adapter and canonical agent renderer.

use crate::sparrow_capnp as wire;
use purrdf_lex::json::{Object, Value};

const HEADERS: &[(wire::SyslogHeader, &str)] = &[
    (wire::SyslogHeader::Timestamp, "timestamp"),
    (wire::SyslogHeader::Host, "host"),
    (wire::SyslogHeader::App, "app"),
    (wire::SyslogHeader::Procid, "procid"),
    (wire::SyslogHeader::Msgid, "msgid"),
    (wire::SyslogHeader::Tag, "tag"),
];

pub(crate) fn listeners(
    value: capnp::struct_list::Reader<'_, wire::syslog_listener_status::Owned>,
) -> crate::agent::AgentResult<Value> {
    value
        .iter()
        .map(listener)
        .collect::<crate::agent::AgentResult<Vec<_>>>()
        .map(Into::into)
}

pub(crate) fn listener(
    listener: wire::syslog_listener_status::Reader<'_>,
) -> crate::agent::AgentResult<Value> {
    let framing = match listener.get_framing()?.which()? {
        wire::optional_syslog_framing::Absent(()) => Value::Null,
        wire::optional_syslog_framing::Value(value) => Value::from(match value? {
            wire::SyslogFraming::OctetCounting => "octet_counting",
            wire::SyslogFraming::Lf => "lf",
            wire::SyslogFraming::CrLf => "cr_lf",
        }),
    };
    let failure = match listener.get_failure()?.which()? {
        wire::optional_text::Absent(()) => Value::Null,
        wire::optional_text::Value(value) => Value::from(value?.to_str()?),
    };
    Ok(Value::from(
        Object::new()
            .with("id", listener.get_id()?.to_str()?)
            .with("source", listener.get_source()?.to_str()?)
            .with("address", listener.get_address()?.to_str()?)
            .with(
                "transport",
                match listener.get_transport()? {
                    wire::SyslogTransport::Tcp => "tcp",
                    wire::SyslogTransport::Udp => "udp",
                },
            )
            .with("framing", framing)
            .with(
                "max_message_bytes",
                u64::from(listener.get_max_message_bytes()),
            )
            .with("max_connections", u64::from(listener.get_max_connections()))
            .with("read_timeout_ms", u64::from(listener.get_read_timeout_ms()))
            .with(
                "active_connections",
                u64::from(listener.get_active_connections()),
            )
            .with(
                "accepted_messages",
                listener.get_accepted_messages().to_string(),
            )
            .with(
                "interpretation_failures",
                listener.get_interpretation_failures().to_string(),
            )
            .with(
                "refused_messages",
                listener.get_refused_messages().to_string(),
            )
            .with(
                "truncated_datagrams",
                listener.get_truncated_datagrams().to_string(),
            )
            .with(
                "framing_failures",
                listener.get_framing_failures().to_string(),
            )
            .with(
                "rejected_connections",
                listener.get_rejected_connections().to_string(),
            )
            .with(
                "timed_out_connections",
                listener.get_timed_out_connections().to_string(),
            )
            .with("held_frames", u64::from(listener.get_held_frames()))
            .with(
                "holds_storage_pressure",
                listener.get_holds_storage_pressure().to_string(),
            )
            .with(
                "holds_no_permit",
                listener.get_holds_no_permit().to_string(),
            )
            .with(
                "longest_hold_ms",
                listener.get_longest_hold_ms().to_string(),
            )
            .with("continuity_unknown", listener.get_continuity_unknown())
            .with("kernel_drops_unknown", listener.get_kernel_drops_unknown())
            .with("failure", failure),
    ))
}

pub(crate) fn snapshot(
    mut out: wire::optional_syslog_snapshot::Builder<'_>,
    value: Option<&sparrow_core::SyslogSnapshot>,
) -> capnp::Result<()> {
    let Some(value) = value else {
        out.set_absent(());
        return Ok(());
    };
    let mut out = out.init_value();
    out.set_profile_id(&value.profile_id);
    if let Some(diagnostic) = &value.diagnostic {
        out.set_failed(diagnostic);
        return Ok(());
    }
    let mut parsed = out.init_parsed();
    parsed.set_dialect(match value.dialect {
        Some(sparrow_core::SyslogDialect::Rfc5424) => wire::SyslogDialect::Rfc5424,
        Some(sparrow_core::SyslogDialect::Rfc3164) => wire::SyslogDialect::Rfc3164,
        None => return Err(capnp::Error::failed("parsed syslog dialect absent".into())),
    });
    parsed.set_priority(
        value
            .priority
            .ok_or_else(|| capnp::Error::failed("parsed syslog priority absent".into()))?,
    );
    parsed.set_priority_start(value.priority_start);
    parsed.set_priority_end(value.priority_end);
    parsed.set_message_start(value.message_start);
    let mut fields = parsed.reborrow().init_fields(value.fields.len() as u32);
    for (index, field) in value.fields.iter().enumerate() {
        let mut row = fields.reborrow().get(index as u32);
        row.set_kind(
            HEADERS
                .iter()
                .find(|(_, name)| *name == field.kind)
                .ok_or_else(|| capnp::Error::failed("unknown syslog header role".into()))?
                .0,
        );
        row.set_value(&field.value);
        row.set_source_start(field.source_start);
        row.set_source_end(field.source_end);
    }
    let mut elements = parsed.init_structured_data(value.structured_data.len() as u32);
    for (index, element) in value.structured_data.iter().enumerate() {
        let mut row = elements.reborrow().get(index as u32);
        row.set_id(&element.id);
        row.set_source_start(element.source_start);
        row.set_source_end(element.source_end);
        let mut parameters = row.init_parameters(element.parameters.len() as u32);
        for (index, parameter) in element.parameters.iter().enumerate() {
            let mut row = parameters.reborrow().get(index as u32);
            row.set_name(&parameter.name);
            row.set_value_start(parameter.value_start);
            row.set_value_end(parameter.value_end);
        }
    }
    Ok(())
}

pub(crate) fn render(
    value: wire::optional_syslog_snapshot::Reader<'_>,
) -> crate::agent::AgentResult<Value> {
    let value = match value.which()? {
        wire::optional_syslog_snapshot::Absent(()) => return Ok(Value::Null),
        wire::optional_syslog_snapshot::Value(value) => value?,
    };
    let base = Object::new().with("profile_id", value.get_profile_id()?.to_str()?);
    let parsed = match value.which()? {
        wire::syslog_snapshot::Failed(diagnostic) => {
            return Ok(base
                .with("state", "failed")
                .with("diagnostic", diagnostic?.to_str()?)
                .into());
        }
        wire::syslog_snapshot::Parsed(value) => value?,
    };
    let mut fields = Vec::new();
    for field in parsed.get_fields()? {
        let name = HEADERS
            .iter()
            .find(|(kind, _)| field.get_kind() == Ok(*kind))
            .ok_or_else(|| capnp::Error::failed("unknown syslog header role".into()))?
            .1;
        fields.push(Value::from(
            Object::new()
                .with("kind", name)
                .with("value", field.get_value()?.to_str()?)
                .with("source_start", field.get_source_start().to_string())
                .with("source_end", field.get_source_end().to_string()),
        ));
    }
    let mut elements = Vec::new();
    for element in parsed.get_structured_data()? {
        let mut parameters = Vec::new();
        for parameter in element.get_parameters()? {
            parameters.push(Value::from(
                Object::new()
                    .with("name", parameter.get_name()?.to_str()?)
                    .with("value_start", parameter.get_value_start().to_string())
                    .with("value_end", parameter.get_value_end().to_string()),
            ));
        }
        elements.push(Value::from(
            Object::new()
                .with("id", element.get_id()?.to_str()?)
                .with("source_start", element.get_source_start().to_string())
                .with("source_end", element.get_source_end().to_string())
                .with("parameters", parameters),
        ));
    }
    Ok(base
        .with("state", "parsed")
        .with(
            "dialect",
            match parsed.get_dialect()? {
                wire::SyslogDialect::Rfc5424 => "rfc5424",
                wire::SyslogDialect::Rfc3164 => "rfc3164",
            },
        )
        .with("priority", u64::from(parsed.get_priority()))
        .with("facility", u64::from(parsed.get_priority() / 8))
        .with("severity", u64::from(parsed.get_priority() % 8))
        .with("priority_start", parsed.get_priority_start().to_string())
        .with("priority_end", parsed.get_priority_end().to_string())
        .with("fields", fields)
        .with("structured_data", elements)
        .with("message_start", parsed.get_message_start().to_string())
        .into())
}
