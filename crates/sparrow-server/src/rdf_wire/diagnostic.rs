// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

//! Lossless typed diagnostic transport, including native presentation arguments.

use crate::sparrow_capnp as wire;
use purrdf_lex::{
    allocation::StorageError,
    diagnostic::DiagnosticValue,
    json::{Object, Value},
};
use sparrow_core::Error;

pub(crate) fn encode(
    mut out: wire::native_diagnostic::Builder<'_>,
    value: &purrdf::RdfDiagnostic,
) -> Result<(), Error> {
    out.set_severity(match value.severity {
        purrdf::RdfSeverity::Error => wire::NativeSeverity::Error,
        purrdf::RdfSeverity::Warning => wire::NativeSeverity::Warning,
        purrdf::RdfSeverity::Note => wire::NativeSeverity::Note,
        purrdf::RdfSeverity::Info => wire::NativeSeverity::Info,
    });
    out.set_code(&value.code);
    out.set_message(&value.message);
    sparrow_client::encode_optional(
        out.reborrow().init_detail(),
        value.detail.as_deref(),
        |mut out, v| out.set_value(v),
        |mut out| out.set_absent(()),
    );
    if let Some(location) = &value.location {
        out.set_has_location(true);
        let mut target = out.reborrow().init_location();
        for (name, text) in [
            (0, &location.path),
            (1, &location.logical),
            (2, &location.subject),
        ] {
            let field = match name {
                0 => target.reborrow().init_path(),
                1 => target.reborrow().init_logical(),
                _ => target.reborrow().init_subject(),
            };
            sparrow_client::encode_optional(
                field,
                text.as_deref(),
                |mut out, v| out.set_value(v),
                |mut out| out.set_absent(()),
            );
        }
        for (index, value) in [
            location.line,
            location.column.map(u64::from),
            location.gts_term_id,
            location.gts_quad_index,
            location.gts_reifier_id,
            location.gts_frame_index,
            location.gts_segment_index,
        ]
        .into_iter()
        .enumerate()
        {
            let field = match index {
                0 => target.reborrow().init_line(),
                1 => target.reborrow().init_column(),
                2 => target.reborrow().init_gts_term_id(),
                3 => target.reborrow().init_gts_quad_index(),
                4 => target.reborrow().init_gts_reifier_id(),
                5 => target.reborrow().init_gts_frame_index(),
                _ => target.reborrow().init_gts_segment_index(),
            };
            sparrow_client::encode_optional(
                field,
                value,
                |mut out, v| out.set_value(v),
                |mut out| out.set_absent(()),
            );
        }
    }
    let mut frames = Vec::new();
    let mut current = value.presentation();
    while let Some(value) = current {
        frames.push(value);
        current = value.detail();
    }
    let mut target = out.reborrow().init_presentation(frames.len() as u32);
    for (index, value) in frames.into_iter().enumerate() {
        let mut target = target.reborrow().get(index as u32);
        target.set_message_id(value.message_id());
        target.set_english(value.english());
        let mut parameters = target.init_parameters(value.parameters().len() as u32);
        for (index, value) in value.parameters().iter().enumerate() {
            let mut target = parameters.reborrow().get(index as u32);
            target.set_name(value.name());
            match value.value() {
                DiagnosticValue::Text(v) => target.set_text(v),
                DiagnosticValue::Unsigned(v) => target.set_unsigned(*v),
                DiagnosticValue::Signed(v) => target.set_signed(*v),
                DiagnosticValue::Boolean(v) => target.set_boolean(*v),
                DiagnosticValue::Character(v) => target.set_character(*v as u32),
                _ => {
                    return Err(Error::new(
                        "UNSUPPORTED",
                        "native presentation argument lacks a wire variant",
                    ));
                }
            }
        }
    }
    let storage = match value.storage_error() {
        None => wire::NativeStorageFailure::Absent,
        Some(StorageError::Capacity(value)) => {
            let mut capacity = out.reborrow().init_capacity();
            capacity.set_space(value.space.as_str());
            sparrow_client::encode_wide_integer(
                capacity.reborrow().init_attempted(),
                value.attempted(),
            );
            sparrow_client::encode_wide_integer(capacity.init_limit(), value.limit());
            wire::NativeStorageFailure::Capacity
        }
        Some(StorageError::SizeOverflow) => wire::NativeStorageFailure::SizeOverflow,
        Some(StorageError::AllocationFailed) => wire::NativeStorageFailure::AllocationFailed,
        Some(StorageError::AdmissionFailed) => wire::NativeStorageFailure::AdmissionFailed,
        Some(StorageError::FormattingFailed) => wire::NativeStorageFailure::FormattingFailed,
    };
    out.set_storage_failure(storage);
    Ok(())
}

pub(crate) fn render(
    value: wire::native_diagnostic::Reader<'_>,
) -> crate::agent::AgentResult<Value> {
    let mut out = Object::new()
        .with("schema", "purrdf-diagnostic-v1")
        .with(
            "severity",
            match value.get_severity()? {
                wire::NativeSeverity::Error => "error",
                wire::NativeSeverity::Warning => "warning",
                wire::NativeSeverity::Note => "note",
                wire::NativeSeverity::Info => "info",
            },
        )
        .with("code", value.get_code()?.to_str()?)
        .with("message", value.get_message()?.to_str()?);
    if let wire::optional_text::Value(text) = value.get_detail()?.which()? {
        out = out.with("detail", text?.to_str()?);
    }
    if value.get_has_location() {
        let loc = value.get_location()?;
        let mut location = Object::new();
        for (name, field) in [
            ("path", loc.get_path()?),
            ("logical", loc.get_logical()?),
            ("subject", loc.get_subject()?),
        ] {
            if let wire::optional_text::Value(text) = field.which()? {
                location = location.with(name, text?.to_str()?);
            }
        }
        for (name, field) in [
            ("line", loc.get_line()?),
            ("column", loc.get_column()?),
            ("gtsTermId", loc.get_gts_term_id()?),
            ("gtsQuadIndex", loc.get_gts_quad_index()?),
            ("gtsReifierId", loc.get_gts_reifier_id()?),
            ("gtsFrameIndex", loc.get_gts_frame_index()?),
            ("gtsSegmentIndex", loc.get_gts_segment_index()?),
        ] {
            if let wire::optional_limit::Value(n) = field.which()? {
                location = location.with(
                    name,
                    if name == "column" {
                        Value::from(n)
                    } else {
                        Value::from(n.to_string())
                    },
                );
            }
        }
        out = out.with("location", location);
    }
    let frames = value.get_presentation()?;
    let mut detail = None;
    for index in (0..frames.len()).rev() {
        let frame = frames.get(index);
        let mut parameters = Object::new();
        for parameter in frame.get_parameters()? {
            use wire::native_parameter as P;
            let (kind, value) = match parameter.which()? {
                P::Text(v) => ("text", Value::from(v?.to_str()?)),
                P::Unsigned(v) => ("unsigned", Value::from(v.to_string())),
                P::Signed(v) => ("signed", Value::from(v.to_string())),
                P::Boolean(v) => ("boolean", Value::from(v)),
                P::Character(v) => (
                    "character",
                    Value::from(
                        char::from_u32(v)
                            .ok_or_else(|| crate::agent::AgentError {
                                code: "PROTOCOL_FAILURE".into(),
                                message: "native diagnostic has an invalid Unicode scalar".into(),
                                retryable: false,
                            })?
                            .to_string(),
                    ),
                ),
            };
            parameters = parameters.with(
                parameter.get_name()?.to_str()?,
                Object::new().with("kind", kind).with("value", value),
            );
        }
        let mut presentation = Object::new()
            .with("messageId", frame.get_message_id()?.to_str()?)
            .with("parameters", parameters);
        if let Some(value) = detail {
            presentation = presentation.with("detail", value);
        }
        detail = Some(Value::from(presentation));
    }
    if let Some(value) = detail {
        out = out.with("presentation", value);
    }
    let storage = value.get_storage_failure()?;
    if storage != wire::NativeStorageFailure::Absent {
        out = out.with(
            "storage_failure",
            match storage {
                wire::NativeStorageFailure::Capacity => "capacity",
                wire::NativeStorageFailure::SizeOverflow => "size_overflow",
                wire::NativeStorageFailure::AllocationFailed => "allocation_failed",
                wire::NativeStorageFailure::AdmissionFailed => "admission_failed",
                wire::NativeStorageFailure::FormattingFailed => "formatting_failed",
                wire::NativeStorageFailure::Absent => unreachable!(),
            },
        );
        if storage == wire::NativeStorageFailure::Capacity {
            let c = value.get_capacity()?;
            out = out.with(
                "capacity",
                Object::new()
                    .with("space", c.get_space()?.to_str()?)
                    .with(
                        "attempted",
                        sparrow_client::decode_wide_integer(c.get_attempted()?).to_string(),
                    )
                    .with(
                        "limit",
                        sparrow_client::decode_wide_integer(c.get_limit()?).to_string(),
                    ),
            );
        }
    }
    Ok(out.into())
}
