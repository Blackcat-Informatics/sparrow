// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

use purrdf_lex::json::{Object, Value};

/// A daemon domain failure or explicit adapter/transport failure.
#[derive(Debug)]
pub struct AgentError {
    /// Stable daemon code, or the adapter's explicit transport/input code.
    pub code: String,
    /// Actionable diagnostic without a successful result prefix.
    pub message: String,
    /// Whether repeating an otherwise idempotent operation may succeed.
    pub retryable: bool,
}

impl AgentError {
    pub(crate) fn json(&self) -> Value {
        Object::new()
            .with("code", self.code.clone())
            .with("message", self.message.clone())
            .with("retryable", self.retryable)
            .into()
    }
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}; retryable={}",
            self.code, self.message, self.retryable
        )
    }
}
impl std::error::Error for AgentError {}
impl From<capnp::Error> for AgentError {
    fn from(error: capnp::Error) -> Self {
        sparrow_client::Error::from(error).into()
    }
}
impl From<std::str::Utf8Error> for AgentError {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::from(capnp::Error::from(error))
    }
}
impl From<capnp::NotInSchema> for AgentError {
    fn from(error: capnp::NotInSchema) -> Self {
        Self::from(capnp::Error::from(error))
    }
}

impl From<sparrow_client::Error> for AgentError {
    fn from(error: sparrow_client::Error) -> Self {
        use sparrow_client::Error;
        let retryable = error.retryable();
        let (code, message) = match error {
            Error::Transport(error) => ("RPC_FAILURE", error.to_string()),
            Error::Domain {
                code,
                message,
                retryable,
            } => {
                return Self {
                    code,
                    message,
                    retryable,
                };
            }
            Error::Unsupported(message) => ("UNSUPPORTED", message.to_owned()),
            Error::Protocol(message) => ("PROTOCOL_FAILURE", message.to_owned()),
            Error::InvalidArgument(message) => ("INVALID_ARGUMENT", message.to_owned()),
            Error::Io(error) => ("FILE_IO_FAILURE", error.to_string()),
        };
        Self {
            code: code.into(),
            message,
            retryable,
        }
    }
}
