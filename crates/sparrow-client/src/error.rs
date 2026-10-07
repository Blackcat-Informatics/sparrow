// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

/// An explicit transport, daemon, compatibility, input, or local file failure.
#[derive(Debug)]
pub enum Error {
    /// The typed RPC connection or message could not be used.
    Transport(capnp::Error),
    /// The daemon refused the operation.
    Domain {
        /// Stable daemon failure code.
        code: String,
        /// Actionable daemon diagnostic.
        message: String,
        /// Whether an idempotent retry may succeed.
        retryable: bool,
    },
    /// Discovery is incompatible with this client.
    Unsupported(&'static str),
    /// The daemon returned inconsistent progress or metadata.
    Protocol(&'static str),
    /// A caller supplied inadmissible input.
    InvalidArgument(&'static str),
    /// Local file, Unix socket, or producer worker setup failed.
    Io(std::io::Error),
}

impl Error {
    /// Decode a daemon failure, preserving malformed message failures as transport errors.
    pub fn domain(value: crate::sparrow_capnp::failure::Reader<'_>) -> Self {
        let decode = || -> Result<Self, Self> {
            Ok(Self::Domain {
                code: value.get_code()?.to_str()?.to_owned(),
                message: value.get_message()?.to_str()?.to_owned(),
                retryable: value.get_retryable(),
            })
        };
        match decode() {
            Ok(error) | Err(error) => error,
        }
    }

    /// Whether retrying the same immutable request identity may succeed.
    pub fn retryable(&self) -> bool {
        match self {
            Self::Domain { retryable, .. } => *retryable,
            Self::Transport(error) => matches!(
                error.kind,
                capnp::ErrorKind::Disconnected | capnp::ErrorKind::Overloaded
            ),
            _ => false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "RPC transport: {error}"),
            Self::Domain {
                code,
                message,
                retryable,
            } => write!(f, "{code}: {message}; retryable={retryable}"),
            Self::Unsupported(message) => write!(f, "unsupported discovery: {message}"),
            Self::Protocol(message) => write!(f, "invalid daemon response: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid input: {message}"),
            Self::Io(error) => write!(f, "local I/O: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<capnp::Error> for Error {
    fn from(error: capnp::Error) -> Self {
        Self::Transport(error)
    }
}
impl From<capnp::NotInSchema> for Error {
    fn from(error: capnp::NotInSchema) -> Self {
        Self::Transport(error.into())
    }
}
impl From<std::str::Utf8Error> for Error {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::Transport(error.into())
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
