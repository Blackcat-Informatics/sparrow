// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

use crate::{Discovery, Error, sparrow_capnp::InstanceRole};

/// An instance role a caller can require. `unspecified`, the role of a store with none, is
/// not representable: a caller that requires a role never requires "no role" by mistake, and a
/// store that reports `unspecified` satisfies neither variant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequiredRole {
    /// The live store.
    Production,
    /// A scratch store, safe to receive test load.
    Scratch,
}

impl RequiredRole {
    fn wire(self) -> InstanceRole {
        match self {
            Self::Production => InstanceRole::Production,
            Self::Scratch => InstanceRole::Scratch,
        }
    }
}

/// The label of a discovered instance role: `unspecified`, `production` or `scratch`.
#[must_use]
pub fn instance_role_label(role: InstanceRole) -> &'static str {
    match role {
        InstanceRole::Unspecified => "unspecified",
        InstanceRole::Production => "production",
        InstanceRole::Scratch => "scratch",
    }
}

/// The instance role, store id, or both that a caller requires. The crate checks the
/// expectation against `Discovery` on each connection
/// before any import call and fails closed with the non-retryable `INSTANCE_MISMATCH`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expect {
    role: Option<RequiredRole>,
    store_id: Option<String>,
}

impl Expect {
    /// Require `role`, on any store.
    #[must_use]
    pub fn new(role: RequiredRole) -> Self {
        Self {
            role: Some(role),
            store_id: None,
        }
    }

    /// Require this persistent store id, independently of its role.
    #[must_use]
    pub fn store(store_id: impl Into<String>) -> Self {
        Self {
            role: None,
            store_id: Some(store_id.into()),
        }
    }

    /// Also require the store with this persistent id.
    #[must_use]
    pub fn with_store_id(mut self, store_id: impl Into<String>) -> Self {
        self.store_id = Some(store_id.into());
        self
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self
            .store_id
            .as_deref()
            .is_some_and(|id| !crate::receipt::valid_store_id(id))
        {
            return Err(Error::InvalidArgument(
                "expected store id must begin with store: followed by an identity, up to 128 bytes total",
            ));
        }
        Ok(())
    }

    /// Refuse a connection whose discovery reports another role or store id. The error names
    /// what was required and what was reported.
    pub fn require(&self, discovery: &Discovery) -> Result<(), Error> {
        let role_ok = self.role.is_none_or(|role| {
            discovery.unknown_instance_role.is_none() && discovery.instance_role == role.wire()
        });
        let store_ok = self
            .store_id
            .as_deref()
            .is_none_or(|required| required == discovery.store_id);
        if role_ok && store_ok {
            return Ok(());
        }
        let required_store = self.store_id.as_deref().unwrap_or("any");
        let reported_role = discovery.unknown_instance_role.map_or_else(
            || instance_role_label(discovery.instance_role).to_owned(),
            |ordinal| format!("unknown role ordinal {ordinal}"),
        );
        Err(Error::Domain {
            code: "INSTANCE_MISMATCH".into(),
            message: format!(
                "required instance_role {} on store {required_store}, but the daemon reports instance_role {} on store {}; refusing before any import call",
                self.role
                    .map_or("any", |role| instance_role_label(role.wire())),
                reported_role,
                discovery.store_id,
            ),
            retryable: false,
            class: crate::FailureClass::Unclassified,
        })
    }
}

/// Check `discovery` against an optional expectation.
pub(crate) fn require(expect: Option<&Expect>, discovery: &Discovery) -> Result<(), Error> {
    expect.map_or(Ok(()), |expect| expect.require(discovery))
}

/// Whether `error` is an expectation mismatch.
pub(crate) fn is_mismatch(error: &Error) -> bool {
    matches!(error, Error::Domain { code, .. } if code == "INSTANCE_MISMATCH")
}
