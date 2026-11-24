// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The endpoint and kernel-reported caller identity that accepted a piece of evidence.

use super::{Catalog, Error, integer, read_field};
use purrdf_lex::json::{Object, Value};

/// The only endpoint that records caller identity: the group-accessible ingest socket.
pub const INGEST_SOCKET: &str = "ingest-socket";

/// Caller identity the kernel reported for a Unix connection (`SO_PEERCRED`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerCredential {
    /// Effective user ID of the connecting process.
    pub uid: u32,
    /// Effective group ID of the connecting process.
    pub gid: u32,
    /// Process ID of the connecting process.
    pub pid: u32,
}

/// Immutable intake provenance, present only for evidence accepted on the ingest socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intake {
    /// Accepted on the ingest socket from this caller.
    IngestSocket(PeerCredential),
}

impl Intake {
    /// The endpoint label stored in catalogs and RDF.
    #[must_use]
    pub fn endpoint(&self) -> &'static str {
        match self {
            Self::IngestSocket(_) => INGEST_SOCKET,
        }
    }

    /// The caller identity recorded with the evidence.
    #[must_use]
    pub fn credential(&self) -> &PeerCredential {
        match self {
            Self::IngestSocket(credential) => credential,
        }
    }
}

impl Catalog for Intake {
    fn encode(&self) -> Value {
        let credential = self.credential();
        Object::new()
            .with("endpoint", self.endpoint())
            .with("uid", credential.uid.to_string())
            .with("gid", credential.gid.to_string())
            .with("pid", credential.pid.to_string())
            .into()
    }

    fn decode(value: &Value) -> Result<Self, Error> {
        let id = |name| {
            u32::try_from(integer(value, name)?)
                .map_err(|_| Error::new("STORAGE_FAILURE", "intake identity exceeds 32 bits"))
        };
        match read_field::<String>(value, "endpoint")?.as_str() {
            INGEST_SOCKET => Ok(Self::IngestSocket(PeerCredential {
                uid: id("uid")?,
                gid: id("gid")?,
                pid: id("pid")?,
            })),
            _ => Err(Error::new("STORAGE_FAILURE", "unknown intake endpoint")),
        }
    }
}

/// Fence the store for intake provenance before its first ingest-socket write, so an
/// older writer refuses it rather than publishing, re-deriving or retiring evidence
/// without its caller identity. Idempotent; later feature transitions keep the family
/// (`writer_format::stage`), and like the archive fence it carries the log time law.
///
/// The fence is its own synchronized write ahead of the evidence, as the open-time fences
/// are: `Writer::apply` restages `store_format` from the committed family, so it would
/// replace a fence carried in the same batch.
pub(super) fn fence(writer: &super::Writer) -> Result<(), Error> {
    let db = &writer.db;
    let format = db
        .get("store_format")?
        .ok_or_else(|| Error::new("STORAGE_FAILURE", "intake fence: writer format missing"))?;
    if super::writer_format::has_intake(&format) {
        return Ok(());
    }
    writer.admit_space()?;
    let mut batch = rocksdb::WriteBatch::default();
    batch.put(
        "store_format",
        super::writer_format::enable_intake(&format)?,
    );
    if db.get(super::source_time::LOG_FENCE)?.is_none() {
        batch.put(super::source_time::LOG_FENCE, super::log_time::LAW);
    }
    super::catalog_upgrade::commit(db, batch)
}

/// Decode an optional catalog `intake` member; absence and `null` mean no intake.
pub(super) fn decode_member(value: &Value) -> Result<Option<Intake>, Error> {
    value
        .get("intake")
        .filter(|value| !value.is_null())
        .map(Intake::decode)
        .transpose()
}

/// An import keeps the intake of the connection that began it. Only a caller with the
/// same user identity (or, for an import begun without one, a caller without one) may
/// continue it, so a resumed upload cannot change its provenance.
pub(super) fn admit_import(bound: Option<&Intake>, caller: Option<&Intake>) -> Result<(), Error> {
    let uid = |intake: Option<&Intake>| intake.map(|intake| intake.credential().uid);
    if uid(bound) != uid(caller) {
        return Err(Error::new(
            "PERMISSION_DENIED",
            "this import was begun by a different caller; only the account that ran importBegin may continue it",
        ));
    }
    Ok(())
}
