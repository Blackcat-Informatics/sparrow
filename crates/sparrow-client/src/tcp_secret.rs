// SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
// SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0

//! The one shared-secret CHAP law for public TCP sessions, used by daemon and clients.

use purrdf_hash::{SecretArray, blake3};
use std::{os::unix::fs::MetadataExt, path::Path};

/// BLAKE3 key-derivation context binding secret-file bytes to this protocol.
const CONTEXT: &str = "sparrow 2026-10-27 public TCP CHAP session key";
/// Challenge and response size in bytes.
pub const CHAP_BYTES: usize = 32;
const MIN_SECRET_BYTES: u64 = 32;
const MAX_SECRET_BYTES: u64 = 4096;

/// A derived CHAP key read from a private operator-supplied secret file.
#[derive(Clone)]
pub struct TcpSecret {
    key: SecretArray<u8, CHAP_BYTES>,
}

impl TcpSecret {
    /// Read a regular file of 32..4096 bytes that is not group or world accessible.
    pub fn read(path: &Path) -> Result<Self, crate::Error> {
        if !std::fs::metadata(path)?.is_file() {
            return Err(crate::Error::InvalidArgument(
                "TCP secret must be a regular file",
            ));
        }
        let file = std::fs::File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(crate::Error::InvalidArgument(
                "TCP secret must be a regular file",
            ));
        }
        if metadata.mode() & 0o077 != 0 {
            return Err(crate::Error::InvalidArgument(
                "TCP secret file must not be group or world accessible (chmod 600)",
            ));
        }
        if !(MIN_SECRET_BYTES..=MAX_SECRET_BYTES).contains(&metadata.len()) {
            return Err(crate::Error::InvalidArgument(
                "TCP secret file must contain 32..4096 bytes",
            ));
        }
        let mut material = Vec::with_capacity(metadata.len() as usize);
        std::io::Read::read_to_end(
            &mut std::io::Read::take(&file, MAX_SECRET_BYTES + 1),
            &mut material,
        )?;
        if material.len() as u64 != metadata.len() {
            purrdf_hash::wipe_secret(&mut material);
            return Err(crate::Error::InvalidArgument(
                "TCP secret file changed while it was read",
            ));
        }
        let key = SecretArray::new(blake3::derive_key(CONTEXT, &material));
        purrdf_hash::wipe_secret(&mut material);
        Ok(Self { key })
    }

    /// The response a holder of this secret gives to one store-bound challenge.
    #[must_use]
    pub fn respond(&self, store_id: &str, nonce: &[u8; CHAP_BYTES]) -> [u8; CHAP_BYTES] {
        let mut message = Vec::with_capacity(CHAP_BYTES + store_id.len());
        message.extend_from_slice(nonce);
        message.extend_from_slice(store_id.as_bytes());
        *blake3::keyed_hash(&self.key, &message).as_bytes()
    }

    /// Check a response without data-dependent early exit.
    #[must_use]
    pub fn verify(&self, store_id: &str, nonce: &[u8; CHAP_BYTES], response: &[u8]) -> bool {
        purrdf_ed25519::constant_time_eq(&self.respond(store_id, nonce), response)
    }
}

/// Run one CHAP exchange on an anonymous TCP capability and return its session capability.
pub(crate) async fn authenticate(
    client: &crate::sparrow_capnp::sparrow::Client,
    store_id: &str,
    secret: &TcpSecret,
) -> Result<crate::sparrow_capnp::sparrow::Client, crate::Error> {
    use crate::sparrow_capnp::result;
    let challenge = client.auth_challenge_request().send().promise.await?;
    let nonce: [u8; CHAP_BYTES] = match challenge.get()?.get_result()?.which()? {
        result::Ok(value) => value?
            .get_nonce()?
            .try_into()
            .map_err(|_| crate::Error::Protocol("CHAP challenge must contain 32 bytes"))?,
        result::Error(error) => return Err(crate::Error::domain(error?)),
    };
    let mut request = client.authenticate_request();
    request
        .get()
        .set_response(&secret.respond(store_id, &nonce));
    let response = request.send().promise.await?;
    match response.get()?.get_result()?.which()? {
        result::Ok(value) => Ok(value?.get_session()?),
        result::Error(error) => Err(crate::Error::domain(error?)),
    }
}

impl std::fmt::Debug for TcpSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TcpSecret(<redacted>)")
    }
}

/// Secrets compare by derived key so endpoints remain comparable without exposing bytes.
impl PartialEq for TcpSecret {
    fn eq(&self, other: &Self) -> bool {
        purrdf_ed25519::constant_time_eq(self.key.as_slice(), other.key.as_slice())
    }
}
impl Eq for TcpSecret {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn secret_file(dir: &Path, bytes: &[u8], mode: u32) -> std::path::PathBuf {
        let path = dir.join("tcp.secret");
        std::fs::write(&path, bytes).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    /// INVARIANT: only a private regular file of 32..4096 bytes becomes a CHAP key, and
    /// a response verifies only for the same secret, store and single challenge.
    #[test]
    fn secret_files_are_private_bounded_and_bind_store_and_challenge() {
        let dir = tempfile::tempdir().unwrap();
        let readable = secret_file(dir.path(), &[7; 32], 0o640);
        assert!(TcpSecret::read(&readable).is_err());
        let short = secret_file(dir.path(), &[7; 31], 0o600);
        assert!(TcpSecret::read(&short).is_err());
        assert!(TcpSecret::read(dir.path()).is_err());
        let secret = TcpSecret::read(&secret_file(dir.path(), &[7; 32], 0o600)).unwrap();
        let other = TcpSecret::read(&secret_file(dir.path(), &[8; 32], 0o600)).unwrap();
        let nonce = [1; CHAP_BYTES];
        let response = secret.respond("store-a", &nonce);
        assert!(secret.verify("store-a", &nonce, &response));
        assert!(!secret.verify("store-b", &nonce, &response));
        assert!(!secret.verify("store-a", &[2; CHAP_BYTES], &response));
        assert!(!other.verify("store-a", &nonce, &response));
        assert!(!secret.verify("store-a", &nonce, &response[..31]));
        assert_ne!(secret, other);
    }
}
