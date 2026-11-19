//! The instance role: one metadata row that fixes, at store creation, whether the store is the
//! live one or a scratch one. A store with no row has no role and behaves as it always has.

use super::Error;
use rocksdb::DB;

/// Metadata key holding the role as its configuration word.
pub(crate) const KEY: &str = "instance_role";

/// What a store is for. A store with no role is represented by the absence of this value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InstanceRole {
    /// The live store.
    Production,
    /// A test or throwaway store.
    Scratch,
}

impl InstanceRole {
    /// The configuration and stored word for this role.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::Scratch => "scratch",
        }
    }

    /// Parse the configuration word.
    pub fn parse(word: &str) -> Result<Self, Error> {
        match word {
            "production" => Ok(Self::Production),
            "scratch" => Ok(Self::Scratch),
            other => Err(Error::new(
                "INVALID_ARGUMENT",
                format!(
                    "instance_role = {other:?} is not a role; use \"production\" or \"scratch\""
                ),
            )),
        }
    }
}

/// Read the stored role, `None` when the store has no row.
fn stored(db: &DB) -> Result<Option<InstanceRole>, Error> {
    let Some(bytes) = db.get(KEY)? else {
        return Ok(None);
    };
    let word = std::str::from_utf8(&bytes)
        .map_err(|e| Error::new("STORAGE_FAILURE", format!("stored instance role: {e}")))?;
    InstanceRole::parse(word).map(Some).map_err(|_| {
        Error::new(
            "STORAGE_FAILURE",
            format!("stored instance role {word:?} is unknown; inspect or restore the catalog"),
        )
    })
}

/// Check the configured role against the store's and return the role the store reports. A
/// configured role that differs from the stored one, or that meets a store with no row, stops
/// startup: no operation changes a stored role, and stamping an existing store is an operator
/// step outside the daemon.
pub(crate) fn resolve(
    db: &DB,
    configured: Option<InstanceRole>,
) -> Result<Option<InstanceRole>, Error> {
    let stored = stored(db)?;
    match (stored, configured) {
        (Some(have), Some(want)) if have != want => Err(Error::new(
            "INVALID_ARGUMENT",
            format!(
                "instance_role = \"{}\" in the configuration, but this store is \"{}\"; start it with the configuration for \"{}\" or point data_dir at the intended store (the role never changes after creation)",
                want.as_str(),
                have.as_str(),
                have.as_str()
            ),
        )),
        (None, Some(want)) => Err(Error::new(
            "INVALID_ARGUMENT",
            format!(
                "instance_role = \"{}\" in the configuration, but this store has no instance role; remove the key to run it as before, or stamp the store as a separate operator step",
                want.as_str()
            ),
        )),
        (stored, _) => Ok(stored),
    }
}
