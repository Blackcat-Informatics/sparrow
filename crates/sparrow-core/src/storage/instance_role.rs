//! The instance role: one metadata row that fixes, at store creation, whether the store is the
//! live one or a scratch one. A store with no row has no role and behaves as it always has.

use super::{Error, open_files, table_options, writer_format};
use rocksdb::{DB, WriteOptions};
use std::path::Path;

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

/// Stamp a stopped store that has no role row with `role` and return its store id. The store is
/// opened exclusively without `Store::open`, which would run format upgrades, fences and
/// migrations, create a missing store; this opens the existing families only and
/// writes the one row with a synced write. A running daemon holds the lock and refuses the open
/// (`CONFLICT`); a store that has a role row (`CONFLICT`, naming it), a format this build does
/// not know (`UNSUPPORTED`) and a directory that is not a Sparrow store (`INVALID_ARGUMENT`)
/// are refused unchanged.
pub fn stamp(data_dir: impl AsRef<Path>, role: InstanceRole) -> Result<String, Error> {
    let path = data_dir.as_ref();
    if !path.join("CURRENT").is_file() {
        return Err(not_a_store(path, "it holds no RocksDB database"));
    }
    let open_files = open_files::OpenFiles::from_process()?;
    let options = table_options::database(open_files.max_open_files, false)?;
    let db = DB::open_cf_descriptors(&options, path, table_options::descriptors()).map_err(
        |error| {
            let cause = error.to_string();
            if cause.contains("lock file") || cause.contains("lock hold") {
                Error::new(
                    "CONFLICT",
                    format!(
                        "{} is open in another process, probably the running daemon; stop it before stamping ({cause})",
                        path.display()
                    ),
                )
            } else {
                Error::new(
                    "STORAGE_FAILURE",
                    format!("cannot open {} to stamp it: {cause}", path.display()),
                )
            }
        },
    )?;
    let Some(format) = writer_format::read(&db)? else {
        return Err(not_a_store(path, "it has no store format row"));
    };
    if !super::is_known_format(&format) {
        return Err(Error::new(
            "UNSUPPORTED",
            format!(
                "{} is at store format {:?}, which this build does not know; stamp it with the build that opens it",
                path.display(),
                String::from_utf8_lossy(&format)
            ),
        ));
    }
    let id = db
        .get("store_id")?
        .ok_or_else(|| not_a_store(path, "it has no store identity"))?;
    let id = String::from_utf8(id).map_err(|e| Error::new("STORAGE_FAILURE", e.to_string()))?;
    if let Some(have) = stored(&db)? {
        return Err(Error::new(
            "CONFLICT",
            format!(
                "store {id} already has instance role \"{}\"; a stored role never changes",
                have.as_str()
            ),
        ));
    }
    let mut write = WriteOptions::default();
    write.set_sync(true);
    db.put_opt(KEY, role.as_str(), &write)?;
    Ok(id)
}

fn not_a_store(path: &Path, why: &str) -> Error {
    Error::new(
        "INVALID_ARGUMENT",
        format!("{} is not a Sparrow store: {why}", path.display()),
    )
}
