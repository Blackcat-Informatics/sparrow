p='crates/sparrow-archive/src/decode.rs'
s=open(p).read()
s=s.replace('''//! artifacts; replacing the format replaces this module only. Listing, naming and the
//! restore flow never see the format.''','''//! artifacts; replacing the format replaces this module only. Listing, naming and
//! extraction never see the format.''')
s=s.replace('''    pub(super) const ENTITY: &str = "urn:sparrow:";
''','''    pub(super) const ENTITY: &str = "urn:sparrow:";
    /// An artifact's `payload` object: `urn:sparrow:payload:<hex>`, the BLAKE3 of its bytes.
    pub(super) const PAYLOAD_IRI: &str = "urn:sparrow:payload:";
''')
s=s.replace('''            Some("byteLength") => entry.byte_length = Some(object),
''','''            Some("byteLength") => entry.byte_length = Some(object),
            Some("payload") => entry.artifact_payload = Some(object),
''')
s=s.replace('''                byte_length: integer(&id, "byteLength", facts.byte_length)?,
                id,
            }),''','''                byte_length: integer(&id, "byteLength", facts.byte_length)?,
                payload_id: payload_id(&id, facts.artifact_payload)?,
                id,
            }),''')
s=s.replace('''    payload: Option<String>,
}
''','''    payload: Option<String>,
    artifact_payload: Option<String>,
}

/// The artifact's store identity, `blake3:<hex>`, from its `payload` object.
fn payload_id(id: &str, object: Option<String>) -> Result<String, String> {
    let object = object.ok_or_else(|| format!("{id} has no payload"))?;
    let hex = object
        .strip_prefix(vocabulary::PAYLOAD_IRI)
        .ok_or_else(|| format!("{id} has a payload {object:?} that is not a payload identity"))?;
    Ok(format!("blake3:{hex}"))
}
''',1)
open(p,'w').write(s)

p='crates/sparrow-archive/src/segment.rs'
s=open(p).read()
s=s.replace('''    /// Full original byte count.
    pub byte_length: u64,
}''','''    /// Full original byte count.
    pub byte_length: u64,
    /// The store's identity of the artifact's bytes, `blake3:<hex>`.
    pub payload_id: String,
}''')
open(p,'w').write(s)

p='crates/sparrow-archive/src/lib.rs'
s=open(p).read()
s=s.replace('''//! `decode::decode`, the only code that knows the byte format. Nothing here opens a live store or writes to the archive;
//! [`restore`] writes only into the store the caller hands it.''','''//! `decode::decode`, the only code that knows the byte format. Nothing here opens a live
//! store or writes to the archive; [`extract`] returns one artifact's bytes to the caller.''')
s=s.replace('mod restore;','mod extract;').replace('pub use restore::{RestoreReport, restore};','pub use extract::extract;')
s=s.replace('mod decode;\nmod error;\nmod extract;','mod decode;\nmod error;\nmod extract;')
open(p,'w').write(s)

p='crates/sparrow-archive/src/error.rs'
s=open(p).read()
s=s.replace('decoded or restored','decoded or extracted')
s=s.replace('''    /// The destination store refused a restore.
    Store(sparrow_core::Error),
''','''    /// No segment holds the artifact.
    NotFound {
        /// The artifact identity asked for.
        artifact_id: String,
    },
''')
s=s.replace('''            Self::Store(error) => write!(f, "store refused the restore: {error}"),''','''            Self::NotFound { artifact_id } => {
                write!(f, "no segment under the directory holds {artifact_id}")
            }''')
s=s.replace('''            Self::Store(error) => Some(error),
''','')
open(p,'w').write(s)

p='crates/sparrow-archive/Cargo.toml'
s=open(p).read()
s=s.replace('purrdf.workspace = true\n','purrdf.workspace = true\npurrdf-hash.workspace = true\n',1)
open(p,'w').write(s)
