/// Filesystem magic numbers (`statfs` `f_type`) of the network filesystems an archive share is
/// mounted as: NFS, SMB (the legacy `smb`, CIFS and SMB2), Ceph, AFS, 9P, Lustre, NCP and
/// Coda. FUSE is left out: it fronts local storage as often as a share.
const NETWORK_FILESYSTEMS: &[(u64, &str)] = &[
    (0x6969, "nfs"),
    (0x517B, "smb"),
    (0xFF53_4D42, "cifs"),
    (0xFE53_4D42, "smb2"),
    (0x00C3_6400, "ceph"),
    (0x5346_414F, "afs"),
    (0x0102_1997, "9p"),
    (0x0BD0_0BD0, "lustre"),
    (0x564C, "ncp"),
    (0x7375_7245, "coda"),
];

/// The name of a network filesystem type, or `None` for any other.
fn network_filesystem(f_type: u64) -> Option<&'static str> {
    NETWORK_FILESYSTEMS
        .iter()
        .find_map(|&(magic, name)| (magic == f_type).then_some(name))
}

/// Where a path lives: its device and its filesystem type (`statfs` `f_type`).
#[derive(Clone, Copy)]
struct Placement {
    device: u64,
    f_type: u64,
}

/// The archive directory must be on its own mount. The archive sits below a network mount
/// point; when the share is mounted the directory has the share's device and a network
/// filesystem type, and when it is not, the mount point is an ordinary directory of some
/// local filesystem. Two rules, pure over the values read from the directory and from `/`:
///
/// 1. the device must differ from the root filesystem's; and
/// 2. the filesystem type must be a network filesystem or differ from the root's type. This
///    catches an unmounted share whose parent is another local mount of the root's type (a
///    btrfs subvolume under `/home`), which has a device of its own. A mount point inside a
///    local filesystem of a type other than `/`'s is not caught here; the `STORE_ID` rule is
///    the remaining line of defence there.
fn require_own_mount(
    dir: &Path,
    found: Placement,
    filesystem_root: &Path,
    root: Placement,
) -> Result<(), ArchiveError> {
    let refuse = |found: String| ArchiveError::NotOnMount {
        path: dir.to_path_buf(),
        filesystem_root: filesystem_root.to_path_buf(),
        found,
    };
    if found.device == root.device {
        return Err(refuse(format!(
            "it is on device {}, the same as {}",
            found.device,
            filesystem_root.display()
        )));
    }
    if network_filesystem(found.f_type).is_none() && found.f_type == root.f_type {
        return Err(refuse(format!(
            "its filesystem type is {:#x}, the same local type as {} and not a network filesystem",
            found.f_type,
            filesystem_root.display()
        )));
    }
    Ok(())
}

/// Read the two placements and apply [`require_own_mount`]. Read only.
fn require_mounted(flight: &Flight, dir: &Path) -> Result<(), ArchiveError> {
    use std::os::unix::fs::MetadataExt;
    let place = |path: &Path| {
        let _step = flight.step("read device", path);
        let device = fs::metadata(path)
            .map(|metadata| metadata.dev())
            .map_err(io_at("read device", path))?;
        let stat = rustix::fs::statfs(path)
            .map_err(|errno| io_at("read filesystem type", path)(errno.into()))?;
        // The kernel's word is signed on some targets; the magic numbers are 32-bit patterns.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let f_type = u64::from(stat.f_type as u32);
        Ok::<_, ArchiveError>(Placement { device, f_type })
    };
    require_own_mount(
        dir,
        place(dir)?,
        &flight.filesystem_root,
        place(&flight.filesystem_root)?,
    )
}

/// Whether the root holds nothing an archive owns: no entry at all, apart from the
/// `STORE_ID.partial` an earlier first pass may have left before its rename.
fn is_empty_root(flight: &Flight, root: &Path) -> Result<bool, ArchiveError> {
    let _step = flight.step("list archive root", root);
    for entry in fs::read_dir(root).map_err(io_at("list archive root", root))? {
        let entry = entry.map_err(io_at("list archive root", root))?;
        if entry.file_name() != "STORE_ID.partial" {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The root must hold this store's `STORE_ID`. A root that is on its mount and empty of
/// archive content gets it written here, durably, on the first pass; a root that holds any
/// other entry but no `STORE_ID` is refused, never adopted, and an existing `STORE_ID` is
/// never overwritten: it is accepted when it names this store and refused when it names
/// another.
fn ensure_store_id(flight: &Flight, root: &Path, store_id: &str) -> Result<(), ArchiveError> {
    let path = root.join("STORE_ID");
    let read = {
        let _step = flight.step("read STORE_ID", &path);
        fs::read_to_string(&path)
    };
    let recorded = match read {
        Ok(recorded) => recorded,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            if !is_empty_root(flight, root)? {
                return Err(ArchiveError::Root {
                    path: root.to_path_buf(),
                    reason: "STORE_ID is missing but the directory holds other entries, so it is \
                             not an empty archive root and is not adopted. Point `[archive] dir` \
                             at an empty directory, or write this store's id and a newline into \
                             STORE_ID if the directory is this store's archive"
                        .into(),
                });
            }
            {
                let _step = flight.step("write STORE_ID", &path);
                publish(root, "STORE_ID", format!("{store_id}\n").as_bytes())?;
            }
            return sync_dir(flight, root);
        }
        Err(source) => return Err(io_at("read", &path)(source)),
    };
    if recorded.trim_end() == store_id {
        Ok(())
    } else {
        Err(ArchiveError::Root {
            path: root.to_path_buf(),
            reason: format!(
                "STORE_ID names {:?}, not {store_id}; this root belongs to another store. \
                 Point `[archive] dir` at this store's archive directory, or mount the right archive",
                recorded.trim_end()
            ),
        })
    }
}

