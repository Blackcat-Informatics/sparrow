d = "/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/"
p = d + "crates/sparrow-server/src/archive.rs"
s = open(p).read()
old_start = s.index("    /// INVARIANT: a root that is not this store's (an unmounted")
old_end = s.index("    /// INVARIANT: an archive directory on the root filesystem's device")
new = '''    /// INVARIANT: a root that is not this store's is refused, nothing is written into it and
    /// the watermark stays: another store's `STORE_ID` is never overwritten, and a directory
    /// that holds segments but no `STORE_ID` is not empty and is not adopted.
    #[test]
    fn the_watermark_stays_when_the_root_is_not_this_stores() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let other = nas(dir.path(), "another-store");
        let error = archive_segment(&flight(), &store, &other, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert!(!other.join("s").exists());
        assert_eq!(
            fs::read_to_string(other.join("STORE_ID")).unwrap(),
            "another-store\\n",
            "the other store's id is not overwritten"
        );
        assert_eq!(
            fs::read_dir(&other).unwrap().count(),
            1,
            "the other store's root holds only its STORE_ID"
        );
        let stranger = dir.path().join("stranger");
        fs::create_dir_all(stranger.join("s")).unwrap();
        fs::write(stranger.join("s").join("0-1.gts"), b"segment").unwrap();
        let error = archive_segment(&flight(), &store, &stranger, "s", &segment).unwrap_err();
        assert!(matches!(error, ArchiveError::Root { .. }), "{error}");
        assert!(error.to_string().contains("not adopted"), "{error}");
        assert!(!stranger.join("STORE_ID").exists(), "never adopted");
        assert_eq!(through(&store), None);
    }

    /// INVARIANT: the first pass over an empty archive directory on its mount writes this
    /// store's `STORE_ID` itself (`<id>` and a newline, no partial left behind), the segment
    /// follows, and a second pass accepts the file it wrote without rewriting it.
    #[test]
    fn the_first_pass_writes_store_id_into_an_empty_directory_and_the_second_accepts_it() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = dir.path().join("nas");
        fs::create_dir(&root).unwrap();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(fs::read_to_string(root.join("STORE_ID")).unwrap(), "s\\n");
        assert!(!root.join("STORE_ID.partial").exists());
        assert_eq!(through(&store).as_deref(), Some(segment.through.as_str()));
        let written = fs::metadata(root.join("STORE_ID")).unwrap().modified().unwrap();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(
            fs::metadata(root.join("STORE_ID")).unwrap().modified().unwrap(),
            written,
            "an existing matching STORE_ID is accepted, not rewritten"
        );
    }

    /// INVARIANT: the mount rules are pure over device and filesystem type: the same device
    /// is refused whatever the type; a different device is accepted on a network filesystem
    /// and on a local type unlike the root's, and refused on the root's own local type (an
    /// unmounted share under another local subvolume), with the type named in the message.
    #[test]
    fn the_mount_rules_refuse_the_roots_device_and_its_local_filesystem_type() {
        const NFS: u64 = 0x6969;
        const BTRFS: u64 = 0x9123_683E;
        const TMPFS: u64 = 0x0102_1994;
        let placement = |device, f_type| Placement { device, f_type };
        let root = placement(1, BTRFS);
        let table = [
            (placement(1, BTRFS), false),
            (placement(1, NFS), false),
            (placement(2, NFS), true),
            (placement(2, 0xFF53_4D42), true),
            (placement(2, TMPFS), true),
            (placement(2, BTRFS), false),
        ];
        for (found, accepted) in table {
            let verdict = require_own_mount(Path::new("/a"), found, Path::new("/"), root);
            assert_eq!(
                verdict.is_ok(),
                accepted,
                "device {} type {:#x}: {verdict:?}",
                found.device,
                found.f_type
            );
        }
        let error = require_own_mount(Path::new("/a"), placement(2, BTRFS), Path::new("/"), root)
            .unwrap_err();
        assert!(matches!(error, ArchiveError::NotOnMount { .. }), "{error}");
        assert!(error.to_string().contains("0x9123683e"), "{error}");
        assert_eq!(network_filesystem(NFS), Some("nfs"));
        assert_eq!(network_filesystem(BTRFS), None);
    }

'''
s = s[:old_start] + new + s[old_end:]
s = s.replace('''        assert_eq!(fs::read_dir(&root).unwrap().count(), 1, "only its STORE_ID");
        assert_eq!(through(&store), None);
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();''','''        assert_eq!(fs::read_dir(&root).unwrap().count(), 1, "only its STORE_ID");
        assert_eq!(through(&store), None);
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();''')
open(p, "w").write(s)
