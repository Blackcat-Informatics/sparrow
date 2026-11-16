import re
exec(open("/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/.stage/archive-worker-writes-store-id-on-its/fix2.py").read().split("# first-pass test")[0])
A = "crates/sparrow-server/src/archive.rs"
sub(A, """        let written = fs::metadata(root.join("STORE_ID"))
            .unwrap()
            .modified()
            .unwrap();""", """        let written = fs::metadata(root.join("STORE_ID")).unwrap().ino();""")
sub(A, """            fs::metadata(root.join("STORE_ID"))
                .unwrap()
                .modified()
                .unwrap(),
            written,""", """            fs::metadata(root.join("STORE_ID")).unwrap().ino(),
            written,""")
sub(A, """    /// INVARIANT: the mount rules are pure""", """    /// INVARIANT: a `STORE_ID.partial` left by an interrupted first pass does not make the
    /// directory non-empty: the next pass writes `STORE_ID` and no partial remains.
    #[test]
    fn a_leftover_store_id_partial_does_not_block_the_first_write() {
        let dir = tempfile::tempdir().unwrap();
        let (store, segment) = archiving_store(&dir.path().join("store"));
        let root = dir.path().join("nas");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("STORE_ID.partial"), b"half").unwrap();
        archive_segment(&flight(), &store, &root, "s", &segment).unwrap();
        assert_eq!(fs::read_to_string(root.join("STORE_ID")).unwrap(), "s\\n");
        assert!(!root.join("STORE_ID.partial").exists());
    }

    /// INVARIANT: the mount rules are pure""")
sub(A, "    use sparrow_core::Config;\n", "    use sparrow_core::Config;\n    use std::os::unix::fs::MetadataExt;\n")

R = "crates/sparrow-server/tests/rpc.rs"
s = open(d + R).read()
for ind in ("            ", "        "):
    old = f"{ind}// The archive directory must be off the root filesystem's device (the mount guard);\n{ind}// /dev/shm is a device of its own.\n"
    new = f"{ind}// The mount guard needs the archive directory on a device other than `/`'s and on a\n{ind}// network type or one unlike `/`'s; /dev/shm (tmpfs) is both.\n"
    assert old in s
    s = s.replace(old, new)
open(d + R, "w").write(s)

sub("deploy/README.md", """it refuses a directory that
  is on the root filesystem's device (an unmounted share), and one without a `STORE_ID` file
  naming this store, which the operator writes once. A file naming another store means `dir` is
  that store's archive or the wrong mount.""", """it refuses a directory that
  is on the root filesystem's device, or on a local filesystem of the root's type (an unmounted
  share), and one whose `STORE_ID` names another store or which holds content but no `STORE_ID`.
  It writes `STORE_ID` itself into an empty directory on its first pass. A file naming another
  store means `dir` is that store's archive or the wrong mount. `dir` must be a directory inside
  the share, never the mount point itself.""")
sub("deploy/RECOVERY.md", """followed by an I/O error, a missing `STORE_ID`, or "archive directory ... is not on its
  expected mount".""", """followed by an I/O error, a missing archive directory, a `STORE_ID` naming another store or a
  directory with content but no `STORE_ID`, or "archive directory ... is not on its expected
  mount".""")
sub("deploy/RECOVERY.md", """means the archive directory is on the same device as `/`: the share
  is not mounted and its mount point is an ordinary local directory.""", """means the archive directory is on the same device as `/`, or on a local
  filesystem of the same type as `/`'s (the message names which): the share is not mounted and
  its mount point is an ordinary local directory.""")
I = "docs/interfaces.md"
sub(I, "synced; it is then read back", "synced; it is then read back")
sub(I, "under `<dir>/<store_id>/` as `<from>-<through>.gts`, never anywhere else in `dir`.", "under `<dir>/<store_id>/` as `<from>-<through>.gts`, never anywhere else in `dir` except\nthe `STORE_ID` file below.")
sub(I, "and nothing in `dir` outside\n`<store_id>/` is written.", "and nothing in `dir` outside\n`<store_id>/` and `STORE_ID` is written.")
sub("docs/deployment.md", """pass. A mount point inside a local filesystem of a type other than `/`'s is not caught by these
two checks; `STORE_ID` is the remaining line of defence.""", """pass. A second local btrfs disk mounted as the archive is refused too, with no override. A mount
point inside a local filesystem of a type other than `/`'s passes both checks, and when it is
empty the worker adopts it by writing `STORE_ID`, so segments would go to the local disk;
`STORE_ID` stops only a directory with content or another store's id. Set `[archive] dir` to a
directory inside the share (as `/net/synology/archives/sparrow-archive` is), never to the mount
point itself: an unmounted share leaves no such subdirectory, so the root is missing and refused.""")
