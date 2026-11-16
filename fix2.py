d = "/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/"


def sub(path, old, new):
    s = open(d + path).read()
    assert old in s, (path, old[:50])
    open(d + path, "w").write(s.replace(old, new, 1))


A = "crates/sparrow-server/src/archive.rs"
sub(A, """    /// The directory whose device is the local root disk: `/`. An archive directory on this
    /// device is not on its mount.""", """    /// The directory standing for the local root disk: `/`. An archive directory on its
    /// device, or on a local filesystem of its type, is not on its mount.""")
sub(A, "(0x5346_414F, \"afs\")", "(0x6B41_4653, \"afs\")")
sub(A, """`<root>/<store_id>` and `<root>/STORE_ID` are created; nothing else in `root` is touched. A step that ran at least `io_timeout_secs`
/// fails""", """`<root>/<store_id>` and `<root>/STORE_ID` are created; nothing else in `root` is touched.
/// A step that ran at least `io_timeout_secs`
/// fails""")
sub(A, "/// An archive root that holds `id` as its `STORE_ID`, as the operator writes it.",
    "/// An archive root that already holds `id` as its `STORE_ID`, as an earlier pass left it.")
# residual-gap comment
s = open(d + A).read()
i = s.index("///    btrfs subvolume under `/home`)")
j = s.index("fn require_own_mount")
s = s[:i] + """///    btrfs subvolume under `/home`), which has a device of its own. It also refuses a
///    second local disk of the root's type mounted as the archive. A mount point inside a
///    local filesystem of a type other than `/`'s passes both rules, and when it is empty
///    [`ensure_store_id`] adopts it and the segments go to the local disk. `STORE_ID` stops
///    only a directory that has content or another store's id. The configured `dir` must
///    therefore be a directory inside the share, never the mount point itself: an
///    unmounted share leaves no such subdirectory, so the root is then missing and refused.
""" + s[j:]
open(d + A, "w").write(s)

# first-pass test: inode instead of mtime; partial test
s = open(d + A).read()
s = s.replace("""        let written = fs::metadata(root.join("STORE_ID"))
            .unwrap()
            .modified()
            .unwrap();""", """        let written = fs::metadata(root.join("STORE_ID")).unwrap().ino();""")
open(d + A, "w").write(s)
