d="/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/"
def sub(path, old, new):
    s=open(d+path).read()
    assert old in s, (path, old[:40])
    open(d+path,"w").write(s.replace(old,new,1))

sub("docs/architecture.md","""the 60-second read lease is halved and the shortening is reported to the worker.
""","""the 60-second read lease is halved and the shortening is reported to the worker.
The worker writes nothing until the archive directory is on its own mount (a device
other than `/`'s, and a network filesystem type or one unlike `/`'s) and holds this
store's `STORE_ID`, which it writes itself into an empty directory on the first pass and
never overwrites or adopts into a directory that already has content.
""")

sub("docs/deployment.md","""The archive worker also refuses to write when `[archive] dir` is on the same device as `/`. An
unmounted NFS share leaves its mount point as an ordinary directory on the local root disk, and
writing archives there would fill it. The refusal is the named error "archive directory ... is not
on its expected mount", is journaled on the pass failure line, and writes, moves and deletes
nothing; the daemon itself starts and serves as before, and the worker retries on its next pass. The
operator writes `STORE_ID` (this store's id and a newline) into the archive directory once; the
worker only reads it.""","""The archive worker also refuses to write when `[archive] dir` is not on its own mount. An
unmounted NFS share leaves its mount point as an ordinary directory on the local root disk, and
writing archives there would fill it. Two checks apply, in order: the directory's device must
differ from `/`'s, and its filesystem type must be a network filesystem (NFS, SMB/CIFS, Ceph, AFS,
9P, Lustre, NCP, Coda) or differ from `/`'s type. The second closes the case of an unmounted share
whose parent is another local mount of the root's type, such as a btrfs subvolume under `/home`.
The refusal is the named error "archive directory ... is not on its expected mount", which names
the device or filesystem type found, is journaled on the pass failure line, and writes, moves and
deletes nothing; the daemon itself starts and serves as before, and the worker retries on its next
pass. A mount point inside a local filesystem of a type other than `/`'s is not caught by these
two checks; `STORE_ID` is the remaining line of defence.

The worker writes `STORE_ID` itself. On its first pass, a directory that is on its mount and
empty (no entry at all, apart from a `STORE_ID.partial` left by an interrupted first pass) gets a
`STORE_ID` holding this store's id and a newline, written durably (temp file, fsync, rename,
directory fsync). An existing `STORE_ID` naming this store is accepted as is; one naming another
store is refused and never overwritten. A directory that holds any other entry but no `STORE_ID`,
such as segments whose id file was lost, is refused and not adopted: write the id by hand only
once you have confirmed the directory is this store's archive.""")

sub("docs/interfaces.md","""`dir` must already exist and hold this store's
`STORE_ID` file, which the operator writes once (this store's id and a newline); the worker
only reads it. `dir` must also be on a device other than the root filesystem's: a directory
on the same device is refused with the named error "archive directory ... is not on its
expected mount" before anything is read or written, because an unmounted share leaves its
mount point as an ordinary local directory. A missing root, a missing `STORE_ID` (an
unmounted mount point) or another store's id is a
failure,""","""`dir` must already exist and be on its own mount: a device other than the root
filesystem's, and a filesystem type that is a network filesystem (NFS, SMB/CIFS, Ceph, AFS, 9P,
Lustre, NCP, Coda) or differs from the root's. Otherwise it is refused with the named error
"archive directory ... is not on its expected mount", which names the device or type found,
before anything is read or written, because an unmounted share leaves its mount point as an
ordinary local directory. `dir` must hold this store's `STORE_ID` file (this store's id and a
newline). The worker writes it on its first pass when `dir` is empty (no entry at all, apart
from a leftover `STORE_ID.partial`), durably: temp file, fsync, rename, directory fsync. It
accepts a matching file, never overwrites one, and refuses a directory that holds other
entries but no `STORE_ID`. A missing root, another store's id or a non-empty directory
without `STORE_ID` is a
failure,""")

sub("docs/interfaces.md","""The operator writes `STORE_ID` once, holding this store's id, into `dir`. Until it exists the
archive refuses to write; a file naming another store means `dir` points at that store's
archive or the wrong mount.""","""The archive worker writes `STORE_ID`, holding this store's id, into `dir` on its first pass when
`dir` is on its mount and empty. A directory that has content but no `STORE_ID` is refused, not
adopted; a file naming another store means `dir` points at that store's archive or the wrong
mount, and is never overwritten.""")
