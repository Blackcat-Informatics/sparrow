import sys
d = "/home/paudley/Active/sparrow/.worktrees/332-archive-worker-writes-store-id-on-its/"
p = d + "crates/sparrow-server/src/archive.rs"
s = open(p).read()
a = s.index("/// The archive directory must not share a device")
b = s.index("/// Make one segment durable")
s = s[:a] + open(d + ".stage/archive-worker-writes-store-id-on-its/new_guard.rs").read() + s[b:]
old = """/// `root` must already exist, be on a device other than the root filesystem's, and hold this
/// store's `STORE_ID`: a missing or unmounted root is a failure, never a directory created on the
/// local disk. Only `<root>/<store_id>` is
/// created; nothing else in `root` is touched."""
new = """/// `root` must already exist and be on its own mount (see [`require_own_mount`]): a missing or
/// unmounted root is a failure, never a directory created on the local disk. It must hold this
/// store's `STORE_ID`, which an empty root is given here (see [`ensure_store_id`]). Only
/// `<root>/<store_id>` and `<root>/STORE_ID` are created; nothing else in `root` is touched."""
assert old in s
s = s.replace(old, new)
assert "require_store_id(flight" in s
s = s.replace("require_store_id(flight, root, store_id)?;", "ensure_store_id(flight, root, store_id)?;")
open(p, "w").write(s)
