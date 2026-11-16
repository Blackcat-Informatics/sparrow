def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
p='docs/interfaces.md'
s=open(p).read()
s=s.replace("`STORE_ID` file, the one the backup engine writes; the worker only reads it. A missing\nroot, a missing `STORE_ID` (an unmounted mount point) or another store's id is a\nfailure, never a directory made on the local disk,","`STORE_ID` file, which the operator writes once (this store's id and a newline); the worker\nonly reads it. `dir` must also be on a device other than the root filesystem's: a directory\non the same device is refused with the named error \"archive directory ... is not on its\nexpected mount\" before anything is read or written, because an unmounted share leaves its\nmount point as an ordinary local directory. A missing root, a missing `STORE_ID` (an\nunmounted mount point) or another store's id is a\nfailure, never a directory made on the local disk,")
s=s.replace('dir = "/net/synology/archives/sparrow"  # required','dir = "/net/synology/archives/sparrow-archive"  # required')
s=cut(s,"`STORE_ID` is written by the backup engine on the first backup","Unknown fields are refused. Configuration checks",
"""The operator writes `STORE_ID` once, holding this store's id, into `dir`. Until it exists the
archive refuses to write; a file naming another store means `dir` points at that store's
archive or the wrong mount.

""")
s=s.replace(" `deploy/verify-backup.sh` relies on the mode to verify a\nbackup; see `deploy/RECOVERY.md`, \"Verify a backup\".","")
s=s.replace("`query_only=false intake=false`. `deploy/verify-backup.sh --measure-migration`\nuses the mode; see [the deployment guide](deployment.md), \"Measure the online\nmigration\".","`query_only=false intake=false`.")
s=s.replace("may not set `consume`, `syslog`, `rpc_tcp_address`, `archive` or\n`backup_root`, and the refusal names the key; `ingest_socket_path` is allowed.\nA copy shares the live store's identity, so its online backup would land in\nthe live backup root and purge its oldest sets. The mode cannot be\ncombined with query-only mode. The startup line reports\n`query_only=false intake=private`. `deploy/verify-backup.sh --measure-migration\n--with-traffic` uses the mode; see [the deployment guide](deployment.md),\n\"Measure the online migration with intake and traffic\".","may not set `consume`, `syslog`, `rpc_tcp_address` or `archive`, and the\nrefusal names the key; `ingest_socket_path` is allowed. The mode cannot be\ncombined with query-only mode. The startup line reports\n`query_only=false intake=private`.")
s=s.replace("the oldest one recovery needs), which only an online backup holds for long.","the oldest one recovery needs), which RocksDB holds until a reader releases them.")
s=s.replace("The migration's duration on the live store is measured on a copy of the newest\nonline backup, restored by `deploy/verify-backup.sh --measure-migration`, recorded\nin [load qualification](load-qualification.md).","The migration's duration on the live store is recorded in\n[load qualification](load-qualification.md).")
s=s.replace("| `backup` | Administration (schema 70): start an online backup of the store into the configured `backup_root`; see \"Online backup\". |","| `backup` | Removed (#330). The method keeps its ordinal (@53) and answers `UNSUPPORTED`; see \"Removed online backup\". |")
s=cut(s,"### Online backup\n","### Retiring an unrecoverable claim",
"""### Removed online backup

Sparrow backs up nothing except the archive age-out to GTS files on the NAS. The online store
backup (#156) was removed in #330, with `sparrow-backup`, the restore, and the `backup_root` and
`backup_keep` configuration keys. The server file refuses unknown fields, so a file that still
names either key fails to load with the parser's `unknown field` error.

The wire stays additive-only, so no ordinal moved. `backup @53` stays in the schema and the
server answers every call with `UNSUPPORTED` ("the online store backup was removed"); the `sparrow`
CLI has no `backup` command. `Status.backup @55` stays too, and the daemon always reports it as
before for a daemon without a backup: `phase` `unconfigured`, an empty `root` and no `failure`.
`BackupStatus` and `BackupPhase` remain in the schema for those two. CLI and MCP status no longer
render a `backup` object.

""")
open(p,'w').write(s)
