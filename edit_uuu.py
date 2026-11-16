p='deploy/user-unit-upgrade.sh'
s=open(p).read()
def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
s=cut(s,"# Switch the operator's old user unit","# Every check that can refuse runs before anything changes.",
"""# Switch the operator's old user unit sparrow.service from one kept image pair to another.
#
# What an upgrade relies on: Sparrow backs up nothing but the archive age-out to GTS files, so
# this script takes no backup and requires none. The undo is the kept previous image (FROM stays in
# the libexec directory) and the link swap that points the daemon and CLI links back at it. That
# undo holds only while TO has not changed the store's format: a format-changing upgrade has no
# undo, because the store's fence makes FROM refuse a store TO has upgraded, and the way out is to
# fix forward on TO. It serves until Sparrow issue #22 moves the store to the system unit.
# Ported from the operator's restart-bundle.sh; its predecessor restart-schema68.sh ran clean on
# the live host on 2026-11-07 (exit 0, 180 s down).
""")
s=cut(s,"Usage: deploy/user-unit-upgrade.sh --from","Environment (defaults are the user-unit layout):",
"""Usage: deploy/user-unit-upgrade.sh --from DAEMON_IMAGE --to DAEMON_IMAGE

Switch the user unit sparrow.service (systemctl --user) from the kept daemon image --from to
--to, both names under the libexec directory such as sparrowd-schema68-2aa56a9 and
sparrowd-schema69-3a64e08. Each CLI image is the daemon's name with sparrowd- replaced by
sparrow-; the schema numbers come from the names. Run as the operator, never as root.

What the upgrade relies on: no backup exists to restore from (Sparrow backs up nothing but the
archive age-out to GTS files). The undo is the kept previous image and the link swap. A
format-changing upgrade has no undo: once TO opens the store and upgrades its format, FROM
refuses it (the store fence), so the only way out is forward on TO.

Before anything changes it refuses unless: the daemon link names --from; all four images are
executable; the --to pair is byte-identical to the kept pair in SPARROW_BIN; the --to daemon
carries cap_net_bind_service (getcap: a human with root runs setcap, this script never does);
server.toml sets the ingest keys; the store has copy-on-write off; the unit's drop-in directory
holds no .conf but an identical limits.conf (deploy/systemd/user/sparrow.service.d); and the
running daemon reports the --from schema. Then it stops sparrow-ship.timer and waits for the
shipper, stops the unit, installs limits.conf (LimitNOFILE=65536, the store refuses to open below
2048) with daemon-reload and checks the unit's LimitNOFILESoft, switches the links, starts the
unit, waits up to 600 s for the first status answer, checks the store identity and the --to
schema, prints the migrations, watches and ingest socket, and starts sparrow-ship.timer again. It
exits 0 without waiting for the online backfill, which continues in the running daemon; watch it
with "sparrow SOCKET status" (migrations) and journalctl --user -u sparrow.service. If the
shipper is still running when the timer has been stopped, the run starts the timer again if it
was active before and changes nothing else.

""")
s=s.replace("""  SPARROW_BACKUPS  the online backup root (server.toml backup_root), on the NAS
                                                                 /net/synology/archives/sparrow
""","")
s=s.replace("""  SPARROW_STATE    the state tree holding ship and ship-watches  ~/.local/state/sparrow
  SPARROW_INTAKE   the intake tree the backup set keeps          ~/.local/state/sparrow/intake
""","")
s=cut(s,"Recovery depends on where it failed;","USAGE\n}\n",
"""Recovery depends on where it failed; the FAILED line names the unit and timer states.
- Before "stop timers": nothing stopped and the links name FROM. Fix the cause and rerun.
- From "stop timers" up to "switch links": the links still name FROM and the store is untouched.
  Start sparrow.service (if stopped) and sparrow-ship.timer. An installed limits.conf may stay:
  either image needs it.
- At "switch links", before "start sparrow.service": check both links with readlink; one may
  name TO and the other FROM. Point both at FROM and recover as in the step above, or both at
  TO and start sparrow.service, then continue as below.
- After "start sparrow.service": unless the FAILED line shows the unit inactive or failed, the
  daemon runs TO with the timer stopped, and TO may have accepted ingest and watch work. Go
  forward first: wait for "sparrow SOCKET status" to answer (a slow open passes 600 s without
  failing), confirm its store_id and the TO schema, then start sparrow-ship.timer.
  Roll back by pointing the links at FROM only if TO has not upgraded the store's format (check
  TO's open in journalctl --user -u sparrow.service: a format upgrade is logged and the FROM
  image then refuses the store). After a format change there is no undo: fix forward on TO.
""")
s=s.replace("FROM='' TO='' NO_BACKUP=0 NO_BACKUP_REASON=''","FROM='' TO=''")
s=cut(s,"        --no-backup)\n","        --from | --to)")
s=s.replace("# The user unit's state tree, as sparrow-backup.service passes it (%h/.local/state/sparrow/...).\nSPARROW_STATE=${SPARROW_STATE:-${HOME:?}/.local/state/sparrow}\nSPARROW_INTAKE=${SPARROW_INTAKE:-$SPARROW_STATE/intake}\n","")
s=s.replace("# The user-unit pre-upgrade backup prints its own progress; lib.sh supplies the backup root.\n","")
s=s.replace("BACKUP=$SPARROW_BACKUPS/pre-upgrade-$FROM\n","")
s=s.replace("(verify-backup.sh --measure-migration, 2026-11-07, image 3a64e08)","(measured 2026-11-07, image 3a64e08)")
s=s.replace('[[ ! -e $BACKUP && ! -L $BACKUP ]] || die "pre-upgrade backup $BACKUP exists; move it aside first"\n',"")
s=s.replace('[[ -r $CONFIG_DIR/ship.toml ]] || die "cannot read $CONFIG_DIR/ship.toml; the backup keeps it"\n',"")
s=cut(s,"# TO must start with its online backup configured","# After the preconditions, a failure says what state it left")
s=s.replace("timers: ship %s, backup %s. Read","timer: ship %s. Read")
s=s.replace('''        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)" \\
        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-backup.timer)" >&2''','''        "$("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)" >&2''')
s=s.replace('# A running oneshot backup is "activating", which is-active does not count; settled does.\nsettled sparrow-backup.service || die "the nightly backup is running"\n',"")
s=cut(s,'if ((NO_BACKUP)); then\n    log "NO PRE-UPGRADE BACKUP (--no-backup)',"# Which timers ran before")
s=cut(s,"# Which timers ran before this script stopped them","log \"stop timers\"","# Whether the shipper timer ran before this script stopped it: the abort path restores it.\nship_timer_was_active=0\n\"${USER_SYSTEMCTL[@]}\" is-active --quiet sparrow-ship.timer && ship_timer_was_active=1\n")
s=s.replace('log "stop timers"\n"${USER_SYSTEMCTL[@]}" stop sparrow-ship.timer sparrow-backup.timer','log "stop timers"\n"${USER_SYSTEMCTL[@]}" stop sparrow-ship.timer')
s=cut(s,'if ! settled sparrow-ship.service || ! settled sparrow-backup.service; then','T0=$EPOCHREALTIME',
'''if ! settled sparrow-ship.service; then
    ((ship_timer_was_active == 0)) || "${USER_SYSTEMCTL[@]}" start sparrow-ship.timer
    die "the shipper is still running; sparrow-ship.timer is started again if it was active before, nothing else changed"
fi
''')
s=cut(s,"# sparrow-backup.timer stays stopped: it restarts","log \"switched to $TO.")
s=s.replace('log "switched to $TO.','log "timer: ship $("${USER_SYSTEMCTL[@]}" is-active sparrow-ship.timer)"\nlog "switched to $TO.')
open(p,'w').write(s)
