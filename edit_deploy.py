import re
def cut(s,a,b,new=''):
    i=s.index(a); j=s.index(b,i)
    return s[:i]+new+s[j:]
def edit(p,f):
    s=open(p).read(); s=f(s); open(p,'w').write(s)

def lib(s):
    for l in ["#   SPARROW_BACKUPS   the online backup root, on the NAS    /net/synology/archives/sparrow\n",
              "#   SPARROW_INTAKE    the watched intake tree                /SPARROW/intake\n",
              "#   SPARROW_PRE_UPGRADE_MAX_HOURS  the oldest pre-upgrade backup accepted        24\n",
              "SPARROW_BACKUPS=${SPARROW_BACKUPS:-/net/synology/archives/sparrow}\n",
              "SPARROW_INTAKE=${SPARROW_INTAKE:-/SPARROW/intake}\n",
              "PRE_UPGRADE_MAX_HOURS=${SPARROW_PRE_UPGRADE_MAX_HOURS:-24}\n"]:
        s=s.replace(l,"")
    s=s.replace('''cannot open a store that image upgraded. Fix forward, or restore the pre-upgrade backup \\
(RECOVERY.md, \\"Rollback\\")"''','''cannot open a store that image upgraded. Fix forward (RECOVERY.md, \\"Rollback\\")"''')
    s=cut(s,"# Raising the schema may upgrade the store's format, after which only a backup","# Point the stable names at an image pair")
    s=cut(s,"# Hold the backup's lock for the rest of the script","# The unit's ActiveState.")
    s=s.replace("remove once a backup of the converted store has verified.","remove once the converted store has been checked.")
    return s
edit('deploy/lib.sh',lib)

def install(s):
    s=s.replace("# Copy-on-write off for the store (lib.sh, switch_off_copy_on_write). Backups go to the NAS only\n# (Patrick, 2026-11-08); nothing local holds them.\n","# Copy-on-write off for the store (lib.sh, switch_off_copy_on_write).\n")
    s=s.replace("sparrow-ship sparrow-backup katamari","sparrow-ship katamari")
    s=s.replace("sparrow-ship.timer sparrow-backup.timer","sparrow-ship.timer")
    return s
edit('deploy/install.sh',install)
edit('deploy/check.sh',lambda s:s.replace("for timer in sparrow-ship.timer sparrow-backup.timer; do","for timer in sparrow-ship.timer; do"))
edit('deploy/stop.sh',lambda s:s.replace("for timer in sparrow-ship.timer sparrow-backup.timer; do","for timer in sparrow-ship.timer; do").replace("The timers are left as they are: the backup refuses to run\n# while the service is stopped, and the shipper only fills roots.","The timer is left as it is: the shipper only fills roots.").replace('note "$timer is','note "$timer is'))
def unin(s):
    s=s.replace("sparrow-ship.timer sparrow-backup.timer","sparrow-ship.timer")
    s=s.replace("/SPARROW/ship and the backups on the NAS (/net/synology/archives/sparrow). It never","/SPARROW/ship. It never")
    s=s.replace("/SPARROW/ship and the backups under $SPARROW_BACKUPS\"","/SPARROW/ship\"")
    return s
edit('deploy/uninstall.sh',unin)
def start(s):
    s=s.replace("       deploy/start.sh --restored-from BACKUP\n","")
    s=cut(s,"\n--restored-from starts a store restored","USAGE\n    exit 0")
    s=s.replace("restart.sh to replace a running image.\n","restart.sh to replace a running image.\nUSAGE\n    exit 0",1) if False else s
    s=s.replace("REV='' RESTORED='' RESTORED_STORE=''","REV=''")
    s=s.replace("    --restored-from) RESTORED=${2:?--restored-from needs a backup directory}; shift 2 ;;\n","")
    s=s.replace("hold_backup_lock\n\n","\n")
    s=cut(s,"if [[ -n $RESTORED ]]; then\n    [[ -r","require_settled\n","images=$(select_images \"$REV\") || exit 1\nread -r daemon cli <<<\"$images\"\n")
    s=s.replace("if [[ -z $RESTORED ]]; then\n    refuse_schema_downgrade \"$daemon\"\n    require_pre_upgrade_backup \"$daemon\"\nfi\n","refuse_schema_downgrade \"$daemon\"\n")
    s=cut(s,"if [[ -n $RESTORED ]]; then\n    now=","receipt\n",)
    return s
edit('deploy/start.sh',start)
def restart(s):
    s=s.replace("hold_backup_lock\n\n","\n")
    s=s.replace('require_pre_upgrade_backup "$daemon"\n','')
    s=s.replace("(by the backup, say)","(by systemd, say)")
    return s
edit('deploy/restart.sh',restart)
def ti(s):
    s=cut(s,"#\n# pre-upgrade-backup.sh is not rehearsed here","set -euo pipefail")
    s=s.replace("    printf 'export SPARROW_BACKUPS=%q SPARROW_INTAKE=%q\\n' \"$SCRATCH/backups\" \"$DATA/intake\"\n","")
    return s
edit('deploy/test-instance.sh',ti)
