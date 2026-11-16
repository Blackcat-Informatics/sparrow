#!/bin/bash
set -e
f=writer_format.rs
consts=/home/paudley/Active/sparrow/.worktrees/323-index-artifacts-by-payload-id-the-store/.stage/index-artifacts-by-payload-id-the-store/pi_consts.txt
# insert consts + PAYLOAD_INDEX array before "fn position("
awk -v consts="$consts" '
/^fn position\(families/ && !done {
  print "// Payload id index (#323): older binaries cannot write or delete a `payload-id:` key with the"
  print "// artifact entry it follows. Each family lifts one store counter family by 55, keeping its"
  print "// log time, archive and intake switches."
  while ((getline line < consts) > 0) print line
  print ""
  print "/// Families the payload index fence lifts, in the order of their lifted counterparts."
  print "const PAYLOAD_INDEX: [[&[u8]; 11]; 5] = ["
  print "    PAYLOAD_INDEX_TRACING,"
  print "    PAYLOAD_INDEX_LOG_TIME,"
  print "    PAYLOAD_INDEX_ARCHIVE,"
  print "    PAYLOAD_INDEX_INTAKE,"
  print "    PAYLOAD_INDEX_INTAKE_ARCHIVE,"
  print "];"
  print ""
  done=1
}
{ print }' $f > $f.new
mv $f.new $f
