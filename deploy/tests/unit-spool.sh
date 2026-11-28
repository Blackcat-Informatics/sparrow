#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Checks deploy/libexec/sparrow-unit-spool against a fixture journal. journalctl is replaced by a
# test double that prints the fixture whatever its arguments, so the checks cover the spool's own
# filtering, records and cursor, not journalctl's selection.
#
# drop.jsonl is the recorded default output for the fixture; the default must still produce it byte for byte. keep.jsonl is the output with
# SPARROW_UNIT_SPOOL_OWN_LINES=keep. Prints PASS, or names the failed check and exits 1.
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
script=$here/../libexec/sparrow-unit-spool
fixtures=$here/unit-spool
unit=example-sync.service
work=$(mktemp -d "${TMPDIR:-/tmp}/unit-spool-test.XXXXXX")
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin"
cat > "$work/bin/journalctl" <<'DOUBLE'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$JOURNAL_ARGS_LOG"
# Like journalctl, print only the entries after --after-cursor, not after the fixture's end.
after=
for arg in "$@"; do
    case $arg in --after-cursor=*) after=${arg#--after-cursor=} ;; esac
done
if [[ -z $after ]]; then
    cat -- "$JOURNAL_FIXTURE"
else
    seen=0
    while IFS= read -r line; do
        if (( seen )); then
            printf '%s\n' "$line"
        elif [[ $line == *"\"__CURSOR\":\"$after\""* ]]; then
            seen=1
        fi
    done < "$JOURNAL_FIXTURE"
fi
DOUBLE
chmod +x "$work/bin/journalctl"
export PATH=$work/bin:$PATH JOURNAL_FIXTURE=$fixtures/journal.jsonl JOURNAL_ARGS_LOG=$work/journalctl.args

fail() { printf 'unit-spool: FAIL: %s\n' "$*" >&2; exit 1; }
records() { cat -- "$1"/unit-spool-*.jsonl 2>/dev/null || true; }

# Default: the output is the recorded drop output, byte for byte.
bash "$script" "$unit" "$work/drop" "$work/drop.cursor"
cmp -s <(records "$work/drop") "$fixtures/drop.jsonl" || fail "default output differs from drop.jsonl"
[[ $(cat -- "$work/drop.cursor") == 's=fixture;i=4' ]] || fail "default cursor is not the last kept line"

# Keep: the unit's own lines and the manager lines are both spooled, told apart by source.
SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$work/keep" "$work/keep.cursor"
cmp -s <(records "$work/keep") "$fixtures/keep.jsonl" || fail "keep output differs from the expected records"
[[ $(cat -- "$work/keep.cursor") == 's=fixture;i=6' ]] || fail "keep cursor is not the last line"

# A second run resumes after the cursor: journalctl is asked for the entries after it, not the whole journal.
SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$work/keep" "$work/keep.cursor"
second=$(tail -n 1 -- "$work/journalctl.args")
[[ $second == *"--after-cursor=s=fixture;i=6"* ]] || fail "a resumed run did not ask journalctl for the entries after the cursor"
[[ $second != *"--lines=all"* ]] || fail "a resumed run read the whole journal"
cmp -s <(records "$work/keep") "$fixtures/keep.jsonl" || fail "a resumed run spooled the records again"

# A record that names a unit whose name extends ours is not the unit's own line.
grep -q '"message":"line from a unit whose name extends ours"' "$work/keep/unit-spool-20261127T1310.jsonl" || fail "extended unit name lost"
grep '"message":"line from a unit whose name extends ours"' "$work/keep/unit-spool-20261127T1310.jsonl" | grep -q '"source":"systemd-manager"' || fail "extended unit name matched as the unit's own"

# The cursor never runs ahead of the spool: when the first append fails, the cursor is not written.
mkdir -p "$work/blocked"
mkdir "$work/blocked/unit-spool-20261127T1310.jsonl"
if SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$work/blocked" "$work/blocked.cursor" 2>/dev/null; then
    fail "a failed append exited 0"
fi
[[ ! -e $work/blocked.cursor ]] || fail "cursor written after a failed append"

# The template unit passes the instance verbatim (%i), so a unit name with hyphens keeps them in the spool
# directory, the cursor file and the unit field. Render its ExecStart the way systemd does, with %h, %S and %i
# filled in, and run the program with those arguments against the fixture renamed to the hyphenated unit.
template=$here/../systemd/user/sparrow-spool@.service
instance=example-sync-nightly-job
grep -v '^#' "$template" | grep -q '%I' && fail "the template uses the unescaping specifier %I"
exec_line=$(sed -n 's/^ExecStart=//p' "$template")
[[ $exec_line == "%h/.local/libexec/sparrow/sparrow-unit-spool "* ]] || fail "the template does not run sparrow-unit-spool"
[[ $exec_line != *existing-spool* ]] || fail "the template runs another spool program"
grep -qx 'Environment="SPARROW_UNIT_SPOOL_OWN_LINES=keep"' "$template" || fail "the template does not keep the unit's own lines"
home=$work/home state=$work/home/.local/state
mkdir -p "$home/.local/libexec/sparrow"
printf 'program of an existing spool unit\n' > "$home/.local/libexec/sparrow/existing-spool"
cp -- "$home/.local/libexec/sparrow/existing-spool" "$work/existing.before"
rendered=${exec_line//%h/$home}; rendered=${rendered//%S/$state}; rendered=${rendered//%i/$instance}
read -r -a argv <<< "$rendered"
[[ ${#argv[@]} -eq 4 ]] || fail "the template's ExecStart does not have the program and three arguments"
[[ ${argv[1]} == "$instance.service" ]] || fail "the template did not pass the instance as the unit name"
sed "s/example-sync\.service/$instance.service/g" "$fixtures/journal.jsonl" > "$work/hyphen.jsonl"
JOURNAL_FIXTURE=$work/hyphen.jsonl bash "$script" "${argv[1]}" "${argv[2]}" "${argv[3]}"
[[ -d $state/sparrow/unit-spool/$instance.service && -f $state/sparrow/unit-spool/$instance.service.cursor ]] \
    || fail "the hyphenated instance did not keep its name in the spool directory and cursor file"
grep -q "\"unit\":\"$instance.service\"" "$state"/sparrow/unit-spool/"$instance".service/unit-spool-*.jsonl \
    || fail "the hyphenated instance's records do not carry its unit name"
cmp -s "$home/.local/libexec/sparrow/existing-spool" "$work/existing.before" || fail "the existing spool program changed"

# A failed append ends the spool. A systemd service ignores SIGPIPE by default, so the template restores the default
# (IgnoreSIGPIPE=no) and the test runs the program with the default disposition against a journalctl stand-in that
# never ends. The program must exit, not run on; the timeout would report 124.
[[ $(sed -n 's/^IgnoreSIGPIPE=//p' "$template") == no ]] || fail "the template does not restore SIGPIPE, so a failed append can stall"
endless=$work/endless
mkdir -p "$endless"
cat > "$endless/journalctl" <<'DOUBLE'
#!/usr/bin/env bash
while :; do cat -- "$JOURNAL_FIXTURE" || exit 1; done
DOUBLE
chmod +x "$endless/journalctl"
mkdir -p "$work/stalled" && mkdir "$work/stalled/unit-spool-20261127T1310.jsonl"
rc=0
PATH=$endless:$PATH env --default-signal=PIPE timeout 30 bash "$script" "$unit" "$work/stalled" "$work/stalled.cursor" >/dev/null 2>&1 || rc=$?
[[ $rc -ne 124 && $rc -ne 0 ]] || fail "a failed append did not end the spool (exit $rc)"

# A late line never joins a bucket older than the newest one in the spool. With a newer bucket already present,
# every kept record joins that bucket after its earlier line, and an older bucket that already exists is left unchanged.
late=$work/late
mkdir -p "$late"
printf '%s\n' '{"message":"shipped earlier"}' > "$late/unit-spool-20261127T1310.jsonl"
cp -- "$late/unit-spool-20261127T1310.jsonl" "$work/late-old.before"
printf '%s\n' '{"message":"earlier"}' > "$late/unit-spool-20261127T1400.jsonl"
SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$late" "$work/late.cursor"
cmp -s "$late/unit-spool-20261127T1310.jsonl" "$work/late-old.before" \
    || fail "an older bucket that already existed was appended to"
{ printf '%s\n' '{"message":"earlier"}'; cat -- "$fixtures/keep.jsonl"; } > "$work/late.expected"
cmp -s "$work/late.expected" "$late/unit-spool-20261127T1400.jsonl" \
    || fail "late records did not join the newest bucket after its earlier line"
[[ $(ls -1 -- "$late" | wc -l) -eq 2 ]] || fail "the spool created a bucket other than the newest"

# Closing, on a fake clock. A newest file unwritten for 599 seconds is still open: a late line joins it.
base=$(date -u -d '2026-11-27 14:00:00' +%s)
open=$work/open
mkdir -p "$open"
printf '%s\n' '{"message":"earlier"}' > "$open/unit-spool-20261127T1400.jsonl"
touch -d "@$base" "$open/unit-spool-20261127T1400.jsonl"
SPARROW_UNIT_SPOOL_NOW=$((base + 599)) SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$open" "$work/open.cursor"
{ printf '%s\n' '{"message":"earlier"}'; cat -- "$fixtures/keep.jsonl"; } > "$work/open.expected"
cmp -s "$work/open.expected" "$open/unit-spool-20261127T1400.jsonl" || fail "an open bucket (599 seconds quiet) did not take the late lines"
[[ $(ls -1 -- "$open" | wc -l) -eq 1 ]] || fail "an open bucket gained a file"

# At 601 seconds the same bucket is closed: its bytes stay, and the late lines start the current bucket (14:10).
closed=$work/closed
mkdir -p "$closed"
printf '%s\n' '{"message":"earlier"}' > "$closed/unit-spool-20261127T1400.jsonl"
touch -d "@$base" "$closed/unit-spool-20261127T1400.jsonl"
cp -- "$closed/unit-spool-20261127T1400.jsonl" "$work/closed-old.before"
SPARROW_UNIT_SPOOL_NOW=$((base + 601)) SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$closed" "$work/closed.cursor"
cmp -s "$closed/unit-spool-20261127T1400.jsonl" "$work/closed-old.before" || fail "a closed bucket (601 seconds quiet) was appended to"
cmp -s "$closed/unit-spool-20261127T1410.jsonl" "$fixtures/keep.jsonl" || fail "late lines after a closed bucket did not start the current bucket"
[[ $(ls -1 -- "$closed" | wc -l) -eq 2 ]] || fail "a closed bucket produced more than one new file"

# A clock behind the newest bucket: the closed file is 2099-12-31 23:50, the clock says 14:10 on 2026-11-27,
# so the late lines start the bucket after it (2100-01-01 00:00), and the closed file keeps its bytes.
behind=$work/behind
mkdir -p "$behind"
printf '%s\n' '{"message":"earlier"}' > "$behind/unit-spool-20991231T2350.jsonl"
touch -d "@$base" "$behind/unit-spool-20991231T2350.jsonl"
cp -- "$behind/unit-spool-20991231T2350.jsonl" "$work/behind-old.before"
SPARROW_UNIT_SPOOL_NOW=$((base + 601)) SPARROW_UNIT_SPOOL_OWN_LINES=keep bash "$script" "$unit" "$behind" "$work/behind.cursor"
cmp -s "$behind/unit-spool-20991231T2350.jsonl" "$work/behind-old.before" || fail "a closed bucket was appended to with a clock behind it"
cmp -s "$behind/unit-spool-21000101T0000.jsonl" "$fixtures/keep.jsonl" || fail "a clock behind the newest bucket did not start the bucket after it"

# A PRIORITY that is not a number keeps its record: the message is spooled and the priority is left out.
printf '%s\n' '{"__CURSOR":"s=fixture;i=1","__REALTIME_TIMESTAMP":"1795785000000000","MESSAGE":"priority is not a number","PRIORITY":"high","_SYSTEMD_UNIT":"example-sync.service","_COMM":"example-sync"}' > "$work/badprio.jsonl"
SPARROW_UNIT_SPOOL_OWN_LINES=keep JOURNAL_FIXTURE=$work/badprio.jsonl bash "$script" "$unit" "$work/badprio" "$work/badprio.cursor"
grep -q '"message":"priority is not a number"' "$work/badprio"/unit-spool-*.jsonl || fail "a record with a bad priority was dropped"
grep -q '"priority"' "$work/badprio"/unit-spool-*.jsonl && fail "a bad priority was written as a field"
[[ $(cat -- "$work/badprio.cursor") == 's=fixture;i=1' ]] || fail "the cursor did not move past the kept record"

# Bad settings refuse to start.
if SPARROW_UNIT_SPOOL_OWN_LINES=maybe bash "$script" "$unit" "$work/x" "$work/x.cursor" 2>/dev/null; then
    fail "an unknown OWN_LINES value was accepted"
fi

printf 'PASS\n'
