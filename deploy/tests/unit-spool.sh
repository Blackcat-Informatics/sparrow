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
cat -- "$JOURNAL_FIXTURE"
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

# Bad settings refuse to start.
if SPARROW_UNIT_SPOOL_OWN_LINES=maybe bash "$script" "$unit" "$work/x" "$work/x.cursor" 2>/dev/null; then
    fail "an unknown OWN_LINES value was accepted"
fi

printf 'PASS\n'
