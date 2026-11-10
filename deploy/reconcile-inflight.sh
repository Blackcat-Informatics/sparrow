#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc. <paudley@blackcatinformatics.ca>
# SPDX-License-Identifier: MIT OR Apache-2.0 OR MulanPSL-2.0
#
# Reconcile consume claims after a rollback restores an older store (docs/deployment.md,
# "Claims after a rollback"). The restored store does not know the claims the newer daemon
# made after the backup, and it still holds claims the newer daemon finished after it. Both
# findings come from the schema 69 to 68 rollback of 2026-11-07 (#145). The newer store, set
# aside by the rollback, answers what each claim was. Bytes are never discarded: an entry
# goes back to its root under its original name by a link and an unlink, and a finished
# claim's bytes are read back from the newer store and checked against their catalog digest
# before re-shipping.
set -euo pipefail
umask 077
# shellcheck source-path=SCRIPTDIR source=lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

usage() {
    cat <<'USAGE'
Usage: deploy/reconcile-inflight.sh inflight --restored CLI SOCKET --newer CLI SOCKET [--private-intake DIR]
       deploy/reconcile-inflight.sh missing --restored CLI SOCKET --newer CLI SOCKET [--private-intake DIR]

RESTORED is a daemon serving the restored store and NEWER one serving the newer store the
rollback set aside, each with its own CLI image (a CLI must match its daemon's schema).
Open the newer store query-only on a scratch configuration; never open it with its watches.

inflight  Run before the restored daemon starts with its watches; RESTORED is then a
          query-only daemon on the restored store. For each consume root in RESTORED's
          status, every entry of ROOT/.sparrow-inflight/ that RESTORED's ingest-get does not
          know is moved back to ROOT under the original name NEWER's ingest-get records for
          it: a hard link under that name, then the entry is unlinked, so the inode and bytes
          are unchanged and no file is ever overwritten. An entry neither store knows, or
          whose original name is taken, stops the run with nothing more moved.
missing   Run once the restored daemon serves with its watches. For each watch whose failure
          names a claim with "unpublished claimed source is missing", NEWER must record that
          claim as published: its artifact's bytes are read from NEWER, checked against the
          artifact's payload digest, and placed in the root as NAME.reshipped.EXT (rename
          from a temporary file beside the root), and the claim is retired with
          "watch retire-claim". Each failing claim names itself at its next retry (at most
          15 minutes apart), so run it again until no watch reports that failure.

--private-intake DIR
                  reconcile the private roots a `sparrowd --private-intake DIR` daemon consumes:
                  each watch's root is DIR/NAME, NAME its configured root's last component, as
                  the daemon rebinds it, and no other root is touched (the measurement's
                  rehearsal uses it on scratch copies)

Exit 0 when done, 1 on a refusal or failure, 2 on a usage error, 3 when the bytes were
re-shipped but the restored image has no "watch retire-claim" (schema 69 and older): the
claim stays pending until a schema 70 image retires it.
USAGE
}

(($# >= 1)) || { usage >&2; exit 2; }
case $1 in
    -h | --help) usage; exit 0 ;;
    inflight | missing) PHASE=$1; shift ;;
    *) usage >&2; exit 2 ;;
esac
RESTORED_CLI='' RESTORED_SOCKET='' NEWER_CLI='' NEWER_SOCKET='' PRIVATE_INTAKE=''
while (($#)); do
    case $1 in
        --restored) (($# >= 3)) || { usage >&2; exit 2; }; RESTORED_CLI=$2 RESTORED_SOCKET=$3; shift 3 ;;
        --newer) (($# >= 3)) || { usage >&2; exit 2; }; NEWER_CLI=$2 NEWER_SOCKET=$3; shift 3 ;;
        --private-intake) (($# >= 2)) || { usage >&2; exit 2; }; PRIVATE_INTAKE=$(realpath -e -- "$2") || exit 2; shift 2 ;;
        *) usage >&2; exit 2 ;;
    esac
done
[[ -n $RESTORED_CLI && -n $NEWER_CLI ]] || { usage >&2; exit 2; }
[[ -x $RESTORED_CLI && -x $NEWER_CLI ]] || die "both CLI images must be executable"

restored_status=$("$RESTORED_CLI" "$RESTORED_SOCKET" status) || die "the restored daemon did not answer status"
newer_status=$("$NEWER_CLI" "$NEWER_SOCKET" status) || die "the newer daemon did not answer status"
store=$(json_field store_id <<<"$restored_status")
[[ $(json_field store_id <<<"$newer_status") == "$store" ]] \
    || die "the restored and newer daemons serve different stores"
# One "WATCH<TAB>ROOT<TAB>FAILURE" line per consume watch the restored store holds.
mapfile -t watches < <(python3 -I -c 'import json,sys
for w in json.load(sys.stdin)["consume_watches"]:
    print("%s\t%s\t%s" % (w["id"], w["root"], (w["failure"] or "").replace("\t", " ").replace("\n", " ")))' <<<"$restored_status")

# The root a watch consumes: as its status names it, or, with --private-intake, the private
# root the daemon rebinds it to.
watch_root() {
    if [[ -z $PRIVATE_INTAKE ]]; then
        printf '%s\n' "$1"
    else
        printf '%s/%s\n' "$PRIVATE_INTAKE" "$(basename -- "$1")"
    fi
}

# ingest-get IMPORT on a daemon; prints the JSON, or nothing when the store does not know it.
# The CLI reports a daemon refusal on stderr as its typed error, `AgentError { code: "CODE",
# ...`, and ingest-get's only NOT_FOUND is an unknown or expired ingestion, so only that code
# field means unknown; any other failure stops the run.
ingest_get() {
    local cli=$1 socket=$2 import=$3 out err
    err=$(mktemp) || exit 1
    if out=$("$cli" "$socket" ingest-get "$store" "$import" 2>"$err"); then
        rm -f -- "$err"
        printf '%s\n' "$out"
        return 0
    fi
    if grep -q 'AgentError { code: "NOT_FOUND",' -- "$err"; then
        rm -f -- "$err"
        return 0
    fi
    die "ingest-get $import failed: $(cat -- "$err")"
}

# The original relative path an ingest-get document records, decoded from hex.
original_name() {
    python3 -I -c 'import json,sys; o=json.load(sys.stdin)["ownership"]
if o is None: sys.exit(1)
sys.stdout.buffer.write(bytes.fromhex(o["relative_path_hex"]))'
}

if [[ $PHASE == inflight ]]; then
    moved=0 kept=0
    for row in "${watches[@]}"; do
        IFS=$'\t' read -r watch root _ <<<"$row"
        root=$(watch_root "$root")
        inflight=$root/.sparrow-inflight
        [[ -d $inflight ]] || continue
        for entry in "$inflight"/* "$inflight"/.[!.]*; do
            [[ -e $entry ]] || continue
            name=${entry##*/}
            [[ $name != .lock ]] || continue
            known=$(ingest_get "$RESTORED_CLI" "$RESTORED_SOCKET" "$name")
            if [[ -n $known ]]; then
                note "keep $watch/$name: the restored store holds it ($(json_field deletion <<<"$known"))"
                kept=$((kept + 1))
                continue
            fi
            newer=$(ingest_get "$NEWER_CLI" "$NEWER_SOCKET" "$name")
            [[ -n $newer ]] || die "$entry is known to neither store; inspect it by hand and keep its bytes"
            relative=$(original_name <<<"$newer") || die "the newer store records no original name for $name"
            target=$root/$relative
            [[ -d ${target%/*} ]] || die "the original directory of $target is missing"
            # link then unlink: the same inode under its original name, never over a file.
            ln -- "$entry" "$target" || die "cannot restore $entry to $target (it may exist already)"
            rm -- "$entry"
            sync -f -- "$root"
            note "moved $watch/$name back to $target"
            moved=$((moved + 1))
        done
    done
    note "inflight moved=$moved kept=$kept"
    exit 0
fi

# Whether the restored CLI has "watch retire-claim" (schema 70 and newer): its watch usage,
# which it reports for a bare "watch", names every administration command it has.
watch_usage=$("$RESTORED_CLI" "$RESTORED_SOCKET" watch 2>&1 >/dev/null || true)
retire=0
[[ $watch_usage == *"watch retire-claim"* ]] && retire=1
reshipped=0 unretired=0
for row in "${watches[@]}"; do
    IFS=$'\t' read -r watch root failure <<<"$row"
    [[ $failure =~ claim\ (import:[0-9a-f]+):\ unpublished\ claimed\ source\ is\ missing ]] || continue
    claim=${BASH_REMATCH[1]}
    root=$(watch_root "$root")
    newer=$(ingest_get "$NEWER_CLI" "$NEWER_SOCKET" "$claim")
    [[ -n $newer ]] || die "the newer store does not know $claim; its bytes must be restored from the producer before it is retired"
    read -r artifact payload length < <(python3 -I -c 'import json,sys; v=json.load(sys.stdin)
a=v["accepted"]
if a is None or v["deletion"] not in ("published","removed"): sys.exit(1)
print(a["artifact_id"], a["payload_id"], a["byte_length"])' <<<"$newer") \
        || die "the newer store did not publish $claim; nothing is retired"
    relative=$(original_name <<<"$newer") || die "the newer store records no original name for $claim"
    base=${relative##*/}
    if [[ $base == *.* ]]; then reship=${base%.*}.reshipped.${base##*.}; else reship=$base.reshipped; fi
    target=$root/${relative%"$base"}$reship
    [[ ! -e $target ]] || die "$target exists already"
    work=$(mktemp -d -- "$(dirname -- "$root")/.reconcile-XXXXXX") || exit 1
    for ((offset = 0; offset < length; offset += 1048576)); do
        size=$((length - offset < 1048576 ? length - offset : 1048576))
        "$NEWER_CLI" "$NEWER_SOCKET" bytes "$store" "$artifact" "$offset" "$size" "$work/chunk" >/dev/null \
            || die "reading $artifact at offset $offset from the newer store failed"
        cat -- "$work/chunk" >>"$work/bytes"
        rm -- "$work/chunk"
    done
    [[ -e $work/bytes ]] || : >"$work/bytes"
    digest=$(b3sum --no-names -- "$work/bytes") || exit 1
    [[ blake3:$digest == "$payload" ]] || die "$artifact reads back as blake3:$digest, not $payload; nothing re-shipped"
    chmod 0640 -- "$work/bytes"
    mv -T --no-clobber -- "$work/bytes" "$target"
    [[ ! -e $work/bytes ]] || die "$target appeared while re-shipping; the bytes stay in $work"
    rmdir -- "$work"
    note "re-shipped $artifact of $watch/$claim ($length bytes, $payload) as $target"
    reshipped=$((reshipped + 1))
    reason="published after the restored backup as $artifact by the newer store; its bytes were re-shipped as ${target##*/}"
    if ((retire == 0)); then
        note "$claim stays pending: the restored image has no \"watch retire-claim\" (its watch usage: $watch_usage)" >&2
        unretired=$((unretired + 1))
        continue
    fi
    err=$("$RESTORED_CLI" "$RESTORED_SOCKET" watch retire-claim "$watch" "$claim" "$reason" 2>&1 >/dev/null) \
        || die "retiring $claim failed: $err"
    note "retired $watch/$claim"
done
note "missing reshipped=$reshipped unretired=$unretired"
((unretired == 0)) || exit 3
