#!/usr/bin/env bash
# Slice-1 kill test as a standalone, unattended check (BUILD-BRIEF
# criterion 3). The same procedure is also an integration test
# (tests/slice1.rs::kill_patch_and_runtime_then_resume_loses_nothing);
# this script exists to produce a human-readable transcript with the
# intermediate state visible.
#
# What it does:
#   1. run the two-patch relay scenario twice: once clean, once paced
#      so the kill lands mid-run;
#   2. SIGKILL a patch child mid-run, then SIGKILL the runtime itself;
#   3. restart the runtime on the same run dir and let it resume;
#   4. check: verify passes (nothing lost, everything re-derives),
#      both patch logs keep their pre-kill bytes as a prefix, the
#      pre-kill stream stands unchanged, and the resumed stream is
#      playback-identical to the clean run, event for event.
#
# Exit 0 = pass, 1 = fail. No arguments; works from the repo root.

set -u
repo="$(cd "$(dirname "$0")/.." && pwd)"
bin="$repo/target/debug/continuum-runtime"
work="$(mktemp -d /tmp/cr-kill-check.XXXXXX)"
fail=0

step() { printf '\n== %s ==\n' "$1"; }
check() { # check <label> <command...>
    local label="$1"; shift
    if "$@"; then
        echo "PASS  $label"
    else
        echo "FAIL  $label"
        fail=1
    fi
}

if [[ ! -x "$bin" ]]; then
    echo "binary not found at $bin — run cargo build first" >&2
    exit 2
fi

mkdir -p "$work/manifests" "$work/clean" "$work/kill"
cat > "$work/manifests/alpha.json" <<'EOF'
{"id":"alpha","role":"patch","triggers":[{"sender":"ingest","intent":"ping"},{"sender":"beta","intent":"relay"}],"policy":{"accept_senders":["*"]},"emit_intent":"relay","cost":2}
EOF
cat > "$work/manifests/beta.json" <<'EOF'
{"id":"beta","role":"patch","triggers":[{"sender":"alpha","intent":"relay"}],"policy":{"accept_senders":["*"]},"emit_intent":"relay","cost":3}
EOF
python3 - "$work/input.ctev" <<'EOF'
import sys
with open(sys.argv[1], "w") as f:
    for i in range(60):
        f.write(f'[{i}, "ingest", "ping", 0.5]\n')
EOF

step "clean reference run"
"$bin" run --run-dir "$work/clean" --input "$work/input.ctev" --manifest-dir "$work/manifests"

step "paced run, killed mid-flight"
"$bin" run --run-dir "$work/kill" --input "$work/input.ctev" --manifest-dir "$work/manifests" --step-delay-ms 8 &
rt=$!
lines=0
for _ in $(seq 1 500); do
    lines=$(grep -c '' "$work/kill/stream.jsonl" 2>/dev/null || echo 0)
    [[ "$lines" -ge 40 ]] && break
    sleep 0.02
done
alpha_pid=$(cat "$work/kill/patches/alpha/pid")
echo "stream at kill: $lines records; alpha pid $alpha_pid; runtime pid $rt"
kill -9 "$alpha_pid"
kill -9 $rt
wait $rt 2>/dev/null
cp "$work/kill/stream.jsonl" "$work/stream-before.jsonl"
cp "$work/kill/patches/alpha/active-log.jsonl" "$work/alpha-before.jsonl"
cp "$work/kill/patches/beta/active-log.jsonl" "$work/beta-before.jsonl"
echo "snapshotted: $(grep -c '' "$work/stream-before.jsonl") stream records, $(grep -c '' "$work/alpha-before.jsonl") alpha log entries, $(grep -c '' "$work/beta-before.jsonl") beta log entries"

step "restart the runtime on the same run dir"
"$bin" run --run-dir "$work/kill" --input "$work/input.ctev" --manifest-dir "$work/manifests"

step "checks"
check "verify: nothing lost, everything re-derives" \
    "$bin" verify --run-dir "$work/kill" --input "$work/input.ctev"
check "alpha log: pre-kill entries are a byte prefix of the post-restart log" \
    cmp -s "$work/alpha-before.jsonl" <(head -c "$(stat -c%s "$work/alpha-before.jsonl")" "$work/kill/patches/alpha/active-log.jsonl")
check "beta log: pre-kill entries are a byte prefix of the post-restart log" \
        cmp -s "$work/beta-before.jsonl" <(head -c "$(stat -c%s "$work/beta-before.jsonl")" "$work/kill/patches/beta/active-log.jsonl")
check "stream: pre-kill records stand unchanged" \
        cmp -s "$work/stream-before.jsonl" <(head -c "$(stat -c%s "$work/stream-before.jsonl")" "$work/kill/stream.jsonl")
check "playback: resumed stream is event-for-event identical to the clean run" \
        "$bin" playback --a "$work/kill" --b "$work/clean"

step "result"
if [[ "$fail" -eq 0 ]]; then
    echo "KILL CHECK: PASS"
else
    echo "KILL CHECK: FAIL (transcript above; artifacts in $work)"
fi
exit "$fail"
