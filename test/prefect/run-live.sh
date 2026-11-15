#!/usr/bin/env bash
# Run the SQL suite's data queries against a REAL Prefect server instead of the replay.
#
#   PREFECT_LIVE=http://127.0.0.1:<port> test/prefect/run-live.sh
#
# The server must be the one record.py snapshotted (seeded by seed.py and started with
# PREFECT_SERVER_API_AUTH_STRING=admin:prefect-fixture-pw), so the expected results hold.
# test/sql/prefect.test runs as is; from test/sql/pushdown.test every query runs except the
# assertions on the replay's request log. This proves that real Prefect accepts every filter
# body the worker sends and answers them the way the replay does.
set -euo pipefail
: "${PREFECT_LIVE:?base URL of the live server, without /api}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/test/sql"
python3 - "$REPO" "$STAGE" <<'PY'
import sys
repo, stage = sys.argv[1], sys.argv[2]
def load_vgi(text: str) -> str:
    return text.replace("\nrequire vgi\n", "\nstatement ok\nLOAD vgi;\n")
open(f"{stage}/test/sql/prefect.test", "w").write(load_vgi(open(f"{repo}/test/sql/prefect.test").read()))
blocks = open(f"{repo}/test/sql/pushdown.test").read().split("\n\n")
keep = [b for b in blocks if "reqs(" not in b and "__requests" not in b and "CREATE MACRO" not in b]
open(f"{stage}/test/sql/pushdown_live.test", "w").write(load_vgi("\n\n".join(keep)))
PY
echo '{"skip_error_messages": ["__vgi_prefect_never_matches__"]}' > "$STAGE/cfg.json"
cd "$STAGE"
PREFECT_WORKER="$REPO/.venv/bin/vgi-prefect-worker" PREFECT_MOCK="$PREFECT_LIVE" \
  "${HAYBARN_UNITTEST:-$REPO/.tools/bin/haybarn-unittest}" --keep-home --test-dir "$STAGE" \
  --test-config "$STAGE/cfg.json" "test/sql/*"
