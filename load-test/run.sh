#!/usr/bin/env bash
# run.sh — reproducible load-test runner.
#
# Usage:
#   PROFILE=smoke    ./run.sh
#   PROFILE=baseline ./run.sh
#   PROFILE=load     ./run.sh
#   PROFILE=stress CONFIRM_STRESS=true ./run.sh   # opt-in only
#   PROFILE=soak   CONFIRM_SOAK=true   ./run.sh   # opt-in only
#
# Writes load-test/results/<run-id>-<profile>/ containing:
#   run-metadata.json, summary.json, k6-summary-export.json, raw.json,
#   console.log, report.md
# Results are git-ignored (see results/.gitignore). Exit code = k6 exit code.
set -euo pipefail
cd "$(dirname "$0")"

PROFILE="${PROFILE:-smoke}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT_DIR="results/${RUN_ID}-${PROFILE}"
mkdir -p "$OUT_DIR"
export RUN_ID PROFILE BASE_URL
export SUMMARY_JSON="${OUT_DIR}/summary.json"

# --- prerequisites -----------------------------------------------------------
command -v k6 >/dev/null 2>&1 || { echo "ERROR: k6 not found in PATH. Install Grafana k6: https://grafana.com/docs/k6/latest/set-up/install-k6/" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is required for metadata/report rendering." >&2; exit 2; }
if command -v curl >/dev/null 2>&1; then
  if ! curl -sf -m 5 "${BASE_URL}/actuator/health" >/dev/null; then
    echo "ERROR: ${BASE_URL}/actuator/health is not reachable/healthy. Start the local stack first (see README.md). Refusing to run." >&2
    exit 2
  fi
fi

# --- run metadata (non-secret only) -------------------------------------------
python3 - "$OUT_DIR/run-metadata.json" <<'PYEOF'
import json, os, platform, subprocess, sys, datetime

def run(cmd):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return (p.stdout.strip() or p.stderr.strip())  # java -version writes to stderr
    except Exception:
        return ""

repo = os.path.dirname(os.getcwd())
git = lambda *a: run(["git", "-C", repo, *a])
whitelisted_cfg = [k for k in sorted(os.environ)
                   if k.startswith(("LOAD_", "STRESS_", "SOAK_", "MAX_", "POLL_", "COMPLETION_", "ERROR_", "P95_", "ORDER_", "THINK_", "HTTP_", "CONFIRM_"))
                   and "PASSWORD" not in k and "TOKEN" not in k and "SECRET" not in k]
meta = {
    "run_id": os.environ.get("RUN_ID"),
    "profile": os.environ.get("PROFILE"),
    "base_url": os.environ.get("BASE_URL"),
    "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "git_commit": git("rev-parse", "HEAD"),
    "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
    "git_dirty": bool(git("status", "--porcelain")),
    "k6_version": run(["k6", "version"]),
    "java_version_host": (run(["java", "-version"]) or run(["java", "-version"])),
    "docker_version": run(["docker", "--version"]),
    "docker_compose_version": run(["docker", "compose", "version"]),
    "docker_ps": run(["docker", "ps", "--format", "{{.Image}} {{.Names}} {{.Status}}"]),
    "os": platform.platform(),
    "cpu_cores": os.cpu_count(),
    "ram_mb_total": (lambda: round(int([l for l in open("/proc/meminfo") if l.startswith("MemTotal")][0].split()[1]) / 1024) if os.path.exists("/proc/meminfo") else None)(),
    "load_generator_shared_host": os.environ.get(
        "LOAD_GENERATOR_SHARED_HOST",
        "undeclared - ASSUME shared with SUT unless you set LOAD_GENERATOR_SHARED_HOST"),
    "config_env_whitelisted": {k: os.environ[k] for k in whitelisted_cfg},
    "dataset": {
        "product_prefix": os.environ.get("LOADTEST_PRODUCT_PREFIX", "LOADTEST-"),
        "note": "product/stock/user counts are recorded in the k6 console.log setup lines and summary.json",
    },
}
with open(sys.argv[1], "w") as f:
    json.dump(meta, f, indent=2)
print(f"metadata -> {sys.argv[1]}")
PYEOF

# --- execute k6 ----------------------------------------------------------------
set +e
k6 run --include-system-env-vars \
  --out "json=${OUT_DIR}/raw.json" \
  --summary-export "${OUT_DIR}/k6-summary-export.json" \
  order-load-test.js 2>&1 | tee "${OUT_DIR}/console.log"
K6_EXIT=${PIPESTATUS[0]}
set -e
echo "k6 exit code: ${K6_EXIT}" | tee -a "${OUT_DIR}/console.log"

# --- render human-readable report ----------------------------------------------
if [ -f "$SUMMARY_JSON" ]; then
  python3 - "$OUT_DIR" <<'PYEOF'
import json, sys
out = sys.argv[1]
s = json.load(open(f"{out}/summary.json"))
m = json.load(open(f"{out}/run-metadata.json"))
tpl = open("report-template.md").read()
h, oc, cp = s["http"], s["order_creation"], s["order_completion"]
vals = {
    "RUN_ID": s["run_id"], "PROFILE": s["profile"], "TIMESTAMP": m["timestamp_utc"],
    "GIT_COMMIT": m["git_commit"], "GIT_BRANCH": m["git_branch"], "GIT_DIRTY": str(m["git_dirty"]),
    "K6_VERSION": m["k6_version"], "JAVA_VERSION": (m["java_version_host"] or "").split("\n")[0],
    "DOCKER_VERSION": m["docker_version"], "OS": m["os"], "CPU_CORES": str(m["cpu_cores"]),
    "RAM_MB": str(m["ram_mb_total"]), "SHARED_HOST": str(m["load_generator_shared_host"]),
    "BASE_URL": s["started_from_env"]["base_url"],
    "PEAK_VUS": str(s["started_from_env"]["peak_vus_configured"]),
    "PLANNED_DURATION_S": str(s["started_from_env"]["planned_duration_s"]),
    "ACTUAL_DURATION_S": f"{s['test_run_duration_s']:.1f}",
    "HTTP_REQUESTS": str(h["requests"].get("count", 0)),
    "HTTP_RPS": f"{h['requests'].get('rate', 0):.2f}",
    "HTTP_ERROR_RATE_PCT": f"{h['error_rate_custom'] * 100:.2f}",
    "HTTP_2XX": str(h["status_2xx"]), "HTTP_4XX": str(h["status_4xx"]),
    "HTTP_5XX": str(h["status_5xx"]), "HTTP_TIMEOUTS": str(h["timeouts"]),
    "CHECKS": str(s["checks"]),
    "CREATE_P50": f"{oc['latency']['p50_ms']:.1f}", "CREATE_P95": f"{oc['latency']['p95_ms']:.1f}",
    "CREATE_P99": f"{oc['latency']['p99_ms']:.1f}",
    "ORDERS_ACCEPTED": str(oc["accepted"]), "ORDERS_ACCEPTED_PS": f"{oc['accepted_per_s']:.2f}",
    "ORDERS_COMPLETED": str(cp["completed"]), "ORDERS_FAILED": str(cp["failed_terminal"]),
    "ORDERS_PENDING": str(cp["pending_at_timeout"]),
    "COMPLETION_RATE_PCT": f"{cp['completion_rate'] * 100:.1f}",
    "TERMINAL_RATE_PCT": f"{cp['terminal_rate'] * 100:.1f}",
    "E2E_P50": f"{cp['e2e_latency']['p50_ms']:.0f}", "E2E_P95": f"{cp['e2e_latency']['p95_ms']:.0f}",
    "E2E_P99": f"{cp['e2e_latency']['p99_ms']:.0f}",
    "POLLED": str(cp["polled"]),
}
for k, v in vals.items():
    tpl = tpl.replace("{{" + k + "}}", v)
open(f"{out}/report.md", "w").write(tpl)
print(f"report -> {out}/report.md")
PYEOF
else
  echo "WARNING: $SUMMARY_JSON missing (k6 aborted before summary) — see console.log" >&2
fi

echo "Results in ${OUT_DIR}/ (git-ignored). Now run the read-only DB verification:"
echo "  psql ... -f scripts/verify-consistency.sql   # see README.md"
exit "$K6_EXIT"
