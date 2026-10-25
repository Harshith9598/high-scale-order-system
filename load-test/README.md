# Load Testing (Grafana k6)

Safe, repeatable load tests for the order platform. This suite implements
the happy-path scenario **B** of [`docs/load-test-plan.md`](../docs/load-test-plan.md);
scenarios A and C–F (browsing, contended stock, duplicates, outage, poison
events) remain future work. It supersedes the ad-hoc
[`performance-tests/PerformanceTest.java`](../performance-tests/PerformanceTest.java)
for order-flow measurements (that tool times a single URL and knows nothing
about asynchronous completion).

**Read this first — acceptance ≠ completion.** `POST /api/orders` returns
`201` after the order (and its outbox row) is committed. Inventory, payment,
and final status then happen asynchronously over Kafka. This suite reports
API acceptance and end-to-end completion (polled per order) as **separate**
metrics; never cite acceptance RPS as "orders completed per second".

## What it does (verified API surface)

| Step | Endpoint | Notes |
|---|---|---|
| Health gate | `GET /actuator/health` | public |
| Discover products | `GET /api/products?size=100` | public; suite uses only names prefixed `LOADTEST-` and aborts if none exist |
| Register synthetic users | `POST /api/auth/register` | one per peak VU, unique per run: `loadtest-<runId>-<n>` |
| Create order | `POST /api/orders` `{items:[{productId,quantity}]}` | Bearer JWT (USER), expects `201` + `status=CREATED` + correct `totalAmount` |
| Poll completion | `GET /api/orders/{id}` | owner-only; terminal states: `COMPLETED`, `FAILED`, `CANCELLED` |

API facts that shape this suite (from source inspection, 2026-10-25):

- **No client idempotency-key support** on `POST /api/orders` — every 201 is
  a new logical order, so creates are never retried. Idempotency exists
  internally (per `(eventId, consumerName)`) and is verified in SQL checks.
- **No admin can be provisioned via API** (registration always assigns
  `USER`; product/inventory writes need `ADMIN`; none is seeded). Synthetic
  products/stock are therefore seeded with guarded SQL (below). The 10
  catalog products seeded by `DataInitializer` (100 stock each) are never
  used for load by default.
- **No bulk status/reset endpoints** — completion is polled per order
  (bounded: interval × timeout), and there is no test-only reset API.
  Metrics endpoints other than health/info require `ADMIN`
  (`/actuator/prometheus` included), so this suite is client-side + SQL.
- Simulated payment failures: `app.payment.failure-rate` defaults to `0.05`
  (≈5% of orders legitimately end `FAILED`). The outbox publisher polls
  every 5s by default, which floors end-to-end latency. Both are
  configurable — see the override example — and must be recorded per run.

## Prerequisites

- Docker + Docker Compose v2, 4 GB+ free RAM recommended for the full stack
- [Grafana k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) on `PATH`
- `python3` and `curl` (metadata, report rendering, prerequisite check)

## Quick start (local stack)

```bash
# 1) Start the stack (app + PostgreSQL + Redis + Kafka) on localhost:8080
docker compose up -d --build
curl -sf http://localhost:8080/actuator/health

# 2) Seed synthetic products/stock into the LOCAL database (idempotent)
docker compose exec -T postgres psql -U orderuser -d orderdb \
  -v stock=100000 -f - < load-test/scripts/seed-load-data.sql

# 3) Smoke profile (1 VU, 30s), then baseline (5 VUs, 2m)
cd load-test
PROFILE=smoke    ./run.sh
PROFILE=baseline ./run.sh

# 4) Post-run consistency verification (read-only)
docker compose exec -T postgres psql -U orderuser -d orderdb \
  -v userprefix='loadtest' -v initial_stock=100000 \
  -f - < scripts/verify-consistency.sql

# 5) Optional cleanup of synthetic data (stop app first; local DB only)
docker compose stop app
docker compose exec -T postgres psql -U orderuser -d orderdb \
  -f - < scripts/cleanup-load-data.sql
docker compose down            # stop everything (add -v only to wipe volumes)
```

Deterministic-completion benchmark (optional): copy
`docker-compose.override.example.yml` to the repo root as
`docker-compose.override.yml` **before** `docker compose up` — it pins
`APP_PAYMENT_FAILURE_RATE=0` and `APP_OUTBOX_POLLING_INTERVAL_MS=500`.
Delete the override afterwards; never commit it.

## Profiles

| Profile | Shape | Opt-in |
|---|---|---|
| `smoke` | 1 VU × 30s | — |
| `baseline` | 5 VUs × 2m | — |
| `load` | ramp/hold 10 → 25 → 50 (`LOAD_RAMP_S`/`LOAD_HOLD_S`) | — |
| `stress` | steps of +10 VUs to `STRESS_MAX_VUS`, `STRESS_STEP_S` apart | `CONFIRM_STRESS=true` |
| `soak` | `SOAK_VUS` for `SOAK_DURATION` (15m–30m enforced) | `CONFIRM_SOAK=true` |

All knobs are environment variables — see [`config.example.env`](config.example.env).
`run.sh` passes them with k6's `--include-system-env-vars`; if you invoke
`k6 run order-load-test.js` directly, add that flag yourself (or `-e VAR=value`
per variable) or the script will silently use its defaults.
Stress/soak are **not** wired into CI and must never run unattended.

The workload is a **closed model**: a VU creates an order, (optionally) polls
it to completion, then repeats. With polling on, per-VU throughput includes
pipeline latency — that is intentional and why acceptance and completion are
reported separately. Set `POLL_COMPLETION=false` to measure pure API
acceptance, and state that you did.

## Output

Each run writes `load-test/results/<run-id>-<profile>/` (**git-ignored**):

- `run-metadata.json` — git commit/branch, k6/Java/Docker versions, OS/CPU/RAM,
  whitelisted config, whether the load generator shared the host
  (`LOAD_GENERATOR_SHARED_HOST`), dataset notes
- `summary.json` — machine-readable metrics (see below)
- `k6-summary-export.json`, `raw.json` — stock k6 outputs for diagnosis
- `console.log` — full k6 console incl. setup lines (product/stock/user counts; no tokens)
- `report.md` — human-readable report rendered from [`report-template.md`](report-template.md)

Metrics reported: total requests + RPS; per-class status counts (2xx/4xx/5xx);
HTTP error rate and timeouts; failed checks; create-order p50/p95/p99;
accepted orders/sec; completed/failed-terminal/cancelled/**pending** counts;
completion + terminal rates; end-to-end latency p50/p95/p99. Default
thresholds (configurable): HTTP error rate < `ERROR_RATE_MAX` (1%),
create p95 < `P95_MS_MAX` (5s), checks > 99%; `COMPLETION_RATE_MIN` is
report-only unless set. k6 exits non-zero on threshold breach, and `run.sh`
propagates that exit code.

Interpreting results honestly:

- Numbers describe **this machine and dataset only** — record them verbatim,
  never extrapolate to "production capacity".
- If k6 shares the host with the stack (normal locally), client and server
  compete for CPU; say so (`LOAD_GENERATOR_SHARED_HOST`).
- `pending_at_timeout` > 0 with draining consumers usually means the
  pipeline is slower than the deadline (outbox 5s/hop at defaults), not
  necessarily data loss — confirm with the SQL stuck-order check.
- ~5% `FAILED` at default settings is the simulated payment failure, not a
  defect. Deviations far from the configured failure-rate are findings.

## Safety controls

- **Local targets only by default** (`localhost`, `127.0.0.0/8`, `::1`,
  `host.docker.internal`). Non-local requires **both**
  `LOADTEST_ALLOW_NON_LOCAL=true` and the exact host in
  `LOADTEST_ALLOWED_HOSTS`. Hosts containing `prod` are always refused.
- Hard caps: run aborts if the profile exceeds `MAX_VUS` (default 60) or
  `MAX_DURATION_S` (default 1800) — raising them is an explicit act.
- Aborts before traffic if `/actuator/health` fails, if no `LOADTEST-`
  products exist, or if synthetic-user registration fails.
- Polling is bounded (`COMPLETION_TIMEOUT_S`, `POLL_INTERVAL_S`); there is no
  background/unbounded traffic. Users/products/orders are uniquely named per
  run; no order-create retries.
- No credentials, JWTs, or `Authorization` headers are ever logged; metadata
  env capture is a whitelist that excludes password/token/secret keys.
- Seeding/cleanup SQL refuses any database not named `orderdb` by default
  and touches only `LOADTEST-%` products / `loadtest-%` users. Cleanup is a
  manual, explicit step — the suite never deletes data.
- Generated results are git-ignored; never commit `results/` contents,
  tokens, or data from a non-synthetic source.

File map: `order-load-test.js` (suite), `run.sh` (runner + report),
`config.example.env`, `report-template.md`,
`scripts/{seed,verify-consistency,cleanup}-load-data.sql`,
`docker-compose.override.example.yml`.
