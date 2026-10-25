# Load Test Report — {{PROFILE}} (run {{RUN_ID}})

> Benchmarks describe THIS run on THIS hardware only. They are capacity
> observations, not production guarantees. `HTTP 201` means the API accepted
> an order; completion is asynchronous and reported separately below.

## Environment (recorded at run start)

| Item | Value |
|---|---|
| Timestamp (UTC) | {{TIMESTAMP}} |
| Git commit / branch (dirty) | `{{GIT_COMMIT}}` / `{{GIT_BRANCH}}` ({{GIT_DIRTY}}) |
| k6 | {{K6_VERSION}} |
| Java (host) | {{JAVA_VERSION}} |
| Docker | {{DOCKER_VERSION}} |
| OS / CPU cores / RAM | {{OS}} / {{CPU_CORES}} / {{RAM_MB}} MB |
| Target | {{BASE_URL}} |
| Load generator shares host with SUT | {{SHARED_HOST}} |
| Profile, peak VUs, planned vs actual duration | {{PROFILE}}, {{PEAK_VUS}} VUs, {{PLANNED_DURATION_S}}s vs {{ACTUAL_DURATION_S}}s |
| Completion polling enabled | {{POLLED}} |

Also record (fill in): Docker service versions (`postgres:15-alpine`,
`redis:7-alpine`, `apache/kafka-native:3.8.0`, app image), topic partitions,
outbox polling interval, payment failure-rate, dataset size (seeded products
and stock, synthetic users — see console.log setup lines).

## API acceptance (synchronous HTTP)

| Metric | Value |
|---|---|
| Total HTTP requests (rate) | {{HTTP_REQUESTS}} ({{HTTP_RPS}}/s) |
| Status 2xx / 4xx / 5xx | {{HTTP_2XX}} / {{HTTP_4XX}} / {{HTTP_5XX}} |
| HTTP error rate / timeouts | {{HTTP_ERROR_RATE_PCT}}% / {{HTTP_TIMEOUTS}} |
| Checks (k6) | {{CHECKS}} |
| Create-order latency p50 / p95 / p99 (ms) | {{CREATE_P50}} / {{CREATE_P95}} / {{CREATE_P99}} |
| Orders accepted (HTTP 201) rate | {{ORDERS_ACCEPTED}} ({{ORDERS_ACCEPTED_PS}}/s) |

## Asynchronous completion (polled to terminal state)

| Metric | Value |
|---|---|
| Completed / failed-terminal / pending-at-timeout | {{ORDERS_COMPLETED}} / {{ORDERS_FAILED}} / {{ORDERS_PENDING}} |
| Completion rate / terminal rate | {{COMPLETION_RATE_PCT}}% / {{TERMINAL_RATE_PCT}}% |
| End-to-end latency p50 / p95 / p99 (ms) | {{E2E_P50}} / {{E2E_P95}} / {{E2E_P99}} |

Notes: at the default `app.payment.failure-rate=0.05`, ~5% of orders end
`FAILED` by design (simulated payment failure). End-to-end latency has a
floor set by the outbox polling interval (default 5s per hop).

## Post-run consistency verification (paste output of scripts/verify-consistency.sql)

```
(status counts, stuck orders, payment cardinality, stock reconciliation, duplicates)
```

Result: PASS / FAIL + anomalies:

## Interpretation & anomalies

- Bottlenecks observed (client CPU? DB? Kafka? outbox polling?) and evidence:
- Anything that would not reproduce elsewhere (shared host, thermal, other load):
- Follow-ups:

Raw artifacts: `raw.json` (full k6 output), `summary.json`,
`k6-summary-export.json`, `console.log`, `run-metadata.json` in this
directory (not committed to git).
