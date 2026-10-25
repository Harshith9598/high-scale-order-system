# Load Test Plan — status: NOT EXECUTED

No load test has been run against this system. Nothing in this repository's
documentation should be read as a measured throughput/latency claim. This plan
exists so the first person who *does* run one produces reproducible numbers.

## What to measure (and why)

| Scenario | Workload | Hypothesis / bottleneck to look for |
|---|---|---|
| A. Product browsing | `GET /api/products` (cached) and cache-bypassed variants (unique `search`), ramp 10→200 rps | Redis hit ratio; DB query cost on cache miss; product `Page` serialisation |
| B. Order creation (happy path) | Authenticated `POST /api/orders`, 1–3 line items, stock pre-seeded high | REST latency is only the first hop — measure **time-to-COMPLETED** separately (poll `GET /api/orders/{id}`): outbox poll interval (5s/hop) dominates end-to-end time |
| C. Contended stock | N concurrent orders for a product with stock = N/2 | Atomic `UPDATE` correctness under load: successful orders == stock, `quantity_available` never negative, losers end `FAILED` |
| D. Duplicate delivery | Republish 20% of `OrderCreated` events (reset outbox rows to `PENDING`) | `payments` per order stays 1; stock decremented once; consumer lag from dedupe lookups |
| E. Kafka outage | Stop broker 60s mid-run B, restart | Orders still accepted (outbox accumulates), zero lost orders after recovery, outbox `retry_count`/backlog drains, no `DEAD` rows below the 10-retry budget |
| F. Poison events | 1% events without `eventId` | They land on `.DLT` + `failed_events` without stalling their partitions beyond one backoff cycle |

## Environment to record with every result

- CPU/RAM, Docker resource limits per service, JDK version
- Topic partitions/replication, consumer concurrency (3), Hikari pool (20)
- Outbox `polling-interval-ms`, `batch-size`; payment `failure-rate` (set to
  `0` for B, default `0.05` otherwise) and `processing-delay-ms`
- Data sizes: products, users, pre-existing orders

## Metrics to capture

- Client side: RPS offered vs completed, error rate by class (4xx/5xx/timeout),
  p50/p95/p99 latency per endpoint
- App: `/actuator/prometheus` — `http_server_requests`, Hikari active
  connections, JVM GC, `orders_created_total`/`orders_completed_total`
- Kafka: consumer lag per group (`kafka-consumer-groups.sh --describe`),
  `.DLT` message counts
- DB: `outbox_events` backlog by status, `pg_stat_activity` waits
- Derived: order time-to-COMPLETED distribution; stock integrity check
  (`available + reserved + sold = initial`)

## Tooling

`performance-tests/PerformanceTest.java` covers basic endpoint timing
(RPS/avg/p95/p99). For scenarios C–F prefer k6 **plus** the SQL/Kafka checks
above; HTTP numbers alone cannot prove the event pipeline kept up.

## Smoke observation (not a benchmark)

2026-10-25 audit sandbox (2 GB RAM total, single instance, sequential client):
`GET /api/products?size=5` ×30 → min 21 ms, p50 28 ms, p95 63 ms. Recorded only
because it was actually measured; it says nothing about capacity.
