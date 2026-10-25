# Interview Guide — High-Scale Event-Driven Order Processing System

Every answer below describes the code as it stands on `audit/fix-critical-issues`
(after the 2026-10-25 audit, see `engineering-audit.md`). File paths are given so
you can open the code while you explain. Items marked ⚠️ are things you must be
able to explain *honestly* — do not claim more than the code does.

## 30-second pitch

> "A Spring Boot order platform where creating an order writes the order and an
> outbox event in one PostgreSQL transaction. A poller publishes outbox rows to
> Kafka; inventory, payment, order-status and notification consumers advance the
> order through a state machine. Every consumer records an idempotency key in the
> same transaction as its business effect and emits its follow-up event through
> the outbox too, so at-least-once delivery doesn't double-charge or double-reserve.
> Failures retry with exponential backoff and land on dead-letter topics. I
> verified the full flow, overselling, duplicate delivery and the DLQ path by
> running the real stack, not just unit tests."

## Q&A

### 1. Why Kafka?
Durable, replayable log with per-key ordering (key = `orderId`, so one order's
events stay ordered per partition), consumer groups for fan-out (the payment
result feeds both `OrderStatusConsumer` and `NotificationConsumer` independently),
and decoupling so a slow payment step doesn't block order creation.
Trade-off to name: operational complexity, eventual consistency, and at-least-once
semantics you must design for. If the interviewer pushes: RabbitMQ would be simpler
for pure work queues, but we'd lose replay/log retention.
Code: `config/KafkaConfig.java`, topics in `application.yml`.

### 2. Why is PostgreSQL the source of truth (not Redis/Kafka)?
Orders, payments and stock need ACID + constraints (`CHECK (quantity >= 0)`,
unique idempotency keys, FK integrity). Redis is only a read cache for products/
inventory display (`config/RedisConfig.java`) — if Redis dies, `CacheErrorConfig`
falls back to the DB. Kafka is transport, not state: you can't query "current
stock" from a log cheaply, and retention/compaction isn't designed for that.

### 3. Why the transactional outbox? What breaks without it?
The dual-write problem: `INSERT order; kafka.send(...)` — crash between them and
either the order exists with no event (stuck forever) or the event exists with no
order. The outbox writes the event row *in the same DB transaction*
(`OrderService.createOrder` → `OutboxService.saveEvent`, `REQUIRED` propagation),
and `OutboxPublisher` relays rows to Kafka afterwards. Every state-changing hop
does this (inventory/payment/status consumers also write their next event via the
outbox — they originally called Kafka directly, which was the same bug one level
down; the audit fixed that).

### 4. What happens if the app crashes between DB commit and Kafka publish?
Nothing is lost: the outbox row is already committed as `PENDING`. On restart the
poller publishes it. Crash *after* the Kafka send but before marking `COMPLETED`?
The row's `PROCESSING` lease (`next_retry_at`) expires after 5 minutes and it's
republished **with the same `eventId`** (the id lives in the stored payload) —
consumers dedupe. This exact scenario was smoke-tested by resetting a row to
`PENDING`: stock and payments were unchanged. Code: `outbox/OutboxPublisher.java`.

### 5. How are duplicate events handled?
`idempotency_records` with `UNIQUE(event_id, consumer_name)`. Each consumer
claims the key with `saveAndFlush` *inside the same transaction* as its business
effect, so claim+effect commit atomically; a redelivery finds the key and returns
without side effects. Per-consumer scoping matters because of fan-out — the same
payment event must be processed once by the status consumer *and* once by the
notification consumer (V1 mistakenly made `event_id` globally unique; Flyway `V2`
fixed it). ⚠️ Say "effectively-once business effects", never "exactly-once".
Code: `service/IdempotencyService.java`, migration `V2__*.sql`.

### 6. Retries and DLQ — how do they work?
`DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries(3)` (1s→2s→4s),
explicitly wired into the listener factory. Non-retryable: deserialization
errors and events without an `eventId` (`IllegalArgumentException`). On
exhaustion, `DeadLetterPublishingRecoverer` publishes to `<topic>.DLT` with
`kafka_dlt-*` headers and we also persist a `failed_events` row (admin API:
`GET /api/failed-events`). Verified live: a hand-crafted event with no `eventId`
landed on `order.created.DLT` within seconds. ⚠️ Know the wart: in-memory retry
blocks the partition (head-of-line blocking) — preserves ordering, costs latency;
retry topics are the alternative. Code: `config/KafkaRetryConfig.java`.

### 7. How do you avoid overselling the last item?
One atomic conditional update:
`UPDATE inventory SET available = available - :q, reserved = reserved + :q WHERE product_id = :p AND available >= :q`
(`InventoryRepository.reserveInventory`). Under concurrency exactly the requests
that fit succeed; `0 rows updated` ⇒ insufficient. DB `CHECK` constraints back
it up. Insufficient stock returns `false` (order → `INVENTORY_FAILED` → `FAILED`);
it must not throw — an exception inside the consumer transaction marks it
rollback-only and destroys the failure handling (that was a real bug found in the
audit). Partial multi-line reservations compensate *only the lines that actually
reserved*. `ConcurrentOrderTest` covers 10 threads / 5 units.

### 8. Consistency across PostgreSQL, Kafka and Redis?
Postgres is authoritative; Kafka is at-least-once transport reconciled by
idempotency; Redis is a disposable cache (TTL + eviction on product/inventory
mutations — worst case a listing briefly shows stale availability, but an order
can never oversell because reservation checks the DB atomically, never the cache).
State is also guarded by the `OrderStatus` transition map — illegal transitions
throw `InvalidOrderStateException`.

### 9. Authentication & authorization?
BCrypt passwords, stateless JWT (HS256), `USER`/`ADMIN` roles, URL rules in
`SecurityConfig` plus `@PreAuthorize` on admin writes. Ownership checks in the
service layer: order reads/cancels and payment reads return **403** for other
users' data (the payment endpoint originally had no check — IDOR — found and
fixed in the audit). The JWT secret has no committed default; the app fails fast
if `JWT_SECRET` is missing or < 32 bytes. Actuator is locked down: only
health/info public, the rest ADMIN, `env`/`beans` not exposed at all.

### 10. How would you scale this?
- Consumers: more instances per group up to the partition count (3 today → raise
  partitions for hot topics; ordering is per `orderId` key).
- Outbox: claim rows with `SELECT … FOR UPDATE SKIP LOCKED`, shorten polling or
  switch to Debezium CDC.
- DB: read replicas for product browsing, connection-pool tuning, index on
  `(status, next_retry_at, created_at)` already supports the outbox scan.
- Kafka: replication factor > 1 (it's 1 here — single-broker local setup ⚠️).
- Split consumers into separate services only when team/scale demands it; today
  it's a modular monolith, which is the right default.

### 11. Known bottlenecks / what's *not* proven ⚠️
- No load test has been run (`docs/load-test-plan.md` is a plan, not results).
  Only a sequential smoke timing exists. **Never quote RPS numbers.**
- Outbox polling adds ~5s per hop (~15–20s order completion locally).
- Single Kafka broker, replication factor 1; single DB; payment is *simulated*
  with a configurable random failure rate.
- Testcontainers suites run in CI, not in the audit sandbox (no Docker there) —
  they were compile-checked; the manual stack run is the end-to-end evidence.
- The Testcontainers Kafka image/config and `docker build` are CI-verified, not
  sandbox-verified.

### 12. Trade-offs you should own
- Polling outbox vs CDC: simplicity/no extra infra vs latency.
- DB idempotency vs Redis: one extra write per event vs durability + same-tx claim.
- In-memory retry vs retry topics: ordering vs head-of-line blocking.
- Modular monolith vs microservices: one deployable, shared DB transactions
  (which make the outbox trivially correct) vs independent scaling.

### 13. What would you improve with more time? (prioritised)
1. `SELECT … FOR UPDATE SKIP LOCKED` outbox claiming + multi-instance tests.
2. Execute the load-test plan; tune partitions/pool/batch sizes from data.
3. Retry topics with delay levels instead of in-memory backoff.
4. Flyway-tested rollback/upgrade path + Testcontainers test for V1→V2 on a
   pre-populated DB (today V2 is verified on a clean DB).
5. `ErrorHandlingDeserializer` so malformed JSON (not just malformed events)
   goes to the DLT instead of failing at poll time.
6. Admin dead-letter *replay* endpoint; ShedLock if scheduling ever duplicates.
7. OpenTelemetry tracing across the Kafka hops (correlation IDs already flow).

## Red-flag phrases to avoid
- "Exactly-once processing" → say at-least-once + idempotent consumers.
- "Handles X thousand requests per second" → unmeasured; show the plan instead.
- "Microservices" → it's a modular monolith with event-driven internals; say why.
- "Kafka guarantees ordering" → only *within a partition*, keyed by `orderId`.
