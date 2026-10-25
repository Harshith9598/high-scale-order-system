# Engineering Audit — 2026-10-25

Branch: `audit/fix-critical-issues` (based on `main` `bfa95cf`). **Not pushed to `main`.**
Baseline verified before changes: `mvn test -Dtest=OrderStatusTest,ProductServiceTest,IdempotencyServiceTest` → **9/9 pass**; GitHub CI on `main` → success. Docker/Testcontainers tests were *not* runnable in the audit sandbox (no Docker daemon).

Severity: 🔴 critical (wrong/loss/security) · 🟠 high · 🟡 medium · ⚪ low/cleanup

## Confirmed findings (evidence in code, not README)

| # | Sev | Finding | Evidence |
|---|---|---|---|
| 1 | 🔴 | Idempotency schema defeats fan-out: `event_id` is globally `UNIQUE`, so the *second* consumer of the same event fails. `PaymentProcessedEvent` is consumed by both `OrderStatusConsumer` and `NotificationConsumer`; docs claim `UNIQUE(event_id, consumer_name)`. | `V1__init_schema.sql` (`event_id … UNIQUE`), `IdempotencyRecord.java` (`@Column(unique=true)`) |
| 2 | 🔴 | Typed Kafka listeners + `JsonDeserializer(Object.class, mapper, false)` (type headers ignored, default type `OrderCreatedEvent` for every topic). Payload arrives as `Map`, not the `@Payload` type — the end-to-end Kafka flow cannot be trusted; CI never runs the Testcontainers tests that would catch this. | `KafkaConfig.consumerFactory()`, `.github/workflows/ci.yml` |
| 3 | 🔴 | Insufficient-inventory path poisons the transaction: `InventoryService.reserveInventory()` throws inside the consumer's tx; Spring marks it rollback-only, the consumer catches it and continues → `UnexpectedRollbackException` on commit, order never reaches `INVENTORY_FAILED`. | `InventoryConsumer`, `InventoryService.reserveInventory` |
| 4 | 🔴 | Partial-reservation compensation releases **all** items, including ones never reserved — with pre-existing reservations this releases *other orders'* stock. | `InventoryConsumer` rollback loop |
| 5 | 🔴 | `MANUAL_IMMEDIATE` ack inside `@Transactional` listeners commits the Kafka offset *before* the DB tx commits. Crash/rollback after ack = lost event. Correct order is DB commit → offset commit (`AckMode.RECORD`). | `KafkaConfig`, all consumers (`ack.acknowledge()`) |
| 6 | 🔴 | Consumers dual-write: DB tx + `eventProducer.publishSync()` (new random `eventId` each attempt). Publish-then-rollback ⇒ phantom downstream events that downstream idempotency cannot dedupe. Only order *creation* used the outbox. | `InventoryConsumer`, `PaymentConsumer`, `OrderStatusConsumer` |
| 7 | 🔴 | Outbox publisher: whole-batch `@Transactional` held across `future.get()`, **plus** async `whenComplete` racing the sync path (double retry increments, `@Version` conflicts); payload re-published as an untyped `Map`; events stuck forever in `PROCESSING` after a crash (never re-polled); no max retries; batch limit applied in memory after loading everything. | `OutboxPublisher` |
| 8 | 🔴 | Payment IDOR: `GET /api/payments/{orderId}` has no ownership check — any authenticated user reads any payment. | `PaymentController`, `PaymentService` |
| 9 | 🟠 | Actuator over-exposure: `beans, env, caches` exposed, `health.show-details: always`; any authenticated USER can read `/actuator/env`. | `application.yml`, `SecurityConfig` |
| 10 | 🟠 | Hardcoded JWT secret in committed `application.yml` + `docker-compose.yml`; `JwtTokenProvider` calls `Decoders.BASE64.decode()` *outside* its try/catch (non-Base64 secrets such as the one in `.env.example` crash startup) and silently zero-pads weak secrets. | `application.yml`, `docker-compose.yml`, `JwtTokenProvider` |
| 11 | 🟠 | DLQ is largely fictional: `CommonErrorHandler` bean is never wired into the listener factory; nothing is ever published to the `.DLT` topics (handler only writes a DB row with `consumerName="unknown"`, fabricated `eventId`s and `payload.toString()`); backoff is *fixed*, docs claim exponential. | `KafkaRetryConfig`, `KafkaConfig` |
| 12 | 🟠 | `docker-compose` Kafka advertises only `localhost:9092`, while the app container bootstraps via `kafka:9092` → metadata redirects the app to `localhost` inside the container (classic advertised-listener bug). `SPRING_PROFILES_ACTIVE=docker` references a non-existent profile file. | `docker-compose.yml` |
| 13 | 🟠 | `GET /api/inventory/**` is `permitAll` despite the controller declaring `bearerAuth`; exact available/reserved stock is public. | `SecurityConfig`, `InventoryController` |
| 14 | 🟡 | Access-denied returns 400 (`BusinessException`) instead of 403; controllers throw raw `RuntimeException("User not found")` → 500. | `OrderService`, `OrderController`, `NotificationController` |
| 15 | 🟡 | Redis: configured `ObjectMapper` in `RedisConfig` is dead code (serializers built without it, no JavaTimeModule); `CacheErrorHandler` bean is not wired (no `CachingConfigurer`); `products` cache key ignores sort; inventory mutations don't evict product caches (stale availability ≤5 min). | `RedisConfig`, `CacheErrorConfig`, `ProductService`, `InventoryService` |
| 16 | 🟡 | Custom metrics in `MetricsConfig` are registered but **never incremented anywhere**. | `MetricsConfig` (grep: no usages) |
| 17 | 🟡 | `DataInitializer` runs in every profile with `Math.random()` stock levels (non-reproducible); order numbers use `currentTimeMillis + random<10000` (collision-prone under load, unique column); duplicate product lines in one order are not merged. | `DataInitializer`, `OrderService` |
| 18 | 🟡 | `OrderStatusConsumer` swallows `confirmReservation` failures and still completes the order (reserved stock leaks forever); `release/confirm` bulk updates don't bump `@Version`/`updated_at` unlike `reserve`. | `OrderStatusConsumer`, `InventoryRepository` |
| 19 | 🟡 | README contradicts itself: "exactly-once" (line 29) vs "at-least-once" (trade-offs); claims optimistic **and** pessimistic locking in the hot path (only the atomic `UPDATE` is used; lock finders are dead code). | `README.md`, `InventoryRepository` |
| 20 | 🟡 | No page-size cap (`size=100000` allowed); password min length 6; failed-event API returns the entity including stack traces (admin-only, but uncontrolled). | `application.yml`, `RegisterRequest`, `FailedEventController` |
| 21 | ⚪ | `OrderService.updateOrderStatus` logs the *new* status twice; Dockerfile installs unpinned Maven via `apk`, no dependency layer caching, shell-form entrypoint breaks signal handling/graceful shutdown; no Maven wrapper; CI builds twice and never runs integration tests. | `OrderService`, `Dockerfile`, `ci.yml` |
| 22 | ⚪ | `PaymentService` sleeps inside the DB transaction (simulated delay holds a pooled connection); consumers fabricate a random `eventId` when null (defeats idempotency — a null-id event should go to the DLT, non-retryable). | `PaymentService`, all consumers |

## What was already correct (verified)

- Order + outbox row are written in one `@Transactional` method (`OrderService.createOrder` → `OutboxService.saveEvent` with `REQUIRED` propagation) — the dual-write atomicity for *creation* is real.
- Overselling guard is a single atomic conditional `UPDATE … WHERE quantity_available >= :qty` with DB `CHECK (>= 0)` backstops (`InventoryRepository`, V1 schema).
- Passwords are BCrypt-hashed; registration always assigns `USER`; product/inventory writes and failed-event APIs are `ADMIN`-only (URL rules + `@PreAuthorize`).
- Order reads/cancels check ownership (`OrderService.getOrder/cancelOrder`); state machine enum is sound and unit-tested.
- Validation annotations exist on all request DTOs; `GlobalExceptionHandler` produces a consistent `ErrorResponse` with correlation id; `ddl-auto: validate` + Flyway; idempotent producer (`acks=all`, `enable.idempotence=true`).

## Additional findings from live verification (running the real stack)

Running PostgreSQL 17 + Kafka 3.8 (KRaft) + Redis + the packaged jar in the
audit sandbox surfaced defects that no amount of code reading/unit tests caught:

| # | Sev | Finding | Status |
|---|---|---|---|
| 23 | 🔴 | `GET /api/products` without `search` ⇒ **500 on real PostgreSQL**: Hibernate binds the null `:search` inside `CONCAT` as `bytea` → `lower(bytea) does not exist`. Pre-existing; no test ever hit the list endpoint. Fixed with `CAST(:search AS string)` in `ProductRepository`. | ✅ fixed, re-verified live |
| 24 | 🟠 | First DLT attempt blocked 60s and failed: the recoverer targeted the *source* partition (2) of 1-partition `.DLT` topics. Fixed: producer-chosen partition (`-1`), `verifyPartition=false`, 10s send timeout. | ✅ fixed, re-verified live |
| 25 | 🟠 | Regression *introduced by the first outbox rewrite in this audit*: detached-entity `save()` calls that ignore the merged return value carry a stale `@Version` → `OptimisticLockingFailureException` when marking rows `COMPLETED` (rows stuck in `PROCESSING`). Fixed by always reassigning `event = repository.save(event)` and isolating per-row failures. The mocked unit test could not see this; only the live run did. | ✅ fixed, re-verified live |
| 26 | ⚪ | Duplicate YAML key (`spring.data`) introduced while adding pageable limits — SnakeYAML rejects it at startup (PyYAML silently accepts). Caught by executing the jar. | ✅ fixed, re-verified live |

**Verified live (2026-10-25):** Flyway V1→V2 on a clean DB (global unique
constraint gone, composite remains); happy-path order `CREATED → COMPLETED`
(all 4 outbox rows `COMPLETED`, stock 100→98, `reserved` back to 0, payment
`COMPLETED`, `ORDER_COMPLETED` notification); oversell (qty 500 vs 100) →
`INVENTORY_FAILED → FAILED`, stock untouched, zero `failed_events`; duplicate
republish deduped (same `eventId`, one payment, stock unchanged); malformed
event (no `eventId`) → `order.created.DLT` with `kafka_dlt-*` headers +
`failed_events` root-cause row; unauthenticated writes 403, other-user
order/payment reads 403, short password 400; Swagger/api-docs/dashboard 200.

**Not verified here (no Docker daemon in the sandbox):** `docker build`,
Testcontainers suites (`OrderIntegrationTest`, `ConcurrentOrderTest`) — they
compile and are wired into CI; treat them as *unverified* until CI is green on
this branch. App process was also OOM-killed twice when Maven ran concurrently
(sandbox has 2 GB RAM) — an environment limit, not an application failure.

## Fix status for findings 1–22

All items above were addressed on this branch except: #13 is now
`authenticated()` for inventory reads (accepted product decision);
#20's stack-trace exposure remains admin-only by design;
`PaymentService`'s simulated in-transaction sleep (#22) is kept but documented
as simulation-only. Everything else maps to code changes in `git diff main`.

## Delivery guarantees (honest statement, after fixes)

**At-least-once** Kafka delivery + transactional outbox for every state-changing hop +
per-consumer idempotency records committed in the *same* DB transaction as the business
effect ⇒ **effectively-once business effects for recorded operations**, not end-to-end
exactly-once. A crash between outbox-publish and marking `COMPLETED` re-publishes with a
*stable* `eventId`, which consumers dedupe. Offsets are committed (`AckMode.RECORD`)
only after the listener transaction commits.
