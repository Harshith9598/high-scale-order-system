# Architecture Documentation

## Overview

The High-Scale Event & Order Processing System is designed as an event-driven microservices-ready platform using Spring Boot 3.x, PostgreSQL, Apache Kafka, and Redis.

## High-Level Architecture

```
Client (REST)
    ↓
Spring Boot REST API (Stateless, Horizontally Scalable)
    ↓
┌─────────────────────────────────────────────┐
│  Core Services Layer                        │
│  - Auth Service (JWT)                       │
│  - Product Service (Cached)                 │
│  - Order Service (Transactional)            │
│  - Inventory Service (Pessimistic Lock)     │
│  - Payment Service (Simulated)              │
│  - Notification Service                     │
│  - Outbox Publisher (Reliable)              │
└─────────────────────────────────────────────┘
    ↓                ↓           ↓
PostgreSQL      Redis Cache   Kafka (Event Bus)
    ↓                ↓           ↓
                            ┌─────────────────────┐
                            │  Async Consumers    │
                            │  - Inventory        │
                            │  - Payment          │
                            │  - Order Status     │
                            │  - Notification     │
                            └─────────────────────┘
```

## Design Principles

### 1. Event-Driven Architecture
- **Why Kafka?** Decouples order creation from processing, enables async scaling, provides durability and replay.
- **Why not synchronous?** Direct synchronous calls would couple services and reduce resilience. Event-driven allows independent scaling of consumers.
- **Trade-off:** Eventual consistency vs strong consistency. Chosen for scalability.

### 2. Transactional Outbox Pattern
- **Problem:** DB commit succeeds but Kafka publish fails → inconsistent state.
- **Solution:** Store events in `outbox_events` table within same transaction as order creation. Separate poller publishes to Kafka.
- **Benefits:** Guarantees at-least-once delivery without 2PC.
- **Implementation:** `@Transactional` on order creation + outbox save. Scheduled publisher with retry.

### 3. Idempotency
- **Problem:** Kafka delivers at-least-once; consumers may receive duplicates.
- **Solution:** `idempotency_records` table with unique constraint on (event_id, consumer_name).
- **Mechanism:** `tryMarkProcessed()` checks existence before processing. Race conditions handled via DB unique constraint + exception.
- **Why DB-backed?** Survives restarts, shared across consumer instances.

### 4. Concurrency Control
- **Problem:** Two users buying last item simultaneously → overselling.
- **Solutions layered:**
  1. **Atomic DB update:** `UPDATE inventory SET available = available - :qty WHERE available >= :qty` returns 0 if insufficient.
  2. **Pessimistic locking:** `SELECT ... FOR UPDATE` where needed.
  3. **Optimistic locking:** `@Version` field prevents lost updates.
  4. **Unique constraints:** Prevent duplicate reservations.
- **Demonstrated:** `ConcurrentOrderTest` with 10 threads competing for 5 items.

### 5. Caching Strategy (Redis)
- **What:** Products, inventory, orders.
- **Why:** Product catalog is read-heavy, inventory lookup is high-frequency during order creation.
- **TTL:** Products 5-10 min, inventory 2 min (more volatile).
- **Invalidation:** `@CacheEvict` on write operations.
- **Fallback:** Custom `CacheErrorHandler` logs and falls back to DB if Redis unavailable.

### 6. Retry & DLQ
- **Retry:** Spring Kafka `DefaultErrorHandler` with `FixedBackOff(2s, 3 attempts)`.
- **DLQ:** After 3 failures, event saved to `failed_events` table with exception details.
- **Inspection:** Admin API `/api/failed-events` to view failures.
- **Future:** Could implement reprocessing endpoint.

### 7. Security
- JWT stateless authentication, BCrypt password hashing, role-based authorization.
- Stateless enables horizontal scaling.

### 8. Observability
- Spring Boot Actuator health, metrics, Prometheus.
- Correlation ID propagated via `X-Correlation-Id` header and MDC logging.
- Structured logging with correlation ID in pattern.

## Scalability

- **API Layer:** Stateless, scale horizontally via load balancer.
- **Consumers:** Kafka consumer groups allow parallel processing. Partition count = 3 enables 3 concurrent consumers per group.
- **DB:** Connection pool (Hikari 20 max), indexes on hot paths, read replicas could be added.
- **Redis:** Shared cache across app instances.
- **Kafka:** KRaft mode (no Zookeeper) simplifies ops, topic partitions enable parallelism.

## Failure Modes

| Component | Failure | Handling |
|-----------|---------|----------|
| PostgreSQL down | API fails | Health check fails, circuit breaker (future) |
| Redis down | Cache miss | Fallback to DB via error handler |
| Kafka down | Outbox queues | Outbox retains events, publisher retries |
| Payment fails | Order FAILED | Inventory released, notification sent |
| Inventory insufficient | Order FAILED | No inventory reserved, notification sent |

## Future Improvements
- Saga pattern for distributed transactions
- API Gateway + Service Discovery
- Separate microservices deployment
- CQRS for read/write separation
- Event sourcing
- Distributed tracing (OpenTelemetry)
