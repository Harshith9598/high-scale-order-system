# Reliability & Resilience

## Overview

Production systems must handle failures gracefully. This document describes how the order platform handles various failure modes.

## Failure Modes & Handling

### 1. Database Failures

**PostgreSQL Down:**
- API returns 500, health check fails
- Outbox publisher cannot save events, but order creation already failed so no inconsistency
- Consumers cannot update order status, Kafka messages will be retried
- **Mitigation:** 
  - Connection pool (Hikari) with timeout
  - Health check alerts
  - Read replica (future)
  - Circuit breaker (future)

**Connection Pool Exhausted:**
- HikariCP max pool 20, min idle 5
- If all connections busy, new requests wait 30s then fail
- **Mitigation:** Monitor active connections metric, increase pool if needed, optimize queries

### 2. Redis Failures

**Redis Down:**
- CacheErrorHandler logs warning and falls back to DB
- API continues working, just slower
- No data loss (cache is ephemeral)
- **Mitigation:** Redis Sentinel or Cluster for HA, but app already resilient

**Redis Slow:**
- Timeout 2000ms
- If timeout, treated as cache miss, falls back to DB

### 3. Kafka Failures

**Kafka Broker Down During Order Creation:**
- Order saved to DB + outbox PENDING
- Outbox publisher retries every 5s
- When Kafka recovers, events published
- Order processing delayed but not lost
- **Why outbox critical:** Without it, order would be stuck

**Kafka Down During Consumer Processing:**
- Consumer cannot commit offset, message will be retried
- If consumer crashes, Kafka rebalances and another consumer picks up
- Idempotency prevents double-processing on retry

**Topic Not Exist:**
- Auto-create enabled in dev (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`)
- In prod, topics should be pre-created with proper partitions/replication
- Admin API or Terraform for topic management

**Consumer Lag:**
- If consumers slow, lag grows
- Monitor via Kafka metrics or `kafka-consumer-groups.sh`
- Scale consumers by increasing partitions and consumer instances

### 4. Payment Failures

**Simulated Random Failure (5% rate):**
- Payment marked FAILED
- Inventory released
- Order marked FAILED
- Notification sent to user: "Payment failed"
- User can retry order

**Why 5%?** Simulates real-world payment gateway failures (network, insufficient funds, etc.) without real integration.

**Handling:**
```java
if (paymentFailed) {
    order.setStatus(FAILED);
    releaseInventory();
    sendNotification("Payment failed");
}
```

### 5. Inventory Failures

**Insufficient Stock:**
- Atomic update returns 0 rows
- Throw `InsufficientInventoryException`
- Order marked INVENTORY_FAILED → FAILED
- Notification: "Out of stock"
- No payment attempted

**Race Condition:**
- Atomic update prevents overselling
- Even if check-then-update race, WHERE clause ensures consistency

### 6. Consumer Failures & Retry

**Transient Failure (e.g., DB deadlock):**
- Consumer throws exception
- Spring Kafka `DefaultErrorHandler` retries 3 times with 2s backoff
- If succeeds on retry, ack and continue

**Permanent Failure (e.g., bug):**
- After 3 retries, event saved to `failed_events` table
- Includes: eventId, payload, exception, stack trace, retry count
- Admin can inspect via `/api/failed-events`
- Future: manual reprocessing endpoint

**Code:**
```java
@Bean
public CommonErrorHandler kafkaErrorHandler() {
    FixedBackOff backOff = new FixedBackOff(2000L, 3);
    DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
        // Save to DLQ table
        failedEventRepository.save(...);
    }, backOff);
    return handler;
}
```

### 7. Order Cancellation

**User Cancels:**
- Check if order in cancellable state (CREATED, INVENTORY_RESERVED, PAYMENT_PENDING, etc.)
- If COMPLETED or CANCELLED, reject
- If inventory reserved, release it
- If payment completed, refund (simulated: mark REFUNDED)
- Update status to CANCELLED

**Invalid State Transition:**
- State machine prevents invalid transitions
- Throws `InvalidOrderStateException`
- Returns 400 to client

### 8. Security Failures

**Invalid JWT:**
- Filter returns 401
- No stack trace exposed

**Expired JWT:**
- 401, client must re-login

**Access Denied:**
- 403, user lacks role
- Example: USER tries to access `/api/failed-events` (ADMIN only)

### 9. Validation Failures

**Bad Input:**
- Jakarta Bean Validation checks
- Returns 400 with field-level errors
```json
{
  "error": "VALIDATION_ERROR",
  "validationErrors": {
    "email": "Invalid email format",
    "quantity": "must be greater than 0"
  }
}
```

## Observability for Reliability

### Health Checks

- `/actuator/health` shows DB, Redis, Kafka health
- Used by Docker HEALTHCHECK and orchestrators (K8s)
- If any down, overall health DOWN

### Metrics

- `orders.failed.total` - alert if spikes
- `kafka.events.failed.total` - alert if high
- `http.server.requests` - latency p95, p99
- `cache.misses` - if 100% miss, Redis may be down
- DB connection pool metrics

### Logging

- Structured logging with correlationId
- Every request and event logs correlationId for tracing
- Errors log stack trace server-side but not exposed to client

### Correlation ID

- Client can send `X-Correlation-Id` header, or server generates UUID
- Propagated through: REST → OrderService → Outbox → Kafka → Consumers → Notification
- Enables tracing order flow across logs

## Chaos Testing (Future)

- Kill PostgreSQL container mid-order, verify outbox recovers
- Kill Kafka broker, verify orders queued
- Kill Redis, verify fallback to DB
- Simulate network latency with Toxiproxy
- Use Chaos Monkey for Spring Boot

## Disaster Recovery

- **PostgreSQL:** Regular backups, point-in-time recovery
- **Kafka:** Replication factor 1 in dev, should be 3 in prod
- **Redis:** AOF persistence enabled, but cache can be rebuilt
- **Outbox:** If publisher fails, events remain PENDING, no data loss

## Summary Table

| Component | Failure | Impact | Recovery | Data Loss? |
|-----------|---------|--------|----------|------------|
| PostgreSQL | Down | API 500, orders fail | Auto when DB up | No |
| Redis | Down | Slower, cache miss | Auto when Redis up | No (cache) |
| Kafka | Down | Orders queued in outbox | Outbox publisher retries | No |
| Consumer | Crash | Message retried | Kafka rebalance | No |
| Payment | Random fail | Order FAILED, inventory released | User retries | No |
| Inventory | Insufficient | Order FAILED | User retries or admin restocks | No |

## Design Principles

1. **Fail fast, recover automatically** where possible
2. **No data loss** for orders (outbox + idempotency)
3. **Graceful degradation** (Redis down → DB)
4. **Clear error messages** to client without exposing internals
5. **Observability** to detect failures quickly
6. **Idempotency** to handle retries safely
