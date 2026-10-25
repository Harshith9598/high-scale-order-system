# Idempotency Design

## Problem

Kafka provides at-least-once delivery. Network issues, consumer crashes, or rebalance can cause duplicate delivery.

Without idempotency:
- Inventory reserved twice → overselling or negative stock
- Payment processed twice → double charging (even simulated)
- Duplicate notifications → poor UX

## Solution: Database-Backed Idempotency

### Table

```sql
CREATE TABLE idempotency_records (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) NOT NULL UNIQUE,
    consumer_name VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100),
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, consumer_name)
);
```

### Flow

```
Receive Event (eventId=abc, consumer=inventory-consumer)
    ↓
Check idempotency_records WHERE event_id=abc AND consumer_name=inventory-consumer
    ↓
Exists? → Skip processing, ack
Not exists? → Try insert
    ↓
Insert succeeds → Process event, ack
Insert fails (unique violation) → Another thread processed concurrently → Skip
```

### Code

```java
public boolean tryMarkProcessed(String eventId, String consumerName, String aggregateId) {
    if (isProcessed(eventId, consumerName)) {
        return false; // duplicate
    }
    try {
        save(new IdempotencyRecord(eventId, consumerName, aggregateId));
        return true;
    } catch (DataIntegrityViolationException e) {
        return false; // race condition
    }
}
```

### Why This Design?

**Alternatives considered:**

1. **In-memory Set:** Fast but loses state on restart, not shared across instances
2. **Redis:** Fast, shared, but another failure point. If Redis down, duplicates occur. Also needs persistence config.
3. **Kafka transactional idempotence:** Handles producer duplicates, not consumer duplicates
4. **DB unique constraint on business table:** e.g., unique payment per orderId prevents double payment but not generic

**Chosen: PostgreSQL-backed**

- **Durable:** Survives restarts
- **Shared:** Works across multiple consumer instances
- **Transactional:** Can be part of consumer's DB transaction
- **Simple:** No extra infrastructure
- **Consistent:** Uses same DB as business data

**Trade-offs:**

- Extra DB write per event (acceptable overhead)
- Requires DB available (but if DB down, consumer can't process anyway)
- Table grows indefinitely → needs cleanup job (future: TTL or archive after 30 days)

### Consumer-Specific Keys

- Same eventId processed by different consumers is NOT duplicate
- Example: `order.created` eventId=xyz processed by both `inventory-consumer` and `notification-consumer` is valid
- Therefore key is `(eventId, consumerName)` not just `eventId`

### Testing

- `IdempotencyServiceTest` verifies first event processed, duplicate skipped
- `ConcurrentOrderTest` includes race condition test
- Integration test could publish same event twice and verify inventory reserved once

### Future Improvements

- Add `created_at` index and periodic cleanup of records older than 7 days
- Use Redis as L1 cache for idempotency check to reduce DB load (check Redis first, then DB)
- Consider Bloom filter for fast negative check
