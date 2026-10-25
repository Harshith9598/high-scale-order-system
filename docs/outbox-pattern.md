# Transactional Outbox Pattern

## Problem: Dual Write

Order creation requires two operations:
1. Save order to PostgreSQL
2. Publish OrderCreated event to Kafka

Naive approach:

```java
@Transactional
public Order createOrder() {
    orderRepository.save(order); // DB write
    kafkaTemplate.send("order.created", event); // Kafka write
}
```

**Failure scenario:**
- DB save succeeds
- Kafka send fails (network, broker down)
- Result: Order exists but no event → inventory never reserved → order stuck in CREATED

**Other naive fix:**

```java
kafkaTemplate.send(...);
orderRepository.save(...);
```

If Kafka succeeds but DB fails, event published for non-existent order.

**Two-Phase Commit (2PC):** Would solve but not supported by Kafka + PostgreSQL, complex, reduces availability.

## Solution: Outbox Pattern

### Idea

Store events in same DB transaction as business data. Separate process publishes to Kafka.

```
1. BEGIN TRANSACTION
2. INSERT INTO orders ...
3. INSERT INTO outbox_events (payload, topic, status=PENDING) ...
4. COMMIT

5. Background poller: SELECT * FROM outbox_events WHERE status=PENDING
6. Publish to Kafka
7. UPDATE outbox_events SET status=COMPLETED
```

If step 6 fails, event remains PENDING and will be retried.

### Implementation

**Table:**
```sql
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    topic VARCHAR(200) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    correlation_id VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    next_retry_at TIMESTAMP DEFAULT NOW(),
    error_message VARCHAR(1000),
    version BIGINT
);
```

**Service:**
```java
@Transactional
public OrderDto createOrder(...) {
    Order order = orderRepository.save(order);
    OrderCreatedEvent event = buildEvent(order);
    outboxService.saveEvent("Order", order.getId().toString(), "OrderCreated", event, "order.created", correlationId);
    return mapper.toDto(order);
}
```

Both saves in same transaction → atomic.

**Publisher:**
```java
@Scheduled(fixedDelay=5000)
@Transactional
public void publishPendingEvents() {
    List<OutboxEvent> events = outboxRepository.findByStatusAndNextRetryAtBefore(PENDING, now);
    for (event : events) {
        try {
            event.setStatus(PROCESSING);
            repository.save(event);
            kafkaTemplate.send(event.getTopic(), event.getAggregateId(), deserialize(event.getPayload()));
            event.setStatus(COMPLETED);
        } catch (Exception e) {
            event.setStatus(FAILED);
            event.setRetryCount(event.getRetryCount() + 1);
            event.setNextRetryAt(now.plusSeconds(2^retryCount * 30));
        }
    }
}
```

### Why This Design?

**Benefits:**
- Guarantees at-least-once delivery without 2PC
- Simple, uses existing DB
- Survives Kafka outages (events queued in DB)
- Easy to monitor (count of PENDING events)

**Trade-offs:**
- At-least-once, not exactly-once → need idempotency on consumer side (we have it)
- Polling adds latency (5s interval) vs immediate publish
- Extra DB table and poller
- Duplicate publishing possible if publisher crashes after Kafka send but before marking COMPLETED → idempotency handles

**Alternatives:**

1. **Kafka Transactions + DB:** Not supported across systems
2. **Change Data Capture (Debezium):** Reads DB WAL and publishes to Kafka. More complex, requires Debezium deployment, but lower latency and no poller. Could be future improvement.
3. **Event Sourcing:** Store only events, derive state. More radical redesign, not needed for this use case.

### Retry Strategy

- Exponential backoff: `next_retry_at = now + 2^retryCount * 30s`
- Prevents hammering failing Kafka
- After many retries, could alert or move to DLQ

### Monitoring

- Metric: number of PENDING events
- Alert if PENDING grows (Kafka down or publisher stuck)
- Dashboard: outbox_events table query

### Testing

- Unit test: verify outbox save called in same transaction
- Integration test: stop Kafka, create order, verify outbox PENDING, start Kafka, verify published
- Manual: check outbox table after order creation

### Future Improvements

- Use Debezium for CDC instead of polling
- Batch publishing for throughput
- Separate publisher service for independent scaling
- Add priority to events
