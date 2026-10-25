# Kafka Event Architecture

## Why Kafka?

- **Durability:** Events persisted, survive consumer failures
- **Scalability:** Partitions enable parallel processing
- **Decoupling:** Producers don't need to know consumers
- **Replay:** Can reprocess events for debugging or new consumers
- **Ordering:** Partition key (orderId) ensures order events processed in order

Alternatives:
- RabbitMQ: good for routing but less durable, no replay
- Redis Pub/Sub: in-memory, not durable
- Direct REST calls: tight coupling, no resilience

## Topics

| Topic | Partitions | Purpose | Producer | Consumers |
|-------|------------|---------|----------|-----------|
| `order.created` | 3 | Order creation event | OrderService (via Outbox) | InventoryConsumer |
| `inventory.reserved` | 3 | Inventory success | InventoryConsumer | PaymentConsumer |
| `inventory.failed` | 3 | Inventory failure | InventoryConsumer | OrderStatusConsumer, NotificationConsumer |
| `payment.processed` | 3 | Payment success | PaymentConsumer | OrderStatusConsumer |
| `payment.failed` | 3 | Payment failure | PaymentConsumer | OrderStatusConsumer, NotificationConsumer |
| `order.completed` | 3 | Order completion | OrderStatusConsumer | NotificationConsumer |
| `*.DLT` | 1 | Dead Letter | ErrorHandler | Manual inspection |

## Event Schemas

### OrderCreatedEvent
```json
{
  "eventId": "uuid",
  "correlationId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z",
  "eventType": "OrderCreated",
  "orderId": 123,
  "orderNumber": "ORD-123",
  "userId": 456,
  "totalAmount": 199.99,
  "currency": "USD",
  "items": [
    {
      "productId": 1,
      "productName": "Laptop",
      "quantity": 1,
      "unitPrice": 199.99
    }
  ]
}
```

### InventoryReservedEvent
```json
{
  "eventId": "uuid",
  "correlationId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z",
  "eventType": "InventoryReserved",
  "orderId": 123,
  "orderNumber": "ORD-123",
  "userId": 456,
  "success": true,
  "failureReason": null
}
```

### PaymentProcessedEvent
```json
{
  "eventId": "uuid",
  "correlationId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z",
  "eventType": "PaymentProcessed",
  "orderId": 123,
  "orderNumber": "ORD-123",
  "userId": 456,
  "paymentId": 789,
  "amount": 199.99,
  "success": true,
  "failureReason": null
}
```

### OrderCompletedEvent
```json
{
  "eventId": "uuid",
  "correlationId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z",
  "eventType": "OrderCompleted",
  "orderId": 123,
  "orderNumber": "ORD-123",
  "userId": 456
}
```

## Common Fields

All events extend `BaseEvent`:
- `eventId`: UUID for idempotency
- `correlationId`: Tracks request across services
- `timestamp`: Event creation time (Instant)
- `eventType`: String discriminator

## Producer Configuration

- `acks=all` ensures durability
- `enable.idempotence=true` prevents duplicate production
- `retries=3` handles transient failures
- `JsonSerializer` for JSON payloads

## Consumer Configuration

- `group-id` enables load balancing
- `auto-offset-reset=earliest` ensures no events missed on first start
- `enable-auto-commit=false` manual ack for precise control
- `max.poll.records=50` batch size for throughput
- `concurrency=3` matches partition count

## Error Handling

- `DefaultErrorHandler` with `FixedBackOff(2s, 3 attempts)`
- After 3 retries, event saved to `failed_events` table
- Stack trace and payload preserved for debugging
- Admin can inspect via `/api/failed-events`

## Idempotency

- Each consumer checks `idempotency_records` before processing
- Unique constraint on (eventId, consumerName)
- Prevents double inventory reservation, double payment, duplicate notifications

## Outbox Integration

- OrderService doesn't publish directly to Kafka
- Saves event to `outbox_events` table in same transaction
- `OutboxPublisher` polls every 5s and publishes pending events
- Ensures no lost events if Kafka down during order creation

## Monitoring

- Metrics: `kafka.events.published.total`, `kafka.events.failed.total`
- Logs: eventId, correlationId, topic, partition, offset for tracing
- Actuator: Kafka health indicator
