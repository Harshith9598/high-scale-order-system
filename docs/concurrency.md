# Concurrency Handling

## Problem: Overselling

Scenario:
- Product has 1 unit left
- User A and User B simultaneously order 1 unit
- Without concurrency control, both orders succeed → negative inventory → overselling

This is critical for e-commerce.

## Solution Layers

### 1. Atomic DB Update (Primary Defense)

```sql
UPDATE inventory 
SET quantity_available = quantity_available - :qty,
    quantity_reserved = quantity_reserved + :qty
WHERE product_id = :productId 
AND quantity_available >= :qty
```

- Returns number of rows updated
- If 0, insufficient stock
- Atomic at DB level, no race condition
- No need for explicit locking in this case

**Code:**
```java
@Modifying
@Query("UPDATE Inventory i SET i.quantityAvailable = i.quantityAvailable - :quantity, " +
       "i.quantityReserved = i.quantityReserved + :quantity " +
       "WHERE i.product.id = :productId AND i.quantityAvailable >= :quantity")
int reserveInventory(Long productId, int quantity);
```

**Why best?** 
- Single round-trip to DB
- No lock held
- High throughput
- Works even with multiple app instances

### 2. Pessimistic Locking (When Needed)

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT i FROM Inventory i WHERE i.product.id = :productId")
Optional<Inventory> findByProductIdWithPessimisticLock(Long productId);
```

- `SELECT ... FOR UPDATE` locks row until transaction commits
- Prevents other transactions from reading/writing
- Use case: complex logic requiring read-modify-write

**Trade-off:** Lower throughput, potential deadlocks, but strong consistency.

### 3. Optimistic Locking (Secondary Defense)

```java
@Version
private Long version;
```

- Each update increments version
- If version mismatch on update, throws `OptimisticLockException`
- Caller should retry

**Benefits:**
- No locks held, high throughput
- Detects lost updates

**Handling:**
- Spring Data JPA automatically checks version
- In service, catch exception and retry or fail gracefully

### 4. Unique Constraints

- `UNIQUE (product_id)` on inventory ensures 1:1
- Prevents duplicate inventory creation
- `UNIQUE (order_id)` on payments ensures one payment per order

### 5. Idempotency (Prevents Double Processing)

- Even if concurrency control works, duplicate Kafka messages could cause double reservation
- Idempotency table prevents processing same event twice

## Order State Machine

Prevents invalid transitions that could occur under concurrency:

```java
public boolean canTransitionTo(OrderStatus target) {
    return VALID_TRANSITIONS.get(this).contains(target);
}
```

Example:
- Order in CREATED can go to INVENTORY_RESERVED, CANCELLED, FAILED
- Cannot go directly to COMPLETED
- If two consumers try to update status simultaneously, one will fail optimistic lock check

## Testing Concurrency

### ConcurrentOrderTest

```java
@Test
void testConcurrentOrdersPreventOverselling() {
    // Product has 5 units
    // 10 threads try to reserve 1 unit each
    // Only 5 should succeed
}
```

**Mechanism:**
- `ExecutorService` with 10 threads
- `CountDownLatch` to start simultaneously
- `AtomicInteger` to count successes
- Assert final inventory: available=0, reserved=5

**Why important?** Demonstrates solution works under real concurrent load, not just theory.

### How to Run

```bash
mvn test -Dtest=ConcurrentOrderTest
```

Requires Testcontainers (PostgreSQL, Kafka, Redis) - real DB, not mocked.

## Handling Failures

### Inventory Reservation Fails

```java
if (updated == 0) {
    throw new InsufficientInventoryException(...);
}
```

- Order marked as INVENTORY_FAILED
- Notification sent to user
- No payment attempted

### Payment Fails After Inventory Reserved

```java
// Release inventory
for (item : order.getItems()) {
    inventoryService.releaseInventory(item.getProductId(), item.getQuantity());
}
```

- Inventory released back to available
- Order marked FAILED
- User notified

### OptimisticLockException

- Could occur if two consumers update same order simultaneously
- Spring's `@Transactional` will rollback
- Kafka's retry mechanism will retry
- Idempotency prevents double side-effects on retry

## Database Isolation Level

- Default: READ_COMMITTED (PostgreSQL default)
- Sufficient for atomic update approach
- Pessimistic locking uses higher isolation for that transaction

## Future Improvements

- **Distributed lock (Redis Redlock):** For cross-service coordination
- **Saga pattern:** Compensating transactions for long-running flows
- **Sharding:** Partition inventory by product category to reduce contention
- **Queue:** Use Kafka partition key to serialize orders for same product (ensures single consumer processes orders for same product sequentially) - trade-off: lower parallelism
- **Reservation timeout:** Reserved inventory auto-released after timeout if payment not completed

## Metrics

- Track `OptimisticLockException` count - high count indicates contention
- Track inventory reservation failure rate
- Alert if overselling detected (negative inventory - should never happen, indicates bug)

## Summary

| Technique | When Used | Pros | Cons |
|-----------|-----------|------|------|
| Atomic UPDATE | Inventory reservation | High throughput, no locks | Limited to simple cases |
| Pessimistic Lock | Complex read-modify-write | Strong consistency | Lower throughput, deadlock risk |
| Optimistic Lock | All entities | No locks, detects conflicts | Requires retry logic |
| Unique Constraints | Prevent duplicates | DB-enforced, simple | Only for uniqueness |
| Idempotency | Kafka consumers | Prevents duplicate side-effects | Extra DB write |
| State Machine | Order status | Prevents invalid transitions | Must be maintained |
