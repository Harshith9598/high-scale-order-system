# Database Design

## Why PostgreSQL?

- **ACID compliance** critical for order/inventory consistency
- **Strong typing, constraints, FKs** prevent data corruption
- **Optimistic locking support** via `@Version`
- **Excellent index performance** for high-read workloads
- **JSONB support** if needed for event payloads
- **Proven scalability** and ecosystem

Alternatives considered:
- MySQL: similar, but PostgreSQL has better constraint handling and advanced types
- MongoDB: not suitable for transactional order data requiring strong consistency

## Entity Relationship Diagram

```mermaid
erDiagram
    USERS ||--o{ USER_ROLES : has
    ROLES ||--o{ USER_ROLES : assigned
    USERS ||--o{ ORDERS : places
    PRODUCTS ||--o{ INVENTORY : has
    ORDERS ||--o{ ORDER_ITEMS : contains
    PRODUCTS ||--o{ ORDER_ITEMS : referenced
    ORDERS ||--o{ PAYMENTS : has
    ORDERS ||--o{ NOTIFICATIONS : generates
    USERS ||--o{ NOTIFICATIONS : receives
    ORDERS ||--o{ OUTBOX_EVENTS : emits

    USERS {
        bigint id PK
        varchar username UK
        varchar email UK
        varchar password
        boolean enabled
        timestamp created_at
        timestamp updated_at
        bigint version
    }

    ROLES {
        bigint id PK
        varchar name UK
        varchar description
        timestamp created_at
    }

    USER_ROLES {
        bigint user_id FK
        bigint role_id FK
    }

    PRODUCTS {
        bigint id PK
        varchar name
        varchar description
        numeric price
        varchar category
        boolean is_active
        timestamp created_at
        timestamp updated_at
        bigint version
    }

    INVENTORY {
        bigint id PK
        bigint product_id FK UK
        int quantity_available
        int quantity_reserved
        int low_stock_threshold
        timestamp created_at
        timestamp updated_at
        bigint version
    }

    ORDERS {
        bigint id PK
        varchar order_number UK
        bigint user_id FK
        varchar status
        numeric total_amount
        varchar currency
        varchar correlation_id
        timestamp created_at
        timestamp updated_at
        bigint version
    }

    ORDER_ITEMS {
        bigint id PK
        bigint order_id FK
        bigint product_id FK
        int quantity
        numeric unit_price
        numeric total_price
        timestamp created_at
    }

    PAYMENTS {
        bigint id PK
        bigint order_id FK UK
        varchar payment_id_external
        numeric amount
        varchar currency
        varchar status
        varchar failure_reason
        varchar correlation_id
        timestamp created_at
        timestamp updated_at
        bigint version
    }

    NOTIFICATIONS {
        bigint id PK
        bigint user_id FK
        bigint order_id FK
        varchar type
        varchar title
        varchar message
        boolean is_read
        varchar correlation_id
        timestamp created_at
    }

    OUTBOX_EVENTS {
        bigint id PK
        varchar aggregate_type
        varchar aggregate_id
        varchar event_type
        text payload
        varchar topic
        varchar status
        int retry_count
        varchar correlation_id
        timestamp created_at
        timestamp processed_at
        timestamp next_retry_at
        varchar error_message
        bigint version
    }

    IDEMPOTENCY_RECORDS {
        bigint id PK
        varchar event_id UK
        varchar consumer_name
        varchar aggregate_id
        timestamp processed_at
        timestamp created_at
    }

    FAILED_EVENTS {
        bigint id PK
        varchar event_id
        varchar order_id
        varchar topic
        varchar consumer_name
        text payload
        varchar exception_message
        text stack_trace
        int retry_count
        varchar correlation_id
        timestamp created_at
    }
```

## Key Design Decisions

### Primary Keys
- `BIGSERIAL` (auto-increment bigint) for simplicity and performance
- Alternative: UUID would enable distributed ID generation but has index fragmentation cost

### Indexes
- **Users:** email, username for login lookup
- **Products:** name, category for filtering
- **Orders:** user_id, status, created_at for user queries and admin filtering
- **OrderItems:** order_id, product_id for joins
- **Outbox:** status + next_retry_at for poller efficiency
- **Idempotency:** event_id unique + (event_id, consumer_name) unique for fast duplicate check
- **FailedEvents:** topic, created_at for DLQ inspection

### Constraints
- `CHECK (price >= 0)` prevents negative pricing
- `CHECK (quantity > 0)` ensures valid order quantities
- `UNIQUE (product_id)` on inventory ensures 1:1 product-inventory
- `UNIQUE (order_id)` on payments ensures one payment per order
- `UNIQUE (event_id, consumer_name)` on idempotency prevents double-processing

### Optimistic Locking
- `@Version` on User, Product, Inventory, Order, Payment, OutboxEvent
- Prevents lost updates in concurrent scenarios
- Throws `OptimisticLockException` which triggers retry

### Timestamps
- `created_at` immutable, `updated_at` auto-updated via `@PrePersist/@PreUpdate`
- Use `Instant` (UTC) for consistency across timezones

### Status Enums
- Stored as `VARCHAR` with `EnumType.STRING` for readability and debuggability
- Application layer enforces valid transitions via `OrderStatus.canTransitionTo()`

## Migrations

- **Tool:** Flyway
- **Location:** `src/main/resources/db/migration`
- **Naming:** `V1__init_schema.sql`
- **Baseline:** `baseline-on-migrate: true` for existing DBs
- **Future:** Each schema change gets new versioned file (V2__, V3__, etc.)

## Sample Data

- `DataInitializer` creates 10 sample products with inventory if DB empty
- Avoids need for manual seeding in dev
