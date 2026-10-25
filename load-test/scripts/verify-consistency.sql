-- verify-consistency.sql — READ-ONLY post-run checks (SELECTs only).
--
-- Usage (run a few seconds after the test; consumers may still be draining):
--   docker compose exec -T postgres psql -U orderuser -d orderdb \
--     -v userprefix='loadtest' -v initial_stock=100000 \
--     -f - < load-test/scripts/verify-consistency.sql
\set ON_ERROR_STOP on
SELECT current_database() AS db, now() AS checked_at;

-- 1) Orders by final state for synthetic users of any run.
SELECT o.status, count(*) AS orders
FROM orders o JOIN users u ON u.id = o.user_id
WHERE u.username LIKE :'userprefix' || '-%'
GROUP BY o.status ORDER BY o.status;

-- 2) Stuck orders: non-terminal and older than 2 minutes (expect 0 once drained).
SELECT count(*) AS stuck_non_terminal_orders
FROM orders o JOIN users u ON u.id = o.user_id
WHERE u.username LIKE :'userprefix' || '-%'
  AND o.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
  AND o.created_at < now() - interval '2 minutes';

-- 3) Payment cardinality: COMPLETED orders must have exactly one COMPLETED payment.
SELECT count(*) AS completed_orders_without_exactly_one_completed_payment
FROM orders o JOIN users u ON u.id = o.user_id
WHERE u.username LIKE :'userprefix' || '-%' AND o.status = 'COMPLETED'
  AND (SELECT count(*) FROM payments p WHERE p.order_id = o.id AND p.status = 'COMPLETED') <> 1;

-- 4) Duplicate payments (also guarded by UNIQUE(order_id)); expect 0.
SELECT count(*) AS orders_with_multiple_payments
FROM (SELECT order_id FROM payments GROUP BY order_id HAVING count(*) > 1) d;

-- 5) Inventory sanity + stock reconciliation for load-test products.
--    sold = qty in orders at/after payment success (PAYMENT_COMPLETED/PROCESSING/COMPLETED).
--    Invariant: initial_stock = available + reserved + sold  (delta must be 0).
WITH sold AS (
  SELECT oi.product_id, COALESCE(SUM(oi.quantity), 0) AS qty
  FROM order_items oi JOIN orders o ON o.id = oi.order_id
  WHERE o.status IN ('PAYMENT_COMPLETED', 'PROCESSING', 'COMPLETED')
  GROUP BY oi.product_id
)
SELECT p.name, i.quantity_available, i.quantity_reserved, COALESCE(s.qty, 0) AS sold,
       (:initial_stock - i.quantity_available - i.quantity_reserved - COALESCE(s.qty, 0)) AS delta_must_be_0,
       (i.quantity_available < 0 OR i.quantity_reserved < 0) AS negative_stock
FROM products p JOIN inventory i ON i.product_id = p.id
LEFT JOIN sold s ON s.product_id = p.id
WHERE p.name LIKE 'LOADTEST-%' ORDER BY p.name;

-- 6) Duplicate consumer effects at the idempotency layer (unique index; expect 0).
SELECT count(*) AS duplicate_event_consumer_rows
FROM (SELECT event_id, consumer_name FROM idempotency_records
      GROUP BY event_id, consumer_name HAVING count(*) > 1) d;

-- 7) Pipeline health: outbox backlog by status + dead letters recorded.
SELECT status, count(*) FROM outbox_events GROUP BY status ORDER BY status;
SELECT count(*) AS failed_events_rows FROM failed_events;
