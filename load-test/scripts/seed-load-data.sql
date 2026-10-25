-- seed-load-data.sql — synthetic products/inventory for load tests.
--
-- Why SQL: product/inventory write APIs require role ADMIN, registration
-- always yields USER, and no admin is seeded — so API-only seeding of
-- dedicated products is impossible. This script is for a LOCAL TEST DATABASE
-- ONLY (see README). Idempotent: safe to re-run before a run (while the app
-- is idle), it tops stock back up to :'stock'.
--
-- Usage (Docker Compose stack):
--   docker compose exec -T postgres psql -U orderuser -d orderdb \
--     -v stock=100000 -f - < load-test/scripts/seed-load-data.sql
\set ON_ERROR_STOP on
\echo Seeding into database:
SELECT current_database() AS db;
DO $guard$
BEGIN
  IF current_database() NOT IN ('orderdb') THEN
    RAISE EXCEPTION 'refusing to seed non-local-test database %', current_database();
  END IF;
END $guard$;

INSERT INTO products (name, description, price, category, is_active)
SELECT 'LOADTEST-' || g, 'Synthetic load-test product', 10.00, 'LoadTest', true
FROM generate_series(1, 5) AS g
WHERE NOT EXISTS (SELECT 1 FROM products WHERE name = 'LOADTEST-' || g);

INSERT INTO inventory (product_id, quantity_available, quantity_reserved, low_stock_threshold)
SELECT p.id, :'stock', 0, 10
FROM products p WHERE p.name LIKE 'LOADTEST-%'
ON CONFLICT (product_id) DO UPDATE
  SET quantity_available = EXCLUDED.quantity_available,
      quantity_reserved = 0;

SELECT p.id, p.name, p.price, i.quantity_available
FROM products p JOIN inventory i ON i.product_id = p.id
WHERE p.name LIKE 'LOADTEST-%' ORDER BY p.name;
