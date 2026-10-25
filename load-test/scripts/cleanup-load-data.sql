-- cleanup-load-data.sql — DESTRUCTIVE. Local test databases only.
--
-- Removes ONLY synthetic data: orders belonging to loadtest users (and their
-- payments/notifications/items), LOADTEST-* products+inventory, and the
-- loadtest users themselves. Refuses to run on a database not named 'orderdb'
-- (edit the guard list ONLY for an explicitly disposable local database).
-- Stop the application first so consumers do not process in-flight events
-- for rows being deleted.
--
-- Usage:
--   docker compose stop app
--   docker compose exec -T postgres psql -U orderuser -d orderdb \
--     -f - < load-test/scripts/cleanup-load-data.sql
\set ON_ERROR_STOP on
DO $guard$
BEGIN
  IF current_database() NOT IN ('orderdb') THEN
    RAISE EXCEPTION 'refusing cleanup on database %', current_database();
  END IF;
END $guard$;

BEGIN;
DELETE FROM payments      WHERE order_id IN (SELECT o.id FROM orders o JOIN users u ON u.id=o.user_id WHERE u.username LIKE 'loadtest-%');
DELETE FROM notifications WHERE user_id  IN (SELECT id FROM users WHERE username LIKE 'loadtest-%');
DELETE FROM orders        WHERE user_id  IN (SELECT id FROM users WHERE username LIKE 'loadtest-%');  -- cascades order_items
DELETE FROM inventory     WHERE product_id IN (SELECT id FROM products WHERE name LIKE 'LOADTEST-%');
DELETE FROM products      WHERE name LIKE 'LOADTEST-%';
DELETE FROM user_roles    WHERE user_id  IN (SELECT id FROM users WHERE username LIKE 'loadtest-%');
DELETE FROM users         WHERE username LIKE 'loadtest-%';
COMMIT;
\echo Cleanup done. Remaining synthetic rows (all should be 0):
SELECT (SELECT count(*) FROM products WHERE name LIKE 'LOADTEST-%') AS products_left,
       (SELECT count(*) FROM users WHERE username LIKE 'loadtest-%') AS users_left;
