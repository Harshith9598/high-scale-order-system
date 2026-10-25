package com.ordersystem.integration;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Upgrades a <b>populated</b> V1 database to V2 and proves:
 * <ul>
 *   <li>no pre-existing row is lost or altered;</li>
 *   <li>the fan-out insert that V1 rejected (same event_id, second consumer)
 *       succeeds after V2;</li>
 *   <li>a true duplicate (same event_id AND consumer) is still rejected;</li>
 *   <li>the global unique constraint is gone and the V2 outbox index exists.</li>
 * </ul>
 * Runs against a real PostgreSQL via Testcontainers (CI integration job).
 * A plain-SQL equivalent of this scenario was also executed manually against
 * PostgreSQL 17 during the audit (see docs/engineering-audit.md).
 */
@Testcontainers
class FlywayMigrationV1ToV2Test {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private Flyway flyway(MigrationVersion target) {
        var config = Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration");
        if (target != null) {
            config = config.target(target);
        }
        return config.load();
    }

    private long count(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private boolean exists(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        }
    }

    @Test
    void populatedV1DatabaseUpgradesToV2WithoutDataLoss() throws Exception {
        // --- migrate to V1 only -------------------------------------------
        assertEquals(1, flyway(MigrationVersion.fromVersion("1")).migrate().migrationsExecuted);

        try (Connection c = connect(); Statement st = c.createStatement()) {
            // --- populate representative V1 data --------------------------
            st.execute("INSERT INTO users (username, email, password) VALUES ('legacy','legacy@example.com','x')");
            st.execute("INSERT INTO products (name, price) VALUES ('Legacy Widget', 12.50)");
            st.execute("INSERT INTO inventory (product_id, quantity_available) VALUES ((SELECT id FROM products WHERE name='Legacy Widget'), 7)");
            st.execute("INSERT INTO orders (order_number, user_id, status, total_amount) " +
                    "VALUES ('ORD-LEGACY-1', (SELECT id FROM users WHERE username='legacy'), 'CREATED', 25.00)");
            st.execute("INSERT INTO idempotency_records (event_id, consumer_name, aggregate_id) VALUES ('evt-fanout','order-status-consumer','1')");
            st.execute("INSERT INTO idempotency_records (event_id, consumer_name, aggregate_id) VALUES ('evt-other','inventory-consumer','1')");
            st.execute("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, topic) " +
                    "VALUES ('Order','1','OrderCreated','{\"eventId\":\"evt-legacy\"}','order.created')");

            // V1 behaviour that V2 exists to fix: a SECOND consumer for the
            // same event violates the global UNIQUE(event_id).
            assertThrows(SQLException.class, () -> {
                try (Statement s2 = c.createStatement()) {
                    s2.execute("INSERT INTO idempotency_records (event_id, consumer_name, aggregate_id) " +
                            "VALUES ('evt-fanout','notification-consumer','1')");
                }
            }, "V1 must reject the fan-out insert (that is the bug V2 fixes)");

            long usersBefore = count(c, "SELECT count(*) FROM users");
            long idemBefore = count(c, "SELECT count(*) FROM idempotency_records");
            long outboxBefore = count(c, "SELECT count(*) FROM outbox_events");

            // --- upgrade to latest (V2) -----------------------------------
            assertEquals(1, flyway(null).migrate().migrationsExecuted, "exactly V2 should run");

            // --- nothing lost ---------------------------------------------
            assertEquals(usersBefore, count(c, "SELECT count(*) FROM users"));
            assertEquals(idemBefore, count(c, "SELECT count(*) FROM idempotency_records"));
            assertEquals(outboxBefore, count(c, "SELECT count(*) FROM outbox_events"));
            assertEquals(1, count(c, "SELECT count(*) FROM outbox_events WHERE payload LIKE '%evt-legacy%'"));
            assertEquals(7, count(c, "SELECT quantity_available FROM inventory"));

            // --- fan-out now works, true duplicates still rejected --------
            st.execute("INSERT INTO idempotency_records (event_id, consumer_name, aggregate_id) " +
                    "VALUES ('evt-fanout','notification-consumer','1')");
            assertEquals(idemBefore + 1, count(c, "SELECT count(*) FROM idempotency_records"));
            assertThrows(SQLException.class, () -> {
                try (Statement s2 = c.createStatement()) {
                    s2.execute("INSERT INTO idempotency_records (event_id, consumer_name, aggregate_id) " +
                            "VALUES ('evt-fanout','notification-consumer','1')");
                }
            }, "duplicate (event_id, consumer_name) must still be rejected");

            // --- schema shape after V2 -------------------------------------
            assertFalse(exists(c, "SELECT 1 FROM pg_constraint WHERE conname='idempotency_records_event_id_key'"),
                    "global unique constraint must be dropped");
            assertTrue(exists(c, "SELECT 1 FROM pg_indexes WHERE indexname='idx_idempotency_event_consumer' AND indexdef LIKE '%UNIQUE%'"),
                    "composite unique index must remain");
            assertTrue(exists(c, "SELECT 1 FROM pg_indexes WHERE indexname='idx_outbox_status_retry_created'"),
                    "V2 outbox composite index must exist");
        }
    }
}
