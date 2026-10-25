package com.ordersystem.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Business counters, incremented at the actual state transitions (the previous
 * MetricsConfig registered counters that nothing ever incremented).
 * Framework metrics (HTTP, JVM, Kafka client, Hikari) come from Micrometer
 * auto-configuration and are exposed at /actuator/prometheus (ADMIN only).
 */
@Component
public class OrderMetrics {

    private final Counter ordersCreated;
    private final Counter ordersCompleted;
    private final Counter ordersFailed;

    public OrderMetrics(MeterRegistry registry) {
        this.ordersCreated = Counter.builder("orders.created.total")
                .description("Orders created (committed with their outbox event)").register(registry);
        this.ordersCompleted = Counter.builder("orders.completed.total")
                .description("Orders that reached COMPLETED").register(registry);
        this.ordersFailed = Counter.builder("orders.failed.total")
                .description("Orders that reached FAILED or INVENTORY_FAILED").register(registry);
    }

    public void orderCreated() { ordersCreated.increment(); }
    public void orderCompleted() { ordersCompleted.increment(); }
    public void orderFailed() { ordersFailed.increment(); }
}
