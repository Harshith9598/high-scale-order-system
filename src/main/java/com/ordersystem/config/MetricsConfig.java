package com.ordersystem.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter orderCreatedCounter(MeterRegistry registry) {
        return Counter.builder("orders.created.total")
                .description("Total number of orders created")
                .register(registry);
    }

    @Bean
    public Counter orderCompletedCounter(MeterRegistry registry) {
        return Counter.builder("orders.completed.total")
                .description("Total number of orders completed")
                .register(registry);
    }

    @Bean
    public Counter orderFailedCounter(MeterRegistry registry) {
        return Counter.builder("orders.failed.total")
                .description("Total number of orders failed")
                .register(registry);
    }

    @Bean
    public Counter kafkaEventsPublishedCounter(MeterRegistry registry) {
        return Counter.builder("kafka.events.published.total")
                .description("Total Kafka events published")
                .register(registry);
    }

    @Bean
    public Counter kafkaEventsFailedCounter(MeterRegistry registry) {
        return Counter.builder("kafka.events.failed.total")
                .description("Total Kafka events failed")
                .register(registry);
    }

    @Bean
    public Timer orderProcessingTimer(MeterRegistry registry) {
        return Timer.builder("orders.processing.time")
                .description("Order processing time")
                .register(registry);
    }

    @Bean
    public Counter cacheHitCounter(MeterRegistry registry) {
        return Counter.builder("cache.hits.total")
                .description("Total cache hits")
                .tag("cache", "all")
                .register(registry);
    }

    @Bean
    public Counter cacheMissCounter(MeterRegistry registry) {
        return Counter.builder("cache.misses.total")
                .description("Total cache misses")
                .tag("cache", "all")
                .register(registry);
    }
}
