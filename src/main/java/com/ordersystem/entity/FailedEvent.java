package com.ordersystem.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "failed_events", indexes = {
        @Index(name = "idx_failed_events_event_id", columnList = "event_id"),
        @Index(name = "idx_failed_events_topic", columnList = "topic"),
        @Index(name = "idx_failed_events_created_at", columnList = "created_at")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FailedEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "order_id", length = 100)
    private String orderId;

    @Column(name = "topic", nullable = false, length = 200)
    private String topic;

    @Column(name = "consumer_name", nullable = false, length = 100)
    private String consumerName;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "exception_message", length = 1000)
    private String exceptionMessage;

    @Column(name = "stack_trace", columnDefinition = "TEXT")
    private String stackTrace;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount;

    @Column(name = "correlation_id", length = 100)
    private String correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
    }
}
