package com.ordersystem.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "idempotency_records",
        // Idempotency is scoped per consumer: the same event may legitimately be
        // processed once by EACH consumer group (fan-out). See Flyway V2.
        uniqueConstraints = @UniqueConstraint(name = "idx_idempotency_event_consumer",
                columnNames = {"event_id", "consumer_name"}),
        indexes = {
                @Index(name = "idx_idempotency_event_id", columnList = "event_id"),
                @Index(name = "idx_idempotency_created_at", columnList = "created_at")
        })
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class IdempotencyRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, length = 100)
    private String eventId;

    @Column(name = "consumer_name", nullable = false, length = 100)
    private String consumerName;

    @Column(name = "aggregate_id", length = 100)
    private String aggregateId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
        if (processedAt == null) processedAt = Instant.now();
    }
}
