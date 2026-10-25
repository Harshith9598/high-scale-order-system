package com.ordersystem.kafka.event;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public abstract class BaseEvent {
    private String eventId;
    private String correlationId;
    private Instant timestamp;
    private String eventType;

    public void ensureDefaults() {
        if (eventId == null) eventId = java.util.UUID.randomUUID().toString();
        if (correlationId == null) correlationId = java.util.UUID.randomUUID().toString();
        if (timestamp == null) timestamp = Instant.now();
    }
}
