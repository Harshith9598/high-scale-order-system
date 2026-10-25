package com.ordersystem.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.OutboxEvent;
import com.ordersystem.enums.OutboxStatus;
import com.ordersystem.kafka.producer.EventProducer;
import com.ordersystem.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;

    @Value("${app.outbox.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${app.outbox.polling-interval-ms:5000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findByStatusAndNextRetryAtBeforeOrderByCreatedAtAsc(OutboxStatus.PENDING, Instant.now());

        if (pendingEvents.isEmpty()) {
            // Also check failed events that are due for retry
            pendingEvents = outboxEventRepository
                    .findByStatusAndNextRetryAtBeforeOrderByCreatedAtAsc(OutboxStatus.FAILED, Instant.now());
        }

        if (pendingEvents.isEmpty()) return;

        log.info("Found {} outbox events to publish", pendingEvents.size());

        for (OutboxEvent event : pendingEvents.stream().limit(batchSize).toList()) {
            try {
                event.setStatus(OutboxStatus.PROCESSING);
                outboxEventRepository.save(event);

                Object payload = objectMapper.readValue(event.getPayload(), Object.class);

                // Publish the original payload directly
                CompletableFuture<SendResult<String, Object>> future =
                        eventProducer.publishObject(event.getTopic(), event.getAggregateId(), payload);

                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish outbox event id={}: {}", event.getId(), ex.getMessage());
                        markFailed(event.getId(), ex.getMessage());
                    } else {
                        log.info("Outbox event published id={} topic={}", event.getId(), event.getTopic());
                        markCompleted(event.getId());
                    }
                });

                // For simplicity in scheduled task, we wait synchronously for small batches
                // In production, you'd handle async with proper error handling
                try {
                    future.get();
                    event.setStatus(OutboxStatus.COMPLETED);
                    event.setProcessedAt(Instant.now());
                    outboxEventRepository.save(event);
                } catch (Exception e) {
                    log.error("Sync publish failed for outbox event id={}", event.getId(), e);
                    event.setStatus(OutboxStatus.FAILED);
                    event.setErrorMessage(e.getMessage());
                    event.setRetryCount(event.getRetryCount() + 1);
                    event.setNextRetryAt(Instant.now().plusSeconds((long) Math.pow(2, event.getRetryCount()) * 30));
                    outboxEventRepository.save(event);
                }

            } catch (Exception e) {
                log.error("Error processing outbox event id={}", event.getId(), e);
                markFailed(event.getId(), e.getMessage());
            }
        }
    }

    private void markCompleted(Long eventId) {
        outboxEventRepository.findById(eventId).ifPresent(e -> {
            e.setStatus(OutboxStatus.COMPLETED);
            e.setProcessedAt(Instant.now());
            outboxEventRepository.save(e);
        });
    }

    private void markFailed(Long eventId, String error) {
        outboxEventRepository.findById(eventId).ifPresent(e -> {
            e.setStatus(OutboxStatus.FAILED);
            e.setErrorMessage(error);
            e.setRetryCount(e.getRetryCount() + 1);
            e.setNextRetryAt(Instant.now().plusSeconds((long) Math.pow(2, e.getRetryCount()) * 30));
            outboxEventRepository.save(e);
        });
    }

    // Generic wrapper to publish raw JSON payload
    public static class GenericOutboxPayload {
        public String aggregateType;
        public String aggregateId;
        public String eventType;
        public Object payload;
        public String correlationId;
        public String eventId;

        public GenericOutboxPayload() {}

        public GenericOutboxPayload(OutboxEvent outboxEvent, Object payload) {
            this.aggregateType = outboxEvent.getAggregateType();
            this.aggregateId = outboxEvent.getAggregateId();
            this.eventType = outboxEvent.getEventType();
            this.payload = payload;
            this.correlationId = outboxEvent.getCorrelationId();
            this.eventId = outboxEvent.getId().toString();
        }

        public String getAggregateType() { return aggregateType; }
        public String getAggregateId() { return aggregateId; }
        public String getEventType() { return eventType; }
        public Object getPayload() { return payload; }
        public String getCorrelationId() { return correlationId; }
        public String getEventId() { return eventId; }
    }
}
