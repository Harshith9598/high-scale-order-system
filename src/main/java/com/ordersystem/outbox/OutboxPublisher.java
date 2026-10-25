package com.ordersystem.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.OutboxEvent;
import com.ordersystem.enums.OutboxStatus;
import com.ordersystem.kafka.event.*;
import com.ordersystem.kafka.producer.EventProducer;
import com.ordersystem.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox table and publishes to Kafka.
 *
 * <p>Delivery is <b>at-least-once</b>: if this process crashes after the Kafka
 * send but before marking the row {@code COMPLETED}, the event is published
 * again with the <i>same</i> {@code eventId} (it lives in the stored payload),
 * and consumers dedupe on {@code (eventId, consumerName)}. Rows left in
 * {@code PROCESSING} by a crash become eligible again after
 * {@code app.outbox.processing-timeout} instead of being stuck forever.</p>
 *
 * <p>There is deliberately no transaction spanning the batch or the Kafka
 * call: each state transition is its own short repository transaction, so a
 * slow broker never holds a pooled DB connection open.</p>
 *
 * <p>Single-instance assumption: two instances polling concurrently can both
 * publish the same row (same {@code eventId} ⇒ consumers dedupe). Claim with
 * {@code SELECT … FOR UPDATE SKIP LOCKED} before running more than one
 * instance in production.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final Map<String, Class<? extends BaseEvent>> EVENT_TYPES = Map.of(
            "OrderCreated", OrderCreatedEvent.class,
            "InventoryReserved", InventoryReservedEvent.class,
            "InventoryFailed", InventoryReservedEvent.class,
            "PaymentProcessed", PaymentProcessedEvent.class,
            "PaymentFailed", PaymentProcessedEvent.class,
            "OrderCompleted", OrderCompletedEvent.class,
            "NotificationCreated", NotificationEvent.class);

    private final OutboxEventRepository outboxEventRepository;
    private final EventProducer eventProducer;
    private final ObjectMapper objectMapper;

    @Value("${app.outbox.batch-size:100}")
    private int batchSize;

    @Value("${app.outbox.max-retries:10}")
    private int maxRetries;

    @Value("${app.outbox.processing-timeout:PT5M}")
    private Duration processingTimeout;

    @Scheduled(fixedDelayString = "${app.outbox.polling-interval-ms:5000}")
    public void publishPendingEvents() {
        Instant now = Instant.now();
        List<OutboxEvent> due = outboxEventRepository.findDueEvents(now, PageRequest.of(0, batchSize));
        if (due.isEmpty()) {
            return;
        }
        log.info("Found {} outbox events to publish", due.size());
        for (OutboxEvent event : due) {
            try {
                publishOne(event);
            } catch (Exception unexpected) {
                // One poisoned row must not abort the rest of the batch; the
                // row stays PENDING/PROCESSING and is retried next cycle.
                log.error("Unexpected error publishing outbox event id={}", event.getId(), unexpected);
            }
        }
    }

    private void publishOne(OutboxEvent event) {
        event.setStatus(OutboxStatus.PROCESSING);
        // Lease: if we crash before completing, the row becomes due again
        // once this timestamp passes instead of being stuck in PROCESSING.
        event.setNextRetryAt(Instant.now().plus(processingTimeout));
        // NOTE: keep the returned (merged) instance. These saves happen in
        // separate repository transactions on a detached entity with
        // @Version; reusing the pre-merge instance carries a stale version
        // and the next save fails with OptimisticLockingFailureException.
        event = outboxEventRepository.save(event);
        try {
            Class<? extends BaseEvent> type = EVENT_TYPES.get(event.getEventType());
            if (type == null) {
                throw new IllegalStateException("Unknown outbox eventType: " + event.getEventType());
            }
            BaseEvent payload = objectMapper.readValue(event.getPayload(), type);
            eventProducer.publish(event.getTopic(), event.getAggregateId(), payload)
                    .get(10, TimeUnit.SECONDS);

            event.setStatus(OutboxStatus.COMPLETED);
            event.setProcessedAt(Instant.now());
            event.setErrorMessage(null);
            event = outboxEventRepository.save(event);
            log.info("Outbox event published id={} type={} topic={}",
                    event.getId(), event.getEventType(), event.getTopic());
        } catch (Exception e) {
            int retries = event.getRetryCount() + 1;
            event.setRetryCount(retries);
            String msg = e.getMessage();
            event.setErrorMessage(msg != null && msg.length() > 1000 ? msg.substring(0, 1000) : msg);
            if (retries >= maxRetries) {
                event.setStatus(OutboxStatus.DEAD);
                log.error("Outbox event DEAD after {} attempts id={} type={} topic={}: {}",
                        retries, event.getId(), event.getEventType(), event.getTopic(), msg);
            } else {
                event.setStatus(OutboxStatus.FAILED);
                // Exponential backoff capped at 30 minutes.
                long delaySeconds = Math.min(1800, 30L * (1L << Math.min(retries, 6)));
                event.setNextRetryAt(Instant.now().plusSeconds(delaySeconds));
                log.warn("Outbox publish failed (attempt {}/{}) id={}: {}",
                        retries, maxRetries, event.getId(), msg);
            }
            event = outboxEventRepository.save(event);
        }
    }
}
