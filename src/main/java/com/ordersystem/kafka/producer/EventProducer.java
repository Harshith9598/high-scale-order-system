package com.ordersystem.kafka.producer;

import com.ordersystem.kafka.event.BaseEvent;
import com.ordersystem.util.CorrelationIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka publisher used exclusively by the outbox publisher. Business code
 * must not call this directly inside a DB transaction: write an outbox row
 * instead ({@code OutboxService.saveEvent}) so the state change and the event
 * commit atomically and the {@code eventId} stays stable across republishes.
 * (The former publishSync/publishObject helpers were removed for exactly this
 * reason -- they invited dual-writes.)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public <T extends BaseEvent> CompletableFuture<SendResult<String, Object>> publish(String topic, String key, T event) {
        event.ensureDefaults();
        if (event.getCorrelationId() == null) {
            String cid = CorrelationIdUtil.getCorrelationId();
            if (cid != null) event.setCorrelationId(cid);
        }

        log.info("Publishing event type={} id={} to topic={} key={} correlationId={}",
                event.getEventType(), event.getEventId(), topic, key, event.getCorrelationId());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event id={} to topic={}: {}", event.getEventId(), topic, ex.getMessage(), ex);
            } else {
                log.info("Event published id={} to topic={} partition={} offset={}",
                        event.getEventId(), topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return future;
    }
}
