package com.ordersystem.kafka.producer;

import com.ordersystem.kafka.event.BaseEvent;
import com.ordersystem.util.CorrelationIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

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

    public CompletableFuture<SendResult<String, Object>> publishObject(String topic, String key, Object payload) {
        log.info("Publishing object payload to topic={} key={}", topic, key);
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, payload);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish object to topic={}: {}", topic, ex.getMessage(), ex);
            } else {
                log.info("Object published to topic={} partition={} offset={}",
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
        return future;
    }

    public <T extends BaseEvent> void publishSync(String topic, String key, T event) {
        try {
            publish(topic, key, event).get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish event synchronously: " + e.getMessage(), e);
        }
    }

    public void publishObjectSync(String topic, String key, Object payload) {
        try {
            publishObject(topic, key, payload).get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish object synchronously: " + e.getMessage(), e);
        }
    }
}
