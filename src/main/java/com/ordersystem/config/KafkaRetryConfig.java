package com.ordersystem.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.FailedEvent;
import com.ordersystem.kafka.event.BaseEvent;
import com.ordersystem.repository.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Retry + dead-letter handling for all listeners (wired explicitly in
 * {@code KafkaConfig.kafkaListenerContainerFactory}).
 *
 * <p>Retryable failures are retried in-memory with exponential backoff
 * (1s → 2s → 4s, max 3 retries). After exhaustion the record is published to
 * {@code <topic>.DLT} (same partition) via {@link DeadLetterPublishingRecoverer}
 * <i>and</i> persisted to {@code failed_events} for inspection through the
 * admin API. Malformed records (deserialization errors, events without an
 * {@code eventId}) are non-retryable and go straight to the DLT.</p>
 *
 * <p>Note: in-memory retry blocks the partition during backoff — acceptable at
 * this scale and preserves per-partition ordering; a retry-topic topology is
 * the alternative when head-of-line blocking matters.</p>
 */
@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaRetryConfig {

    private final FailedEventRepository failedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        // partition -1 = let the producer choose. The .DLT topics have a
        // single partition; copying the source partition (0..2) would target
        // partitions that do not exist and block on metadata timeout.
        DeadLetterPublishingRecoverer dltRecoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        dltRecoverer.setVerifyPartition(false);
        dltRecoverer.setWaitForSendResultTimeout(java.time.Duration.ofSeconds(10));

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
            // 1) Publish to <topic>.DLT (headers carry exception metadata).
            try {
                dltRecoverer.accept(record, exception);
            } catch (Exception dltEx) {
                log.error("Failed to publish to DLT for topic={} offset={}: {}",
                        record.topic(), record.offset(), dltEx.getMessage(), dltEx);
            }
            // 2) Persist for admin inspection, even if the DLT publish failed.
            persistFailedEvent(record, exception);
        }, backOff);

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,       // e.g. event without eventId
                DeserializationException.class,
                ClassCastException.class);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for topic={} partition={} offset={} error={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));
        return errorHandler;
    }

    private void persistFailedEvent(ConsumerRecord<?, ?> record, Exception exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        try {
            Object value = record.value();
            BaseEvent event = value instanceof BaseEvent be ? be : null;
            Long orderId = extractOrderId(value);

            String payload;
            try {
                payload = value == null ? "null" : objectMapper.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                payload = String.valueOf(value);
            }

            FailedEvent failedEvent = FailedEvent.builder()
                    .eventId(event != null && event.getEventId() != null
                            ? event.getEventId()
                            : record.topic() + "-" + record.partition() + "-" + record.offset())
                    .orderId(orderId != null ? orderId.toString() : null)
                    .topic(record.topic() + ".DLT")
                    .consumerName("kafka-listener:" + record.topic())
                    .payload(payload)
                    .exceptionMessage(truncate(root.getClass().getSimpleName() + ": " + root.getMessage(), 1000))
                    .stackTrace(stackTrace(exception))
                    .retryCount(3)
                    .correlationId(event != null ? event.getCorrelationId() : null)
                    .build();
            failedEventRepository.save(failedEvent);
            log.error("Record dead-lettered: topic={} partition={} offset={} eventId={} error={}",
                    record.topic(), record.partition(), record.offset(),
                    failedEvent.getEventId(), exception.getMessage());
        } catch (Exception e) {
            log.error("Failed to persist dead-lettered record topic={} offset={}: {}",
                    record.topic(), record.offset(), e.getMessage(), e);
        }
    }

    private Long extractOrderId(Object value) {
        try {
            if (value == null) return null;
            Object id = value.getClass().getMethod("getOrderId").invoke(value);
            return id instanceof Number n ? n.longValue() : null;
        } catch (Exception e) {
            return null;
        }
    }

    private String stackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return truncate(sw.toString(), 4000);
    }

    private String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() > max ? s.substring(0, max) : s;
    }
}
