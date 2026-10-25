package com.ordersystem.config;

import com.ordersystem.entity.FailedEvent;
import com.ordersystem.repository.FailedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;

@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaRetryConfig {

    private final FailedEventRepository failedEventRepository;

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        // 3 retries with 2 seconds interval, then send to DLQ
        FixedBackOff backOff = new FixedBackOff(2000L, 3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
            log.error("Permanently failed to process record: topic={} partition={} offset={} error={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());

            try {
                String stackTrace = getStackTrace(exception);
                String eventId = extractEventId(record);
                String orderId = extractOrderId(record);

                FailedEvent failedEvent = FailedEvent.builder()
                        .eventId(eventId)
                        .orderId(orderId)
                        .topic(record.topic() + ".DLT")
                        .consumerName("unknown-consumer")
                        .payload(record.value() != null ? record.value().toString() : "null")
                        .exceptionMessage(exception.getMessage())
                        .stackTrace(stackTrace)
                        .retryCount(3)
                        .correlationId("unknown")
                        .build();

                failedEventRepository.save(failedEvent);
                log.info("Saved failed event to DLQ table: eventId={}", eventId);
            } catch (Exception e) {
                log.error("Failed to save to DLQ table: {}", e.getMessage(), e);
            }
        }, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record topic={} offset={} error={}",
                        deliveryAttempt, record.topic(), record.offset(), ex.getMessage())
        );

        return errorHandler;
    }

    private String getStackTrace(Exception ex) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String trace = sw.toString();
        return trace.length() > 4000 ? trace.substring(0, 4000) : trace;
    }

    private String extractEventId(ConsumerRecord<?, ?> record) {
        try {
            if (record.value() instanceof String str) {
                // Try to parse JSON to get eventId
                if (str.contains("eventId")) {
                    int start = str.indexOf("eventId");
                    // Simplistic extraction
                    return "extracted-" + System.currentTimeMillis();
                }
            }
            // For object payloads, try reflection
            Object value = record.value();
            if (value != null) {
                var method = value.getClass().getMethod("getEventId");
                Object eventId = method.invoke(value);
                if (eventId != null) return eventId.toString();
            }
        } catch (Exception e) {
            // ignore
        }
        return "unknown-" + Instant.now().toEpochMilli();
    }

    private String extractOrderId(ConsumerRecord<?, ?> record) {
        try {
            Object value = record.value();
            if (value != null) {
                var method = value.getClass().getMethod("getOrderId");
                Object orderId = method.invoke(value);
                if (orderId != null) return orderId.toString();
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }
}
