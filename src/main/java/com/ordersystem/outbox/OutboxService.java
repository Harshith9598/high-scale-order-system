package com.ordersystem.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.OutboxEvent;
import com.ordersystem.enums.OutboxStatus;
import com.ordersystem.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OutboxEvent saveEvent(String aggregateType, String aggregateId, String eventType,
                                 Object payload, String topic, String correlationId) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .payload(jsonPayload)
                    .topic(topic)
                    .status(OutboxStatus.PENDING)
                    .correlationId(correlationId)
                    .build();
            event = outboxEventRepository.save(event);
            log.info("Outbox event saved: id={} type={} aggregate={}/{}", event.getId(), eventType, aggregateType, aggregateId);
            return event;
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize outbox event payload", e);
        }
    }

    @Transactional
    public void markCompleted(Long eventId) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.setStatus(OutboxStatus.COMPLETED);
            event.setProcessedAt(java.time.Instant.now());
            outboxEventRepository.save(event);
        });
    }

    @Transactional
    public void markFailed(Long eventId, String errorMessage) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.setStatus(OutboxStatus.FAILED);
            event.setErrorMessage(errorMessage);
            event.setRetryCount(event.getRetryCount() + 1);
            event.setNextRetryAt(java.time.Instant.now().plusSeconds((long) Math.pow(2, event.getRetryCount()) * 60));
            outboxEventRepository.save(event);
        });
    }
}
