package com.ordersystem.service;

import com.ordersystem.entity.IdempotencyRecord;
import com.ordersystem.repository.IdempotencyRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyRecordRepository idempotencyRecordRepository;

    @Transactional(readOnly = true)
    public boolean isProcessed(String eventId, String consumerName) {
        return idempotencyRecordRepository.existsByEventIdAndConsumerName(eventId, consumerName);
    }

    @Transactional
    public boolean tryMarkProcessed(String eventId, String consumerName, String aggregateId) {
        if (isProcessed(eventId, consumerName)) {
            log.info("Duplicate event detected: eventId={} consumer={}", eventId, consumerName);
            return false;
        }
        try {
            IdempotencyRecord record = IdempotencyRecord.builder()
                    .eventId(eventId)
                    .consumerName(consumerName)
                    .aggregateId(aggregateId)
                    .build();
            idempotencyRecordRepository.save(record);
            log.info("Marked event as processed: eventId={} consumer={}", eventId, consumerName);
            return true;
        } catch (DataIntegrityViolationException e) {
            // Race condition: another thread already inserted
            log.warn("Race condition on idempotency check: eventId={} consumer={}", eventId, consumerName);
            return false;
        }
    }
}
