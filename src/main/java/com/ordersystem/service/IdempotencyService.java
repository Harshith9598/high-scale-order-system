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

    /**
     * Atomically claims (eventId, consumerName) for processing.
     *
     * <p>Must be called inside the same transaction as the business effect, so the
     * claim and the effect commit or roll back together. Returns {@code false}
     * when this consumer has already processed the event (safe to skip).</p>
     *
     * <p>{@code saveAndFlush} forces the unique constraint to be checked now
     * rather than at commit time. A concurrent duplicate that loses the race
     * raises {@link DataIntegrityViolationException}; we let it propagate so the
     * message is retried — on the retry the winning record exists and the plain
     * {@code exists} check returns {@code false} cleanly. Swallowing it here
     * would leave the surrounding transaction marked rollback-only.</p>
     *
     * @throws IllegalArgumentException if {@code eventId} is missing — an event
     *                                  without a stable id cannot be deduplicated
     *                                  and must be treated as malformed (the Kafka
     *                                  error handler classifies it non-retryable).
     */
    @Transactional
    public boolean tryMarkProcessed(String eventId, String consumerName, String aggregateId) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException(
                    "Event without eventId cannot be processed idempotently (consumer=" + consumerName + ")");
        }
        if (isProcessed(eventId, consumerName)) {
            log.info("Duplicate event detected: eventId={} consumer={}", eventId, consumerName);
            return false;
        }
        IdempotencyRecord record = IdempotencyRecord.builder()
                .eventId(eventId)
                .consumerName(consumerName)
                .aggregateId(aggregateId)
                .build();
        idempotencyRecordRepository.saveAndFlush(record);
        log.info("Marked event as processed: eventId={} consumer={}", eventId, consumerName);
        return true;
    }
}
