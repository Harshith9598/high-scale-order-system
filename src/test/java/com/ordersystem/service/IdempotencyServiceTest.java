package com.ordersystem.service;

import com.ordersystem.entity.IdempotencyRecord;
import com.ordersystem.repository.IdempotencyRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IdempotencyServiceTest {

    @Mock
    private IdempotencyRecordRepository repository;

    private IdempotencyService service;

    @BeforeEach
    void setUp() {
        service = new IdempotencyService(repository);
    }

    @Test
    void testFirstEventShouldBeProcessed() {
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-a")).thenReturn(false);
        when(repository.saveAndFlush(any(IdempotencyRecord.class))).thenAnswer(i -> i.getArgument(0));

        assertTrue(service.tryMarkProcessed("event-1", "consumer-a", "order-1"));
        verify(repository).saveAndFlush(any(IdempotencyRecord.class));
    }

    @Test
    void testDuplicateEventShouldBeSkipped() {
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-a")).thenReturn(true);

        assertFalse(service.tryMarkProcessed("event-1", "consumer-a", "order-1"));
        verify(repository, never()).saveAndFlush(any());
    }

    @Test
    void testSameEventDifferentConsumerShouldBeProcessed() {
        // Fan-out: uniqueness is (eventId, consumerName), not eventId alone.
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-b")).thenReturn(false);
        when(repository.saveAndFlush(any(IdempotencyRecord.class))).thenAnswer(i -> i.getArgument(0));

        assertTrue(service.tryMarkProcessed("event-1", "consumer-b", "order-1"));
    }

    @Test
    void testMissingEventIdIsRejected() {
        // An event with no stable id cannot be deduplicated; the Kafka error
        // handler treats IllegalArgumentException as non-retryable (-> DLT).
        assertThrows(IllegalArgumentException.class,
                () -> service.tryMarkProcessed(null, "consumer-a", "order-1"));
        assertThrows(IllegalArgumentException.class,
                () -> service.tryMarkProcessed("  ", "consumer-a", "order-1"));
        verify(repository, never()).saveAndFlush(any());
    }

    @Test
    void testConcurrentDuplicateRacePropagatesForRetry() {
        // Both claims pass the exists check; the loser hits the unique
        // constraint on flush. It must propagate (so the message is retried
        // and then cleanly skipped), not be swallowed into a rollback-only tx.
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-a")).thenReturn(false);
        when(repository.saveAndFlush(any(IdempotencyRecord.class)))
                .thenThrow(new DataIntegrityViolationException("unique (event_id, consumer_name)"));

        assertThrows(DataIntegrityViolationException.class,
                () -> service.tryMarkProcessed("event-1", "consumer-a", "order-1"));
    }
}
