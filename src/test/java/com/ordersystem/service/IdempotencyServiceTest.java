package com.ordersystem.service;

import com.ordersystem.entity.IdempotencyRecord;
import com.ordersystem.repository.IdempotencyRecordRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
        when(repository.save(any(IdempotencyRecord.class))).thenAnswer(i -> i.getArgument(0));

        boolean result = service.tryMarkProcessed("event-1", "consumer-a", "order-1");
        assertTrue(result);
        verify(repository).save(any(IdempotencyRecord.class));
    }

    @Test
    void testDuplicateEventShouldBeSkipped() {
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-a")).thenReturn(true);

        boolean result = service.tryMarkProcessed("event-1", "consumer-a", "order-1");
        assertFalse(result);
        verify(repository, never()).save(any());
    }

    @Test
    void testSameEventDifferentConsumerShouldBeProcessed() {
        when(repository.existsByEventIdAndConsumerName("event-1", "consumer-b")).thenReturn(false);
        when(repository.save(any(IdempotencyRecord.class))).thenAnswer(i -> i.getArgument(0));

        boolean result = service.tryMarkProcessed("event-1", "consumer-b", "order-1");
        assertTrue(result);
    }
}
