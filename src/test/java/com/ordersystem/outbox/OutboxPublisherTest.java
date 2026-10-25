package com.ordersystem.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ordersystem.entity.OutboxEvent;
import com.ordersystem.enums.OutboxStatus;
import com.ordersystem.kafka.event.BaseEvent;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.kafka.producer.EventProducer;
import com.ordersystem.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock private OutboxEventRepository repository;
    @Mock private EventProducer eventProducer;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        lenient().when(repository.save(any(OutboxEvent.class))).thenAnswer(inv -> inv.getArgument(0));
        lenient().when(repository.claim(any(), any(), any(Instant.class), any(Instant.class))).thenReturn(1);
        publisher = new OutboxPublisher(repository, eventProducer, mapper);
        ReflectionTestUtils.setField(publisher, "batchSize", 100);
        ReflectionTestUtils.setField(publisher, "maxRetries", 3);
        ReflectionTestUtils.setField(publisher, "processingTimeout", Duration.ofMinutes(5));
    }

    private OutboxEvent pendingOrderCreated(int retryCount) throws Exception {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId("evt-1").correlationId("corr-1").timestamp(Instant.now())
                .eventType("OrderCreated").orderId(42L).orderNumber("ORD-1").userId(7L)
                .items(List.of()).build();
        return OutboxEvent.builder()
                .id(1L).aggregateType("Order").aggregateId("42").eventType("OrderCreated")
                .payload(mapper.writeValueAsString(event)).topic("order.created")
                .status(OutboxStatus.PENDING).retryCount(retryCount)
                .correlationId("corr-1").createdAt(Instant.now()).build();
    }

    @Test
    void publishesTypedPayloadAndMarksCompleted() throws Exception {
        OutboxEvent row = pendingOrderCreated(0);
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of(row));
        when(eventProducer.publish(eq("order.created"), eq("42"), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishPendingEvents();

        // The published payload must be the concrete event (with its stable
        // eventId), not a Map — consumers rely on the type header + eventId.
        ArgumentCaptor<BaseEvent> payload = ArgumentCaptor.forClass(BaseEvent.class);
        verify(eventProducer).publish(eq("order.created"), eq("42"), payload.capture());
        assertInstanceOf(OrderCreatedEvent.class, payload.getValue());
        assertEquals("evt-1", ((OrderCreatedEvent) payload.getValue()).getEventId());
        assertEquals(OutboxStatus.COMPLETED, row.getStatus());
        assertNotNull(row.getProcessedAt());
    }

    @Test
    void publishFailureSchedulesRetryWithBackoff() throws Exception {
        OutboxEvent row = pendingOrderCreated(0);
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of(row));
        when(eventProducer.publish(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPendingEvents();

        assertEquals(OutboxStatus.FAILED, row.getStatus());
        assertEquals(1, row.getRetryCount());
        assertTrue(row.getNextRetryAt().isAfter(Instant.now()), "retry must be in the future");
        assertTrue(row.getErrorMessage().contains("broker down"));
    }

    @Test
    void exhaustedRetriesMoveEventToDead() throws Exception {
        OutboxEvent row = pendingOrderCreated(2); // maxRetries = 3 -> this failure is the last
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of(row));
        when(eventProducer.publish(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        publisher.publishPendingEvents();

        assertEquals(OutboxStatus.DEAD, row.getStatus());
        assertEquals(3, row.getRetryCount());
    }

    @Test
    void unknownEventTypeFailsInsteadOfPublishingGarbage() {
        OutboxEvent row = OutboxEvent.builder()
                .id(9L).aggregateType("Order").aggregateId("1").eventType("Nope")
                .payload("{}").topic("order.created").status(OutboxStatus.PENDING)
                .retryCount(0).createdAt(Instant.now()).build();
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of(row));

        publisher.publishPendingEvents();

        verify(eventProducer, never()).publish(anyString(), anyString(), any(BaseEvent.class));
        assertEquals(OutboxStatus.FAILED, row.getStatus());
    }

    @Test
    void losingTheClaimRaceSkipsPublish() throws Exception {
        // Two publisher instances race: claim() is one conditional UPDATE, so
        // the loser gets 0 and must not publish or mutate the row.
        OutboxEvent row = pendingOrderCreated(0);
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of(row));
        when(repository.claim(any(), any(), any(Instant.class), any(Instant.class))).thenReturn(0);

        publisher.publishPendingEvents();

        verify(eventProducer, never()).publish(anyString(), anyString(), any(BaseEvent.class));
        assertEquals(OutboxStatus.PENDING, row.getStatus());
    }

    @Test
    void nothingDueDoesNothing() {
        when(repository.findDueEvents(any(Instant.class), any(Pageable.class))).thenReturn(List.of());
        publisher.publishPendingEvents();
        verify(eventProducer, never()).publish(anyString(), anyString(), any(BaseEvent.class));
    }
}
