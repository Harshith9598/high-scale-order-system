package com.ordersystem.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.Order;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.kafka.consumer.InventoryConsumer;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.metrics.OrderMetrics;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryConsumerTest {

    @Mock private InventoryService inventoryService;
    @Mock private OrderRepository orderRepository;
    @Mock private IdempotencyService idempotencyService;
    @Mock private OutboxService outboxService;
    @Mock private OrderMetrics orderMetrics;

    private InventoryConsumer consumer;
    private Order order;

    @BeforeEach
    void setUp() {
        consumer = new InventoryConsumer(inventoryService, orderRepository,
                idempotencyService, outboxService, orderMetrics, new ObjectMapper());
        order = Order.builder().id(42L).orderNumber("ORD-42").status(OrderStatus.CREATED).build();
    }

    private OrderCreatedEvent event() {
        return OrderCreatedEvent.builder()
                .eventId("evt-1").correlationId("corr-1").timestamp(Instant.now())
                .eventType("OrderCreated").orderId(42L).orderNumber("ORD-42").userId(7L)
                .items(List.of(
                        OrderCreatedEvent.OrderItem.builder().productId(1L).quantity(2)
                                .unitPrice(BigDecimal.TEN).build(),
                        OrderCreatedEvent.OrderItem.builder().productId(2L).quantity(1)
                                .unitPrice(BigDecimal.TEN).build()))
                .build();
    }

    @Test
    void duplicateDeliveryIsSkippedWithNoSideEffects() {
        when(idempotencyService.tryMarkProcessed("evt-1", "inventory-consumer", "42")).thenReturn(false);
        consumer.handleOrderCreated(event(), "order.created", 0, 0L);
        verify(inventoryService, never()).reserveInventory(anyLong(), anyInt());
        verify(outboxService, never()).saveEvent(any(), any(), any(), any(), any(), any());
    }

    @Test
    void partialFailureCompensatesOnlyWhatWasActuallyReserved() {
        // Regression: the old rollback released EVERY line, which could hand
        // back other orders' reservations for lines never reserved here.
        when(idempotencyService.tryMarkProcessed("evt-1", "inventory-consumer", "42")).thenReturn(true);
        when(inventoryService.reserveInventory(1L, 2)).thenReturn(true);
        when(inventoryService.reserveInventory(2L, 1)).thenReturn(false); // insufficient
        when(inventoryService.releaseInventory(1L, 2)).thenReturn(true);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        consumer.handleOrderCreated(event(), "order.created", 0, 1L);

        verify(inventoryService).releaseInventory(1L, 2);
        verify(inventoryService, never()).releaseInventory(2L, 1);
        assertEquals(OrderStatus.INVENTORY_FAILED, order.getStatus());
        verify(outboxService).saveEvent(eq("Order"), eq("42"), eq("InventoryFailed"),
                any(), any(), eq("corr-1"));
        verify(outboxService, never()).saveEvent(any(), any(), eq("InventoryReserved"),
                any(), any(), any());
    }

    @Test
    void allReservedPublishesReservedEventViaOutbox() {
        when(idempotencyService.tryMarkProcessed("evt-1", "inventory-consumer", "42")).thenReturn(true);
        when(inventoryService.reserveInventory(anyLong(), anyInt())).thenReturn(true);
        when(orderRepository.findById(42L)).thenReturn(Optional.of(order));

        consumer.handleOrderCreated(event(), "order.created", 0, 2L);

        assertEquals(OrderStatus.PAYMENT_PENDING, order.getStatus());
        verify(outboxService).saveEvent(eq("Order"), eq("42"), eq("InventoryReserved"),
                any(), any(), eq("corr-1"));
        verify(inventoryService, never()).releaseInventory(anyLong(), anyInt());
    }
}
