package com.ordersystem.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.Order;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.kafka.event.InventoryReservedEvent;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.metrics.OrderMetrics;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.InventoryService;
import com.ordersystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryConsumer {

    private final InventoryService inventoryService;
    private final OrderRepository orderRepository;
    private final IdempotencyService idempotencyService;
    private final OutboxService outboxService;
    private final OrderMetrics orderMetrics;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.inventory-reserved}")
    private String inventoryReservedTopic;

    @Value("${app.kafka.topics.inventory-failed}")
    private String inventoryFailedTopic;

    private static final String CONSUMER_NAME = "inventory-consumer";

    @KafkaListener(topics = "${app.kafka.topics.order-created}", groupId = "inventory-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handleOrderCreated(@Payload OrderCreatedEvent event,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                   @Header(KafkaHeaders.OFFSET) long offset) {
        String eventId = event.getEventId();
        log.info("Received OrderCreatedEvent: eventId={} orderId={} topic={} partition={} offset={} correlationId={}",
                eventId, event.getOrderId(), topic, partition, offset, event.getCorrelationId());

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME, event.getOrderId().toString())) {
                log.info("Skipping duplicate event: eventId={}", eventId);
                return;
            }

            // Reserve line by line, remembering exactly what succeeded so
            // compensation below releases only THIS order's reservations.
            java.util.List<OrderCreatedEvent.OrderItem> reservedItems = new java.util.ArrayList<>();
            String failureReason = null;
            for (OrderCreatedEvent.OrderItem item : event.getItems()) {
                if (inventoryService.reserveInventory(item.getProductId(), item.getQuantity())) {
                    reservedItems.add(item);
                } else {
                    failureReason = "Insufficient inventory for product " + item.getProductId();
                    break;
                }
            }
            boolean allReserved = failureReason == null;

            if (allReserved) {
                // Update order status
                Order order = orderRepository.findById(event.getOrderId()).orElse(null);
                if (order != null) {
                    if (order.getStatus().canTransitionTo(OrderStatus.INVENTORY_RESERVED)) {
                        order.setStatus(OrderStatus.INVENTORY_RESERVED);
                        orderRepository.save(order);
                    }
                    // Transition to PAYMENT_PENDING
                    if (order.getStatus().canTransitionTo(OrderStatus.PAYMENT_PENDING)) {
                        order.setStatus(OrderStatus.PAYMENT_PENDING);
                        orderRepository.save(order);
                    }
                }

                InventoryReservedEvent reservedEvent = InventoryReservedEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .correlationId(event.getCorrelationId())
                        .timestamp(Instant.now())
                        .eventType("InventoryReserved")
                        .orderId(event.getOrderId())
                        .orderNumber(event.getOrderNumber())
                        .userId(event.getUserId())
                        .success(true)
                        .build();

outboxService.saveEvent("Order", event.getOrderId().toString(), "InventoryReserved",
                        reservedEvent, inventoryReservedTopic, event.getCorrelationId());
                log.info("Inventory reserved for order {}", event.getOrderNumber());
            } else {
                // Compensate ONLY the lines this order actually reserved
                // (releasing everything could hand other orders' reservations back).
                for (OrderCreatedEvent.OrderItem item : reservedItems) {
                    try {
                        inventoryService.releaseInventory(item.getProductId(), item.getQuantity());
                    } catch (Exception ex) {
                        log.warn("Failed to rollback inventory for product {}: {}", item.getProductId(), ex.getMessage());
                    }
                }

                Order order = orderRepository.findById(event.getOrderId()).orElse(null);
                if (order != null && order.getStatus().canTransitionTo(OrderStatus.INVENTORY_FAILED)) {
                    order.setStatus(OrderStatus.INVENTORY_FAILED);
                    orderRepository.save(order);
                    orderMetrics.orderFailed();
                }

                InventoryReservedEvent failedEvent = InventoryReservedEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .correlationId(event.getCorrelationId())
                        .timestamp(Instant.now())
                        .eventType("InventoryFailed")
                        .orderId(event.getOrderId())
                        .orderNumber(event.getOrderNumber())
                        .userId(event.getUserId())
                        .success(false)
                        .failureReason(failureReason)
                        .build();

outboxService.saveEvent("Order", event.getOrderId().toString(), "InventoryFailed",
                        failedEvent, inventoryFailedTopic, event.getCorrelationId());
                log.warn("Inventory reservation failed for order {}: {}", event.getOrderNumber(), failureReason);
            }

            // Offset is committed by the container (AckMode.RECORD) only after
            // this method returns and the transaction has committed.
        } catch (Exception e) {
            log.error("Error processing OrderCreatedEvent: eventId={} error={}", eventId, e.getMessage(), e);
            throw new RuntimeException("Failed to process inventory for order " + event.getOrderId(), e);
        }
    }
}
