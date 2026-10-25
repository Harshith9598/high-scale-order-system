package com.ordersystem.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.entity.Order;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.kafka.event.InventoryReservedEvent;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.kafka.producer.EventProducer;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.InventoryService;
import com.ordersystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
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
    private final EventProducer eventProducer;
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
                                   @Header(KafkaHeaders.OFFSET) long offset,
                                   Acknowledgment ack) {
        String eventId = event.getEventId() != null ? event.getEventId() : UUID.randomUUID().toString();
        log.info("Received OrderCreatedEvent: eventId={} orderId={} topic={} partition={} offset={} correlationId={}",
                eventId, event.getOrderId(), topic, partition, offset, event.getCorrelationId());

        try {
            // Idempotency check
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME, event.getOrderId().toString())) {
                log.info("Skipping duplicate event: eventId={}", eventId);
                ack.acknowledge();
                return;
            }

            // Process inventory reservation
            boolean allReserved = true;
            String failureReason = null;

            for (OrderCreatedEvent.OrderItem item : event.getItems()) {
                try {
                    boolean reserved = inventoryService.reserveInventory(item.getProductId(), item.getQuantity());
                    if (!reserved) {
                        allReserved = false;
                        failureReason = "Failed to reserve inventory for product " + item.getProductId();
                        break;
                    }
                } catch (Exception e) {
                    allReserved = false;
                    failureReason = e.getMessage();
                    log.error("Inventory reservation failed for product {}: {}", item.getProductId(), e.getMessage());
                    break;
                }
            }

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

                eventProducer.publishSync(inventoryReservedTopic, event.getOrderId().toString(), reservedEvent);
                log.info("Inventory reserved for order {}", event.getOrderNumber());
            } else {
                // Rollback any partial reservations
                for (OrderCreatedEvent.OrderItem item : event.getItems()) {
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

                eventProducer.publishSync(inventoryFailedTopic, event.getOrderId().toString(), failedEvent);
                log.warn("Inventory reservation failed for order {}: {}", event.getOrderNumber(), failureReason);
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing OrderCreatedEvent: eventId={} error={}", eventId, e.getMessage(), e);
            // Don't acknowledge - will be retried
            throw new RuntimeException("Failed to process inventory for order " + event.getOrderId(), e);
        }
    }
}
