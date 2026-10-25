package com.ordersystem.kafka.consumer;

import com.ordersystem.entity.Order;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.kafka.event.OrderCompletedEvent;
import com.ordersystem.kafka.event.PaymentProcessedEvent;
import com.ordersystem.metrics.OrderMetrics;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.InventoryService;
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
public class OrderStatusConsumer {

    private final OrderRepository orderRepository;
    private final InventoryService inventoryService;
    private final IdempotencyService idempotencyService;
    private final OutboxService outboxService;
    private final OrderMetrics orderMetrics;

    @Value("${app.kafka.topics.order-completed}")
    private String orderCompletedTopic;

    @Value("${app.kafka.topics.notification-created}")
    private String notificationCreatedTopic;

    private static final String CONSUMER_NAME = "order-status-consumer";

    @KafkaListener(topics = {"${app.kafka.topics.payment-processed}", "${app.kafka.topics.payment-failed}"}, groupId = "order-status-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handlePaymentProcessed(@Payload PaymentProcessedEvent event,
                                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        String eventId = event.getEventId();
        log.info("OrderStatusConsumer received event: eventId={} orderId={} success={} topic={}",
                eventId, event.getOrderId(), event.isSuccess(), topic);

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME, event.getOrderId().toString())) {
                log.info("Skipping duplicate order status event: eventId={}", eventId);
                return;
            }

            Order order = orderRepository.findById(event.getOrderId())
                    .orElseThrow(() -> new RuntimeException("Order not found: " + event.getOrderId()));

            if (event.isSuccess()) {
                // Confirm inventory reservation (deduct reserved)
                var orderWithItems = orderRepository.findByIdWithItems(order.getId()).orElse(order);
                for (var item : orderWithItems.getItems()) {
                    // Do NOT swallow failures: completing the order with an
                    // un-confirmed reservation leaks reserved stock forever.
                    // Throwing rolls back and lets the error handler retry.
                    if (!inventoryService.confirmReservation(item.getProduct().getId(), item.getQuantity())) {
                        throw new IllegalStateException("No reservation to confirm for product "
                                + item.getProduct().getId() + " on order " + order.getOrderNumber());
                    }
                }

                if (order.getStatus().canTransitionTo(OrderStatus.COMPLETED)) {
                    order.setStatus(OrderStatus.COMPLETED);
                    orderRepository.save(order);
                    orderMetrics.orderCompleted();
                    log.info("Order completed: {}", order.getOrderNumber());

                    // Publish order completed event
                    OrderCompletedEvent completedEvent = OrderCompletedEvent.builder()
                            .eventId(UUID.randomUUID().toString())
                            .correlationId(event.getCorrelationId())
                            .timestamp(Instant.now())
                            .eventType("OrderCompleted")
                            .orderId(order.getId())
                            .orderNumber(order.getOrderNumber())
                            .userId(event.getUserId())
                            .build();

outboxService.saveEvent("Order", order.getId().toString(), "OrderCompleted",
                            completedEvent, orderCompletedTopic, event.getCorrelationId());
                }
            } else {
                if (order.getStatus().canTransitionTo(OrderStatus.FAILED)) {
                    order.setStatus(OrderStatus.FAILED);
                    orderRepository.save(order);
                    orderMetrics.orderFailed();
                    log.info("Order failed due to payment failure: {}", order.getOrderNumber());
                }
            }

        } catch (Exception e) {
            log.error("Error in order status consumer for order {}: {}", event.getOrderId(), e.getMessage(), e);
            throw new RuntimeException("Order status processing failed", e);
        }
    }

    @KafkaListener(topics = "${app.kafka.topics.inventory-failed}", groupId = "order-status-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handleInventoryFailed(@Payload com.ordersystem.kafka.event.InventoryReservedEvent event) {
        String eventId = event.getEventId();
        log.info("Received InventoryFailedEvent: eventId={} orderId={}", eventId, event.getOrderId());

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME + "-inventory-failed", event.getOrderId().toString())) {
                return;
            }

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            if (order != null && order.getStatus().canTransitionTo(OrderStatus.FAILED)) {
                order.setStatus(OrderStatus.FAILED);
                orderRepository.save(order);
                orderMetrics.orderFailed();
                log.info("Order marked as FAILED due to inventory failure: {}", order.getOrderNumber());
            }

        } catch (Exception e) {
            log.error("Error handling inventory failed for order {}: {}", event.getOrderId(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
