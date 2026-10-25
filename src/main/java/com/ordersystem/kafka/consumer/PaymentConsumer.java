package com.ordersystem.kafka.consumer;

import com.ordersystem.entity.Order;
import com.ordersystem.entity.Payment;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.kafka.event.InventoryReservedEvent;
import com.ordersystem.kafka.event.PaymentProcessedEvent;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.InventoryService;
import com.ordersystem.service.PaymentService;
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
public class PaymentConsumer {

    private final PaymentService paymentService;
    private final OrderRepository orderRepository;
    private final InventoryService inventoryService;
    private final IdempotencyService idempotencyService;
    private final OutboxService outboxService;

    @Value("${app.kafka.topics.payment-processed}")
    private String paymentProcessedTopic;

    @Value("${app.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    private static final String CONSUMER_NAME = "payment-consumer";

    @KafkaListener(topics = "${app.kafka.topics.inventory-reserved}", groupId = "payment-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handleInventoryReserved(@Payload InventoryReservedEvent event,
                                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                        @Header(KafkaHeaders.OFFSET) long offset) {
        String eventId = event.getEventId();
        log.info("Received InventoryReservedEvent: eventId={} orderId={} success={} topic={} partition={} offset={}",
                eventId, event.getOrderId(), event.isSuccess(), topic, partition, offset);

        if (!event.isSuccess()) {
            log.info("Skipping payment for failed inventory reservation: orderId={}", event.getOrderId());
            return;
        }

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME, event.getOrderId().toString())) {
                log.info("Skipping duplicate payment event: eventId={}", eventId);
                return;
            }

            Order order = orderRepository.findById(event.getOrderId())
                    .orElseThrow(() -> new RuntimeException("Order not found: " + event.getOrderId()));

            Payment payment = paymentService.processPayment(order, event.getCorrelationId());

            boolean paymentSuccess = payment.getStatus().name().equals("COMPLETED");

            if (paymentSuccess) {
                if (order.getStatus().canTransitionTo(OrderStatus.PAYMENT_COMPLETED)) {
                    order.setStatus(OrderStatus.PAYMENT_COMPLETED);
                    orderRepository.save(order);
                }
                if (order.getStatus().canTransitionTo(OrderStatus.PROCESSING)) {
                    order.setStatus(OrderStatus.PROCESSING);
                    orderRepository.save(order);
                }
            } else {
                if (order.getStatus().canTransitionTo(OrderStatus.PAYMENT_FAILED)) {
                    order.setStatus(OrderStatus.PAYMENT_FAILED);
                    orderRepository.save(order);
                }
                // Release inventory on payment failure
                var orderWithItems = orderRepository.findByIdWithItems(order.getId()).orElse(order);
                for (var item : orderWithItems.getItems()) {
                    try {
                        inventoryService.releaseInventory(item.getProduct().getId(), item.getQuantity());
                    } catch (Exception ex) {
                        log.warn("Failed to release inventory on payment failure: {}", ex.getMessage());
                    }
                }
            }

            PaymentProcessedEvent paymentEvent = PaymentProcessedEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .correlationId(event.getCorrelationId())
                    .timestamp(Instant.now())
                    .eventType(paymentSuccess ? "PaymentProcessed" : "PaymentFailed")
                    .orderId(event.getOrderId())
                    .orderNumber(event.getOrderNumber())
                    .userId(event.getUserId())
                    .paymentId(payment.getId())
                    .amount(payment.getAmount())
                    .success(paymentSuccess)
                    .failureReason(payment.getFailureReason())
                    .build();

            String targetTopic = paymentSuccess ? paymentProcessedTopic : paymentFailedTopic;
            // Via the outbox, in this transaction: the payment row, the order
            // status, the idempotency claim and this event commit atomically.
            outboxService.saveEvent("Order", event.getOrderId().toString(),
                    paymentSuccess ? "PaymentProcessed" : "PaymentFailed",
                    paymentEvent, targetTopic, event.getCorrelationId());

            log.info("Payment {} for order {}", paymentSuccess ? "succeeded" : "failed", event.getOrderNumber());

        } catch (Exception e) {
            log.error("Error processing payment for order {}: {}", event.getOrderId(), e.getMessage(), e);
            throw new RuntimeException("Payment processing failed for order " + event.getOrderId(), e);
        }
    }
}
