package com.ordersystem.kafka.consumer;

import com.ordersystem.entity.Order;
import com.ordersystem.entity.User;
import com.ordersystem.kafka.event.NotificationEvent;
import com.ordersystem.kafka.event.OrderCompletedEvent;
import com.ordersystem.kafka.event.PaymentProcessedEvent;
import com.ordersystem.kafka.event.InventoryReservedEvent;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.service.IdempotencyService;
import com.ordersystem.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationService notificationService;
    private final OrderRepository orderRepository;
    private final UserRepository userRepository;
    private final IdempotencyService idempotencyService;

    private static final String CONSUMER_NAME = "notification-consumer";

    @KafkaListener(topics = "${app.kafka.topics.order-completed}", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handleOrderCompleted(@Payload OrderCompletedEvent event,
                                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                     Acknowledgment ack) {
        String eventId = event.getEventId();
        log.info("NotificationConsumer received OrderCompleted: eventId={} orderId={}", eventId, event.getOrderId());

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME, event.getOrderId().toString())) {
                ack.acknowledge();
                return;
            }

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            User user = userRepository.findById(event.getUserId()).orElse(null);

            if (order != null && user != null) {
                notificationService.createNotification(
                        user,
                        order,
                        "ORDER_COMPLETED",
                        "Order Completed",
                        String.format("Your order %s has been completed successfully!", order.getOrderNumber()),
                        event.getCorrelationId()
                );
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing notification for order completed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-failed}", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handlePaymentFailed(@Payload PaymentProcessedEvent event, Acknowledgment ack) {
        String eventId = event.getEventId();
        log.info("NotificationConsumer received PaymentFailed: eventId={} orderId={}", eventId, event.getOrderId());

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME + "-payment-failed", event.getOrderId().toString())) {
                ack.acknowledge();
                return;
            }

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            User user = userRepository.findById(event.getUserId()).orElse(null);

            if (order != null && user != null) {
                notificationService.createNotification(
                        user,
                        order,
                        "PAYMENT_FAILED",
                        "Payment Failed",
                        String.format("Payment failed for order %s: %s", order.getOrderNumber(), event.getFailureReason()),
                        event.getCorrelationId()
                );
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing payment failed notification: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "${app.kafka.topics.inventory-failed}", groupId = "notification-group", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void handleInventoryFailed(@Payload InventoryReservedEvent event, Acknowledgment ack) {
        String eventId = event.getEventId();
        log.info("NotificationConsumer received InventoryFailed: eventId={} orderId={}", eventId, event.getOrderId());

        try {
            if (!idempotencyService.tryMarkProcessed(eventId, CONSUMER_NAME + "-inventory-failed", event.getOrderId().toString())) {
                ack.acknowledge();
                return;
            }

            Order order = orderRepository.findById(event.getOrderId()).orElse(null);
            User user = userRepository.findById(event.getUserId()).orElse(null);

            if (order != null && user != null) {
                notificationService.createNotification(
                        user,
                        order,
                        "INVENTORY_FAILED",
                        "Order Failed - Out of Stock",
                        String.format("Order %s failed: %s", order.getOrderNumber(), event.getFailureReason()),
                        event.getCorrelationId()
                );
            }

            ack.acknowledge();
        } catch (Exception e) {
            log.error("Error processing inventory failed notification: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
