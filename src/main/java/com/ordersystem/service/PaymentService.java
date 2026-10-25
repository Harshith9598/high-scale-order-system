package com.ordersystem.service;

import com.ordersystem.dto.PaymentDto;
import com.ordersystem.entity.Order;
import com.ordersystem.entity.Payment;
import com.ordersystem.enums.PaymentStatus;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final Random random = new Random();

    @Value("${app.payment.failure-rate:0.05}")
    private double failureRate;

    @Value("${app.payment.processing-delay-ms:100}")
    private long processingDelayMs;

    @Transactional
    public Payment processPayment(Order order, String correlationId) {
        // Simulate processing delay
        try {
            Thread.sleep(processingDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Payment existing = paymentRepository.findByOrderId(order.getId()).orElse(null);
        if (existing != null && existing.getStatus() == PaymentStatus.COMPLETED) {
            log.info("Payment already completed for order {}", order.getOrderNumber());
            return existing;
        }

        boolean shouldFail = random.nextDouble() < failureRate;

        Payment payment = existing;
        if (payment == null) {
            payment = Payment.builder()
                    .order(order)
                    .amount(order.getTotalAmount())
                    .currency(order.getCurrency())
                    .correlationId(correlationId)
                    .build();
        }

        if (shouldFail) {
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Simulated payment failure (random failure injection)");
            payment.setPaymentIdExternal("PAY-FAIL-" + UUID.randomUUID().toString().substring(0, 8));
            log.warn("Payment failed for order {}: simulated failure", order.getOrderNumber());
        } else {
            payment.setStatus(PaymentStatus.COMPLETED);
            payment.setPaymentIdExternal("PAY-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase());
            payment.setFailureReason(null);
            log.info("Payment completed for order {}: amount={}", order.getOrderNumber(), order.getTotalAmount());
        }

        return paymentRepository.save(payment);
    }

    @Transactional
    public Payment processPaymentWithForcedFailure(Order order, String correlationId, String reason) {
        Payment payment = paymentRepository.findByOrderId(order.getId()).orElse(
                Payment.builder()
                        .order(order)
                        .amount(order.getTotalAmount())
                        .currency(order.getCurrency())
                        .correlationId(correlationId)
                        .build()
        );
        payment.setStatus(PaymentStatus.FAILED);
        payment.setFailureReason(reason);
        payment.setPaymentIdExternal("PAY-FAIL-" + UUID.randomUUID().toString().substring(0, 8));
        return paymentRepository.save(payment);
    }

    @Transactional(readOnly = true)
    public PaymentDto getPaymentByOrderId(Long orderId) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Payment not found for order: " + orderId));
        return toDto(payment);
    }

    @Transactional(readOnly = true)
    public PaymentDto getPaymentByOrderNumber(String orderNumber) {
        Payment payment = paymentRepository.findByOrderOrderNumber(orderNumber)
                .orElseThrow(() -> new ResourceNotFoundException("Payment not found for order: " + orderNumber));
        return toDto(payment);
    }

    private PaymentDto toDto(Payment payment) {
        return PaymentDto.builder()
                .id(payment.getId())
                .orderId(payment.getOrder().getId())
                .orderNumber(payment.getOrder().getOrderNumber())
                .paymentIdExternal(payment.getPaymentIdExternal())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .status(payment.getStatus())
                .failureReason(payment.getFailureReason())
                .createdAt(payment.getCreatedAt())
                .updatedAt(payment.getUpdatedAt())
                .build();
    }
}
