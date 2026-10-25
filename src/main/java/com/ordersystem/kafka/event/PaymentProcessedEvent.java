package com.ordersystem.kafka.event;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class PaymentProcessedEvent extends BaseEvent {
    private Long orderId;
    private String orderNumber;
    private Long userId;
    private Long paymentId;
    private BigDecimal amount;
    private boolean success;
    private String failureReason;
}
