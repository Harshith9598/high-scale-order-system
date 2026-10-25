package com.ordersystem.dto;

import com.ordersystem.enums.PaymentStatus;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentDto {
    private Long id;
    private Long orderId;
    private String orderNumber;
    private String paymentIdExternal;
    private BigDecimal amount;
    private String currency;
    private PaymentStatus status;
    private String failureReason;
    private Instant createdAt;
    private Instant updatedAt;
}
