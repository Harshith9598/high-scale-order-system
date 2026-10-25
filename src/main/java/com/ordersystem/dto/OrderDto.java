package com.ordersystem.dto;

import com.ordersystem.enums.OrderStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderDto {
    private Long id;
    private String orderNumber;
    private Long userId;
    private OrderStatus status;
    private BigDecimal totalAmount;
    private String currency;
    private String correlationId;

    @NotEmpty(message = "Order must have at least one item")
    @Valid
    private List<OrderItemDto> items;

    private Instant createdAt;
    private Instant updatedAt;
    private Long version;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderItemDto {
        private Long id;
        private Long productId;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
        private BigDecimal totalPrice;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CreateOrderRequest {
        @NotEmpty(message = "Order must have at least one item")
        @Valid
        private List<OrderItemRequest> items;

        @Getter
        @Setter
        @NoArgsConstructor
        @AllArgsConstructor
        @Builder
        public static class OrderItemRequest {
            @NotNull
            private Long productId;
            @NotNull
            @Min(1)
            private Integer quantity;
        }
    }
}
