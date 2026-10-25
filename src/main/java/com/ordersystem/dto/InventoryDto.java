package com.ordersystem.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.*;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryDto {
    private Long id;
    private Long productId;
    private String productName;
    private Integer quantityAvailable;
    private Integer quantityReserved;
    private Integer totalQuantity;
    private Integer lowStockThreshold;
    private Boolean lowStock;
    private Instant updatedAt;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class UpdateInventoryRequest {
        @NotNull
        @Min(0)
        private Integer quantityAvailable;

        @Min(0)
        private Integer lowStockThreshold;
    }
}
