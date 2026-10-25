package com.ordersystem.dto;

import jakarta.validation.constraints.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductDto {
    private Long id;

    @NotBlank(message = "Product name is required")
    @Size(max = 200)
    private String name;

    @Size(max = 1000)
    private String description;

    @NotNull(message = "Price is required")
    @DecimalMin(value = "0.0", inclusive = false, message = "Price must be greater than 0")
    private BigDecimal price;

    @Size(max = 100)
    private String category;

    private Boolean active;
    private Instant createdAt;
    private Instant updatedAt;
    private Integer availableQuantity;
    private Long version;
}
