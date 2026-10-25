package com.ordersystem.validation;

import com.ordersystem.dto.OrderDto;
import com.ordersystem.dto.ProductDto;
import com.ordersystem.dto.RegisterRequest;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RequestValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void shortPasswordAndBadEmailRejected() {
        RegisterRequest r = new RegisterRequest();
        r.setUsername("ab");
        r.setEmail("not-an-email");
        r.setPassword("short");
        var violations = validator.validate(r);
        assertTrue(violations.size() >= 3, violations.toString());
    }

    @Test
    void emptyOrderAndNonPositiveQuantityRejected() {
        OrderDto.CreateOrderRequest empty = new OrderDto.CreateOrderRequest();
        empty.setItems(List.of());
        assertFalse(validator.validate(empty).isEmpty());

        OrderDto.CreateOrderRequest zeroQty = new OrderDto.CreateOrderRequest();
        var item = new OrderDto.CreateOrderRequest.OrderItemRequest();
        item.setProductId(1L);
        item.setQuantity(0);
        zeroQty.setItems(List.of(item));
        assertFalse(validator.validate(zeroQty).isEmpty());
    }

    @Test
    void nonPositivePriceRejectedValidProductPasses() {
        ProductDto bad = ProductDto.builder().name("X").price(BigDecimal.ZERO).build();
        assertFalse(validator.validate(bad).isEmpty());
        ProductDto ok = ProductDto.builder().name("Widget").price(new BigDecimal("9.99")).build();
        assertTrue(validator.validate(ok).isEmpty());
    }
}
