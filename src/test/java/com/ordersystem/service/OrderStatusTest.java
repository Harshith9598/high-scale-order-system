package com.ordersystem.service;

import com.ordersystem.enums.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class OrderStatusTest {

    @Test
    void testValidTransitions() {
        assertTrue(OrderStatus.CREATED.canTransitionTo(OrderStatus.INVENTORY_RESERVED));
        assertTrue(OrderStatus.CREATED.canTransitionTo(OrderStatus.CANCELLED));
        assertTrue(OrderStatus.INVENTORY_RESERVED.canTransitionTo(OrderStatus.PAYMENT_PENDING));
        assertTrue(OrderStatus.PAYMENT_PENDING.canTransitionTo(OrderStatus.PAYMENT_COMPLETED));
        assertTrue(OrderStatus.PAYMENT_COMPLETED.canTransitionTo(OrderStatus.PROCESSING));
        assertTrue(OrderStatus.PROCESSING.canTransitionTo(OrderStatus.COMPLETED));
    }

    @Test
    void testInvalidTransitions() {
        assertFalse(OrderStatus.CREATED.canTransitionTo(OrderStatus.COMPLETED));
        assertFalse(OrderStatus.COMPLETED.canTransitionTo(OrderStatus.CANCELLED));
        assertFalse(OrderStatus.CANCELLED.canTransitionTo(OrderStatus.CREATED));
        assertFalse(OrderStatus.FAILED.canTransitionTo(OrderStatus.COMPLETED));
        assertFalse(OrderStatus.COMPLETED.canTransitionTo(OrderStatus.PROCESSING));
    }

    @Test
    void testIsValidTransitionStatic() {
        assertTrue(OrderStatus.isValidTransition(OrderStatus.CREATED, OrderStatus.INVENTORY_RESERVED));
        assertFalse(OrderStatus.isValidTransition(OrderStatus.CREATED, OrderStatus.COMPLETED));
        assertTrue(OrderStatus.isValidTransition(null, OrderStatus.CREATED));
        assertFalse(OrderStatus.isValidTransition(null, OrderStatus.COMPLETED));
    }
}
