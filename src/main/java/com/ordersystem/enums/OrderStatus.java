package com.ordersystem.enums;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

public enum OrderStatus {
    CREATED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    PAYMENT_PENDING,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    PROCESSING,
    COMPLETED,
    CANCELLED,
    FAILED;

    private static final Map<OrderStatus, Set<OrderStatus>> VALID_TRANSITIONS = new EnumMap<>(OrderStatus.class);

    static {
        VALID_TRANSITIONS.put(CREATED, Set.of(INVENTORY_RESERVED, INVENTORY_FAILED, CANCELLED, FAILED));
        VALID_TRANSITIONS.put(INVENTORY_RESERVED, Set.of(PAYMENT_PENDING, CANCELLED, FAILED));
        VALID_TRANSITIONS.put(INVENTORY_FAILED, Set.of(FAILED, CANCELLED));
        VALID_TRANSITIONS.put(PAYMENT_PENDING, Set.of(PAYMENT_COMPLETED, PAYMENT_FAILED, CANCELLED, FAILED));
        VALID_TRANSITIONS.put(PAYMENT_COMPLETED, Set.of(PROCESSING, CANCELLED, FAILED));
        VALID_TRANSITIONS.put(PAYMENT_FAILED, Set.of(FAILED, CANCELLED));
        VALID_TRANSITIONS.put(PROCESSING, Set.of(COMPLETED, FAILED, CANCELLED));
        VALID_TRANSITIONS.put(COMPLETED, Set.of());
        VALID_TRANSITIONS.put(CANCELLED, Set.of());
        VALID_TRANSITIONS.put(FAILED, Set.of());
    }

    public boolean canTransitionTo(OrderStatus target) {
        return VALID_TRANSITIONS.getOrDefault(this, Set.of()).contains(target);
    }

    public static boolean isValidTransition(OrderStatus from, OrderStatus to) {
        if (from == null) return to == CREATED;
        return from.canTransitionTo(to);
    }
}
