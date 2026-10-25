package com.ordersystem.enums;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    /** Retries exhausted; needs manual intervention. Never polled again. */
    DEAD
}
