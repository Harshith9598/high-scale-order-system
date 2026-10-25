package com.ordersystem.kafka.event;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class InventoryReservedEvent extends BaseEvent {
    private Long orderId;
    private String orderNumber;
    private Long userId;
    private boolean success;
    private String failureReason;
}
