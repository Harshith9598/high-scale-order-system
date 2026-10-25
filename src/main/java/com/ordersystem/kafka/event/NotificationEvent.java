package com.ordersystem.kafka.event;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class NotificationEvent extends BaseEvent {
    private Long orderId;
    private String orderNumber;
    private Long userId;
    private String type;
    private String title;
    private String message;
}
