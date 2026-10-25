package com.ordersystem.dto;

import lombok.*;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationDto {
    private Long id;
    private Long userId;
    private Long orderId;
    private String orderNumber;
    private String type;
    private String title;
    private String message;
    private Boolean read;
    private Instant createdAt;
}
