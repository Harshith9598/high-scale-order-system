package com.ordersystem.service;

import com.ordersystem.dto.NotificationDto;
import com.ordersystem.entity.Notification;
import com.ordersystem.entity.Order;
import com.ordersystem.entity.User;
import com.ordersystem.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final NotificationRepository notificationRepository;

    @Transactional
    public Notification createNotification(User user, Order order, String type, String title, String message, String correlationId) {
        Notification notification = Notification.builder()
                .user(user)
                .order(order)
                .type(type)
                .title(title)
                .message(message)
                .read(false)
                .correlationId(correlationId)
                .build();
        notification = notificationRepository.save(notification);
        log.info("Notification created: id={} userId={} type={} order={}", notification.getId(), user.getId(), type, order != null ? order.getOrderNumber() : "N/A");
        return notification;
    }

    @Transactional(readOnly = true)
    public Page<NotificationDto> getUserNotifications(Long userId, Pageable pageable) {
        return notificationRepository.findByUserId(userId, pageable).map(this::toDto);
    }

    @Transactional(readOnly = true)
    public long getUnreadCount(Long userId) {
        return notificationRepository.countByUserIdAndReadFalse(userId);
    }

    @Transactional
    public void markAsRead(Long notificationId, Long userId) {
        notificationRepository.findById(notificationId).ifPresent(n -> {
            if (n.getUser().getId().equals(userId)) {
                n.setRead(true);
                notificationRepository.save(n);
            }
        });
    }

    private NotificationDto toDto(Notification n) {
        return NotificationDto.builder()
                .id(n.getId())
                .userId(n.getUser().getId())
                .orderId(n.getOrder() != null ? n.getOrder().getId() : null)
                .orderNumber(n.getOrder() != null ? n.getOrder().getOrderNumber() : null)
                .type(n.getType())
                .title(n.getTitle())
                .message(n.getMessage())
                .read(n.getRead())
                .createdAt(n.getCreatedAt())
                .build();
    }
}
