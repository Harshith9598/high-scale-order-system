package com.ordersystem.controller;

import com.ordersystem.dto.NotificationDto;
import com.ordersystem.entity.User;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.service.NotificationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
@Tag(name = "Notifications", description = "Notification APIs")
@SecurityRequirement(name = "bearerAuth")
public class NotificationController {

    private final NotificationService notificationService;
    private final UserRepository userRepository;

    @Operation(summary = "Get user notifications")
    @GetMapping
    public ResponseEntity<Page<NotificationDto>> getNotifications(
            @PageableDefault(size = 20) Pageable pageable,
            Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        Page<NotificationDto> notifications = notificationService.getUserNotifications(user.getId(), pageable);
        return ResponseEntity.ok(notifications);
    }

    @Operation(summary = "Get unread count")
    @GetMapping("/unread-count")
    public ResponseEntity<Long> getUnreadCount(Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        long count = notificationService.getUnreadCount(user.getId());
        return ResponseEntity.ok(count);
    }
}
