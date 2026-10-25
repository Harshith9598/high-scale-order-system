package com.ordersystem.controller;

import com.ordersystem.entity.FailedEvent;
import com.ordersystem.repository.FailedEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/failed-events")
@RequiredArgsConstructor
@Tag(name = "Failed Events", description = "DLQ inspection APIs (Admin only)")
@SecurityRequirement(name = "bearerAuth")
@PreAuthorize("hasRole('ADMIN')")
public class FailedEventController {

    private final FailedEventRepository failedEventRepository;

    @Operation(summary = "Get all failed events")
    @GetMapping
    public ResponseEntity<Page<FailedEvent>> getFailedEvents(@PageableDefault(size = 20) Pageable pageable) {
        return ResponseEntity.ok(failedEventRepository.findByOrderByCreatedAtDesc(pageable));
    }

    @Operation(summary = "Get failed events by topic")
    @GetMapping("/topic/{topic}")
    public ResponseEntity<Page<FailedEvent>> getByTopic(@PathVariable String topic, Pageable pageable) {
        return ResponseEntity.ok(failedEventRepository.findByTopic(topic, pageable));
    }

    @Operation(summary = "Get failed events by order ID")
    @GetMapping("/order/{orderId}")
    public ResponseEntity<Page<FailedEvent>> getByOrderId(@PathVariable String orderId, Pageable pageable) {
        return ResponseEntity.ok(failedEventRepository.findByOrderId(orderId, pageable));
    }
}
