package com.ordersystem.controller;

import com.ordersystem.dto.OrderDto;
import com.ordersystem.service.OrderService;
import com.ordersystem.security.CustomUserDetailsService;
import com.ordersystem.entity.User;
import com.ordersystem.repository.UserRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Tag(name = "Orders", description = "Order management APIs")
@SecurityRequirement(name = "bearerAuth")
public class OrderController {

    private final OrderService orderService;
    private final UserRepository userRepository;

    @Operation(summary = "Create a new order")
    @PostMapping
    public ResponseEntity<OrderDto> createOrder(
            @Valid @RequestBody OrderDto.CreateOrderRequest request,
            Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        OrderDto order = orderService.createOrder(user.getId(), request);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @Operation(summary = "Get order by ID")
    @GetMapping("/{id}")
    public ResponseEntity<OrderDto> getOrder(@PathVariable Long id, Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        boolean isAdmin = authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_ADMIN"));
        OrderDto order = orderService.getOrder(id, user.getId(), isAdmin);
        return ResponseEntity.ok(order);
    }

    @Operation(summary = "Get user orders")
    @GetMapping
    public ResponseEntity<Page<OrderDto>> getOrders(
            @PageableDefault(size = 20) Pageable pageable,
            Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        boolean isAdmin = authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_ADMIN"));

        Page<OrderDto> orders;
        if (isAdmin) {
            orders = orderService.getAllOrders(pageable);
        } else {
            orders = orderService.getUserOrders(user.getId(), pageable);
        }
        return ResponseEntity.ok(orders);
    }

    @Operation(summary = "Cancel order")
    @PostMapping("/{id}/cancel")
    public ResponseEntity<OrderDto> cancelOrder(@PathVariable Long id, Authentication authentication) {
        User user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new RuntimeException("User not found"));
        boolean isAdmin = authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_ADMIN"));
        OrderDto cancelled = orderService.cancelOrder(id, user.getId(), isAdmin);
        return ResponseEntity.ok(cancelled);
    }
}
