package com.ordersystem.controller;

import com.ordersystem.dto.PaymentDto;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.service.PaymentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Tag(name = "Payments", description = "Payment APIs")
@SecurityRequirement(name = "bearerAuth")
public class PaymentController {

    private final PaymentService paymentService;
    private final UserRepository userRepository;

    @Operation(summary = "Get payment by order ID (owner or ADMIN)")
    @GetMapping("/{orderId}")
    public ResponseEntity<PaymentDto> getPayment(@PathVariable Long orderId, Authentication authentication) {
        var user = userRepository.findByUsername(authentication.getName())
                .orElseThrow(() -> new ResourceNotFoundException("Authenticated user not found"));
        boolean isAdmin = authentication.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_ADMIN"));
        return ResponseEntity.ok(paymentService.getPaymentByOrderId(orderId, user.getId(), isAdmin));
    }
}
