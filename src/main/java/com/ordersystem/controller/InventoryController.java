package com.ordersystem.controller;

import com.ordersystem.dto.InventoryDto;
import com.ordersystem.service.InventoryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
@Tag(name = "Inventory", description = "Inventory management APIs")
@SecurityRequirement(name = "bearerAuth")
public class InventoryController {

    private final InventoryService inventoryService;

    @Operation(summary = "Get inventory for product")
    @GetMapping("/{productId}")
    public ResponseEntity<InventoryDto> getInventory(@PathVariable Long productId) {
        InventoryDto inventory = inventoryService.getInventory(productId);
        return ResponseEntity.ok(inventory);
    }

    @Operation(summary = "Update inventory (Admin only)")
    @PutMapping("/{productId}")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<InventoryDto> updateInventory(
            @PathVariable Long productId,
            @Valid @RequestBody InventoryDto.UpdateInventoryRequest request) {
        InventoryDto updated = inventoryService.updateInventory(productId, request);
        return ResponseEntity.ok(updated);
    }
}
