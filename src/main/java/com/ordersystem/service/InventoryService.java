package com.ordersystem.service;

import com.ordersystem.dto.InventoryDto;
import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.exception.InsufficientInventoryException;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final ProductRepository productRepository;

    @Transactional(readOnly = true)
    @Cacheable(value = "inventory", key = "#productId")
    public InventoryDto getInventory(Long productId) {
        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new ResourceNotFoundException("Inventory not found for product: " + productId));
        return toDto(inventory);
    }

    @Transactional
    @CacheEvict(value = "inventory", key = "#productId")
    public InventoryDto updateInventory(Long productId, InventoryDto.UpdateInventoryRequest request) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found: " + productId));

        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseGet(() -> Inventory.builder()
                        .product(product)
                        .quantityAvailable(0)
                        .quantityReserved(0)
                        .build());

        inventory.setQuantityAvailable(request.getQuantityAvailable());
        if (request.getLowStockThreshold() != null) {
            inventory.setLowStockThreshold(request.getLowStockThreshold());
        }

        inventory = inventoryRepository.save(inventory);
        log.info("Inventory updated for product {}: available={}", productId, inventory.getQuantityAvailable());
        return toDto(inventory);
    }

    @Transactional
    @CacheEvict(value = "inventory", key = "#productId")
    public boolean reserveInventory(Long productId, int quantity) {
        // Atomic update to prevent overselling
        int updated = inventoryRepository.reserveInventory(productId, quantity);
        if (updated == 0) {
            // Check if product exists and inventory insufficient
            Inventory inv = inventoryRepository.findByProductId(productId)
                    .orElseThrow(() -> new ResourceNotFoundException("Inventory not found for product: " + productId));
            if (inv.getQuantityAvailable() < quantity) {
                throw new InsufficientInventoryException(
                        String.format("Insufficient inventory for product %d: available=%d, requested=%d",
                                productId, inv.getQuantityAvailable(), quantity));
            }
            return false;
        }
        log.info("Reserved {} units for product {}", quantity, productId);
        return true;
    }

    @Transactional
    @CacheEvict(value = "inventory", key = "#productId")
    public void releaseInventory(Long productId, int quantity) {
        int updated = inventoryRepository.releaseInventory(productId, quantity);
        if (updated > 0) {
            log.info("Released {} units for product {}", quantity, productId);
        }
    }

    @Transactional
    @CacheEvict(value = "inventory", key = "#productId")
    public void confirmReservation(Long productId, int quantity) {
        int updated = inventoryRepository.confirmReservation(productId, quantity);
        if (updated > 0) {
            log.info("Confirmed reservation of {} units for product {}", quantity, productId);
        }
    }

    private InventoryDto toDto(Inventory inv) {
        return InventoryDto.builder()
                .id(inv.getId())
                .productId(inv.getProduct().getId())
                .productName(inv.getProduct().getName())
                .quantityAvailable(inv.getQuantityAvailable())
                .quantityReserved(inv.getQuantityReserved())
                .totalQuantity(inv.getTotalQuantity())
                .lowStockThreshold(inv.getLowStockThreshold())
                .lowStock(inv.isLowStock())
                .updatedAt(inv.getUpdatedAt())
                .build();
    }
}
