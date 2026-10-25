package com.ordersystem.service;

import com.ordersystem.dto.InventoryDto;
import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Caching;
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

    /**
     * Attempts to reserve stock with a single atomic conditional UPDATE
     * ({@code ... WHERE quantity_available >= :quantity}), which cannot
     * oversell under concurrency. Returns {@code false} when stock is
     * insufficient — that is an expected business outcome, not an error.
     * Callers running inside a transaction must be able to react to it
     * (compensate, mark the order failed); throwing here would mark the
     * caller's transaction rollback-only and destroy that handling.
     *
     * @throws ResourceNotFoundException if no inventory row exists at all
     */
    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "inventory", key = "#productId"),
            @CacheEvict(value = "product", key = "#productId"),
            @CacheEvict(value = "products", allEntries = true)
    })
    public boolean reserveInventory(Long productId, int quantity) {
        int updated = inventoryRepository.reserveInventory(productId, quantity);
        if (updated == 0) {
            Inventory inv = inventoryRepository.findByProductId(productId)
                    .orElseThrow(() -> new ResourceNotFoundException("Inventory not found for product: " + productId));
            log.info("Insufficient inventory for product {}: available={} requested={}",
                    productId, inv.getQuantityAvailable(), quantity);
            return false;
        }
        log.info("Reserved {} units for product {}", quantity, productId);
        return true;
    }

    /** Compensation for a previously successful reservation. Returns false when nothing was released. */
    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "inventory", key = "#productId"),
            @CacheEvict(value = "product", key = "#productId"),
            @CacheEvict(value = "products", allEntries = true)
    })
    public boolean releaseInventory(Long productId, int quantity) {
        int updated = inventoryRepository.releaseInventory(productId, quantity);
        if (updated > 0) {
            log.info("Released {} units for product {}", quantity, productId);
            return true;
        }
        log.warn("Release of {} units for product {} matched no reservation", quantity, productId);
        return false;
    }

    /** Finalises a reservation after successful payment. Returns false when no reservation existed. */
    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "inventory", key = "#productId"),
            @CacheEvict(value = "product", key = "#productId"),
            @CacheEvict(value = "products", allEntries = true)
    })
    public boolean confirmReservation(Long productId, int quantity) {
        int updated = inventoryRepository.confirmReservation(productId, quantity);
        if (updated > 0) {
            log.info("Confirmed reservation of {} units for product {}", quantity, productId);
            return true;
        }
        log.warn("Confirm of {} units for product {} matched no reservation", quantity, productId);
        return false;
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
