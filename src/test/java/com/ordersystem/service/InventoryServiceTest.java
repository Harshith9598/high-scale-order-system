package com.ordersystem.service;

import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock private InventoryRepository inventoryRepository;
    @Mock private ProductRepository productRepository;
    private InventoryService service;

    @BeforeEach
    void setUp() {
        service = new InventoryService(inventoryRepository, productRepository);
    }

    @Test
    void reserveReturnsTrueWhenAtomicUpdateSucceeds() {
        when(inventoryRepository.reserveInventory(1L, 2)).thenReturn(1);
        assertTrue(service.reserveInventory(1L, 2));
    }

    @Test
    void reserveReturnsFalseOnInsufficientStockWithoutThrowing() {
        // Regression: throwing here marked the Kafka consumer's transaction
        // rollback-only and destroyed its INVENTORY_FAILED handling.
        when(inventoryRepository.reserveInventory(1L, 5)).thenReturn(0);
        when(inventoryRepository.findByProductId(1L)).thenReturn(Optional.of(
                Inventory.builder().product(Product.builder().id(1L).build())
                        .quantityAvailable(2).quantityReserved(0).build()));
        assertFalse(service.reserveInventory(1L, 5));
    }

    @Test
    void reserveThrowsNotFoundWhenNoInventoryRowExists() {
        when(inventoryRepository.reserveInventory(99L, 1)).thenReturn(0);
        when(inventoryRepository.findByProductId(99L)).thenReturn(Optional.empty());
        assertThrows(com.ordersystem.exception.ResourceNotFoundException.class,
                () -> service.reserveInventory(99L, 1));
    }

    @Test
    void releaseAndConfirmReportWhetherARowWasUpdated() {
        when(inventoryRepository.releaseInventory(1L, 1)).thenReturn(0);
        when(inventoryRepository.confirmReservation(1L, 1)).thenReturn(1);
        assertFalse(service.releaseInventory(1L, 1));
        assertTrue(service.confirmReservation(1L, 1));
    }
}
