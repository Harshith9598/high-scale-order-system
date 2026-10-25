package com.ordersystem.service;

import com.ordersystem.dto.ProductDto;
import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.mapper.ProductMapper;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ProductServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private ProductMapper productMapper;

    private ProductService productService;

    @BeforeEach
    void setUp() {
        productService = new ProductService(productRepository, inventoryRepository, productMapper);
    }

    @Test
    void testGetProductByIdSuccess() {
        Product product = Product.builder().id(1L).name("Test Product").price(new BigDecimal("99.99")).active(true).build();
        ProductDto dto = ProductDto.builder().id(1L).name("Test Product").price(new BigDecimal("99.99")).build();
        Inventory inventory = Inventory.builder().product(product).quantityAvailable(50).build();

        when(productRepository.findById(1L)).thenReturn(Optional.of(product));
        when(productMapper.toDto(product)).thenReturn(dto);
        when(inventoryRepository.findByProductId(1L)).thenReturn(Optional.of(inventory));

        ProductDto result = productService.getProductById(1L);

        assertNotNull(result);
        assertEquals("Test Product", result.getName());
        assertEquals(50, result.getAvailableQuantity());
    }

    @Test
    void testGetProductNotFound() {
        when(productRepository.findById(99L)).thenReturn(Optional.empty());

        assertThrows(com.ordersystem.exception.ResourceNotFoundException.class,
                () -> productService.getProductById(99L));
    }

    @Test
    void testCreateProduct() {
        ProductDto input = ProductDto.builder().name("New Product").price(new BigDecimal("199.99")).category("Electronics").active(true).build();
        Product saved = Product.builder().id(1L).name("New Product").price(new BigDecimal("199.99")).category("Electronics").active(true).build();
        ProductDto output = ProductDto.builder().id(1L).name("New Product").price(new BigDecimal("199.99")).build();

        when(productRepository.save(any(Product.class))).thenReturn(saved);
        when(productMapper.toDto(saved)).thenReturn(output);
        when(inventoryRepository.save(any(Inventory.class))).thenReturn(mock(Inventory.class));

        ProductDto result = productService.createProduct(input);

        assertNotNull(result);
        assertEquals(1L, result.getId());
        verify(productRepository).save(any(Product.class));
    }
}
