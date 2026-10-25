package com.ordersystem.service;

import com.ordersystem.dto.ProductDto;
import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.mapper.ProductMapper;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;
    private final InventoryRepository inventoryRepository;
    private final ProductMapper productMapper;

    @Transactional(readOnly = true)
    @Cacheable(value = "products", key = "#category + '-' + #search + '-' + #pageable.pageNumber + '-' + #pageable.pageSize")
    public Page<ProductDto> getProducts(String category, String search, Pageable pageable) {
        Page<Product> products = productRepository.findByFilters(category, search, pageable);
        return products.map(product -> {
            Optional<Inventory> inv = inventoryRepository.findByProductId(product.getId());
            ProductDto dto = productMapper.toDto(product);
            dto.setAvailableQuantity(inv.map(Inventory::getQuantityAvailable).orElse(0));
            return dto;
        });
    }

    @Transactional(readOnly = true)
    @Cacheable(value = "product", key = "#id")
    public ProductDto getProductById(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with id: " + id));
        ProductDto dto = productMapper.toDto(product);
        inventoryRepository.findByProductId(id).ifPresent(inv -> dto.setAvailableQuantity(inv.getQuantityAvailable()));
        return dto;
    }

    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "products", allEntries = true),
            @CacheEvict(value = "product", key = "#result.id")
    })
    public ProductDto createProduct(ProductDto productDto) {
        Product product = Product.builder()
                .name(productDto.getName())
                .description(productDto.getDescription())
                .price(productDto.getPrice())
                .category(productDto.getCategory())
                .active(productDto.getActive() != null ? productDto.getActive() : true)
                .build();

        product = productRepository.save(product);

        // Create inventory with 0 quantity initially
        Inventory inventory = Inventory.builder()
                .product(product)
                .quantityAvailable(0)
                .quantityReserved(0)
                .lowStockThreshold(10)
                .build();
        inventoryRepository.save(inventory);

        log.info("Product created: id={} name={}", product.getId(), product.getName());

        ProductDto dto = productMapper.toDto(product);
        dto.setAvailableQuantity(0);
        return dto;
    }

    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "products", allEntries = true),
            @CacheEvict(value = "product", key = "#id")
    })
    public ProductDto updateProduct(Long id, ProductDto productDto) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with id: " + id));

        product.setName(productDto.getName());
        product.setDescription(productDto.getDescription());
        product.setPrice(productDto.getPrice());
        product.setCategory(productDto.getCategory());
        if (productDto.getActive() != null) product.setActive(productDto.getActive());

        product = productRepository.save(product);
        log.info("Product updated: id={}", product.getId());

        ProductDto dto = productMapper.toDto(product);
        inventoryRepository.findByProductId(id).ifPresent(inv -> dto.setAvailableQuantity(inv.getQuantityAvailable()));
        return dto;
    }

    @Transactional
    @Caching(evict = {
            @CacheEvict(value = "products", allEntries = true),
            @CacheEvict(value = "product", key = "#id")
    })
    public void deleteProduct(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with id: " + id));
        product.setActive(false);
        productRepository.save(product);
        log.info("Product soft-deleted: id={}", id);
    }
}
