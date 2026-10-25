package com.ordersystem.config;

import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "app.seed-data.enabled", havingValue = "true", matchIfMissing = true)
public class DataInitializer {

    private final ProductRepository productRepository;
    private final InventoryRepository inventoryRepository;

    @Bean
    public CommandLineRunner initData() {
        return args -> {
            if (productRepository.count() > 0) {
                log.info("Products already exist, skipping initialization");
                return;
            }

            log.info("Initializing sample products...");

            List<Product> products = List.of(
                    Product.builder().name("Laptop Pro 16\"").description("High-performance laptop with M2 chip, 32GB RAM, 1TB SSD").price(new BigDecimal("2499.99")).category("Electronics").active(true).build(),
                    Product.builder().name("Wireless Headphones").description("Noise-cancelling over-ear headphones with 30h battery").price(new BigDecimal("299.99")).category("Electronics").active(true).build(),
                    Product.builder().name("Smartphone X").description("Latest flagship smartphone with 256GB storage").price(new BigDecimal("999.99")).category("Electronics").active(true).build(),
                    Product.builder().name("Coffee Maker Deluxe").description("Automatic coffee maker with grinder").price(new BigDecimal("199.99")).category("Home").active(true).build(),
                    Product.builder().name("Running Shoes").description("Lightweight running shoes for marathon").price(new BigDecimal("129.99")).category("Sports").active(true).build(),
                    Product.builder().name("Office Chair Ergonomic").description("Ergonomic office chair with lumbar support").price(new BigDecimal("349.99")).category("Furniture").active(true).build(),
                    Product.builder().name("4K Monitor 27\"").description("27-inch 4K UHD monitor with HDR").price(new BigDecimal("399.99")).category("Electronics").active(true).build(),
                    Product.builder().name("Mechanical Keyboard").description("RGB mechanical keyboard with Cherry MX switches").price(new BigDecimal("149.99")).category("Electronics").active(true).build(),
                    Product.builder().name("Yoga Mat Premium").description("Non-slip premium yoga mat").price(new BigDecimal("49.99")).category("Sports").active(true).build(),
                    Product.builder().name("Bluetooth Speaker").description("Portable waterproof Bluetooth speaker").price(new BigDecimal("89.99")).category("Electronics").active(true).build()
            );

            products = productRepository.saveAll(products);

            List<Inventory> inventories = products.stream().map(p -> Inventory.builder()
                    .product(p)
                    .quantityAvailable(100)
                    .quantityReserved(0)
                    .lowStockThreshold(10)
                    .build()).toList();

            inventoryRepository.saveAll(inventories);

            log.info("Initialized {} products with inventory", products.size());
        };
    }
}
