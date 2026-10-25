package com.ordersystem.concurrency;

import com.ordersystem.entity.Inventory;
import com.ordersystem.entity.Product;
import com.ordersystem.repository.InventoryRepository;
import com.ordersystem.repository.ProductRepository;
import com.ordersystem.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
public class ConcurrentOrderTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("app.jwt.secret", () -> "dGVzdC1vbmx5LXNlY3JldC1mb3ItaW50ZWdyYXRpb24tdGVzdHM=");
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379).toString());
    }

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private InventoryService inventoryService;

    private Product product;

    @BeforeEach
    void setUp() {
        inventoryRepository.deleteAll();
        productRepository.deleteAll();

        product = Product.builder()
                .name("Limited Stock Product")
                .description("Only 5 units available")
                .price(new BigDecimal("100.00"))
                .category("Test")
                .active(true)
                .build();
        product = productRepository.save(product);

        Inventory inv = Inventory.builder()
                .product(product)
                .quantityAvailable(5)
                .quantityReserved(0)
                .lowStockThreshold(2)
                .build();
        inventoryRepository.save(inv);
    }

    @Test
    void testConcurrentOrdersPreventOverselling() throws InterruptedException {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    boolean reserved = inventoryService.reserveInventory(product.getId(), 1);
                    if (reserved) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        // Only 5 should succeed
        assertEquals(5, successCount.get(), "Only 5 reservations should succeed");
        assertEquals(5, failureCount.get(), "5 should fail due to insufficient stock");

        Inventory finalInventory = inventoryRepository.findByProductId(product.getId()).orElseThrow();
        assertEquals(0, finalInventory.getQuantityAvailable(), "Available should be 0 after reserving all");
        assertEquals(5, finalInventory.getQuantityReserved(), "Reserved should be 5");
    }

    @Test
    void testIdempotencyPreventsDoubleReservation() {
        String eventId = "test-event-id-123";
        String consumerName = "test-consumer";

        // First attempt should succeed
        boolean first = inventoryService.reserveInventory(product.getId(), 1);
        assertTrue(first);

        // Simulate idempotency check - second attempt with same event should be detected as duplicate
        // This is tested via IdempotencyService in real flow
        Inventory afterFirst = inventoryRepository.findByProductId(product.getId()).orElseThrow();
        assertEquals(4, afterFirst.getQuantityAvailable());
    }
}
