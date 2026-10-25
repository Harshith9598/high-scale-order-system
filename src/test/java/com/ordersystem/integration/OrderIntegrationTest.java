package com.ordersystem.integration;

import com.ordersystem.dto.OrderDto;
import com.ordersystem.entity.Product;
import com.ordersystem.entity.Role;
import com.ordersystem.entity.User;
import com.ordersystem.enums.RoleType;
import com.ordersystem.repository.ProductRepository;
import com.ordersystem.repository.RoleRepository;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.repository.InventoryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
public class OrderIntegrationTest {

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
        registry.add("app.kafka.topics.order-created", () -> "order.created");
        registry.add("app.kafka.topics.inventory-reserved", () -> "inventory.reserved");
        registry.add("app.kafka.topics.inventory-failed", () -> "inventory.failed");
        registry.add("app.kafka.topics.payment-processed", () -> "payment.processed");
        registry.add("app.kafka.topics.payment-failed", () -> "payment.failed");
        registry.add("app.kafka.topics.order-completed", () -> "order.completed");
        registry.add("app.kafka.topics.notification-created", () -> "notification.created");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RoleRepository roleRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    private String authToken;
    private Product testProduct;

    @BeforeEach
    void setUp() throws Exception {
        // Clean
        inventoryRepository.deleteAll();
        productRepository.deleteAll();
        userRepository.deleteAll();

        // Ensure roles
        Role userRole = roleRepository.findByName(RoleType.USER).orElseGet(() -> {
            Role r = new Role();
            r.setName(RoleType.USER);
            r.setDescription("User role");
            return roleRepository.save(r);
        });

        // Create user
        User user = User.builder()
                .username("testuser")
                .email("test@example.com")
                .password(passwordEncoder.encode("password123"))
                .enabled(true)
                .roles(Set.of(userRole))
                .build();
        userRepository.save(user);

        // Login
        String loginJson = """
                {
                    "usernameOrEmail": "testuser",
                    "password": "password123"
                }
                """;

        String response = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(loginJson))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        authToken = objectMapper.readTree(response).get("accessToken").asText();

        // Create product
        testProduct = Product.builder()
                .name("Integration Test Product")
                .description("Test product")
                .price(new BigDecimal("99.99"))
                .category("Test")
                .active(true)
                .build();
        testProduct = productRepository.save(testProduct);

        com.ordersystem.entity.Inventory inv = com.ordersystem.entity.Inventory.builder()
                .product(testProduct)
                .quantityAvailable(100)
                .quantityReserved(0)
                .lowStockThreshold(10)
                .build();
        inventoryRepository.save(inv);
    }

    @Test
    void testSuccessfulOrderFlow() throws Exception {
        OrderDto.CreateOrderRequest request = new OrderDto.CreateOrderRequest();
        var item = new OrderDto.CreateOrderRequest.OrderItemRequest();
        item.setProductId(testProduct.getId());
        item.setQuantity(2);
        request.setItems(List.of(item));

        String json = objectMapper.writeValueAsString(request);

        String orderResponse = mockMvc.perform(post("/api/orders")
                        .header("Authorization", "Bearer " + authToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderNumber").exists())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andReturn().getResponse().getContentAsString();

        Long orderId = objectMapper.readTree(orderResponse).get("id").asLong();

        // Verify order can be retrieved
        mockMvc.perform(get("/api/orders/" + orderId)
                        .header("Authorization", "Bearer " + authToken))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(orderId));
    }

    @Test
    void testInvalidOrderNoItems() throws Exception {
        OrderDto.CreateOrderRequest request = new OrderDto.CreateOrderRequest();
        request.setItems(List.of());

        String json = objectMapper.writeValueAsString(request);

        mockMvc.perform(post("/api/orders")
                        .header("Authorization", "Bearer " + authToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isBadRequest());
    }

    @Test
    void testUnauthenticatedOrderCreationRejected() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"items\":[{\"productId\":1,\"quantity\":1}]}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void testOrderWithNonExistentProduct() throws Exception {
        OrderDto.CreateOrderRequest request = new OrderDto.CreateOrderRequest();
        var item = new OrderDto.CreateOrderRequest.OrderItemRequest();
        item.setProductId(99999L);
        item.setQuantity(1);
        request.setItems(List.of(item));

        String json = objectMapper.writeValueAsString(request);

        mockMvc.perform(post("/api/orders")
                        .header("Authorization", "Bearer " + authToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isNotFound());
    }
}
