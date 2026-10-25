package com.ordersystem.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.dto.OrderDto;
import com.ordersystem.entity.*;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.exception.BusinessException;
import com.ordersystem.exception.InvalidOrderStateException;
import com.ordersystem.exception.ResourceNotFoundException;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.mapper.OrderMapper;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.repository.ProductRepository;
import com.ordersystem.repository.UserRepository;
import com.ordersystem.util.CorrelationIdUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;
    private final UserRepository userRepository;
    private final OrderMapper orderMapper;
    private final OutboxService outboxService;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topics.order-created}")
    private String orderCreatedTopic;

    @Transactional
    public OrderDto createOrder(Long userId, OrderDto.CreateOrderRequest request) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User not found: " + userId));

        if (request.getItems() == null || request.getItems().isEmpty()) {
            throw new BusinessException("Order must contain at least one item");
        }

        String correlationId = CorrelationIdUtil.getCorrelationId();
        if (correlationId == null) correlationId = UUID.randomUUID().toString();

        Order order = Order.builder()
                .orderNumber("ORD-" + System.currentTimeMillis() + "-" + (int)(Math.random()*10000))
                .user(user)
                .status(OrderStatus.CREATED)
                .totalAmount(BigDecimal.ZERO)
                .correlationId(correlationId)
                .build();

        BigDecimal total = BigDecimal.ZERO;
        List<OrderItem> orderItems = new ArrayList<>();

        for (var itemReq : request.getItems()) {
            Product product = productRepository.findById(itemReq.getProductId())
                    .orElseThrow(() -> new ResourceNotFoundException("Product not found: " + itemReq.getProductId()));

            if (!product.getActive()) {
                throw new BusinessException("Product is not active: " + product.getName());
            }

            if (itemReq.getQuantity() <= 0) {
                throw new BusinessException("Quantity must be positive for product: " + product.getName());
            }

            BigDecimal itemTotal = product.getPrice().multiply(BigDecimal.valueOf(itemReq.getQuantity()));
            total = total.add(itemTotal);

            OrderItem orderItem = OrderItem.builder()
                    .order(order)
                    .product(product)
                    .quantity(itemReq.getQuantity())
                    .unitPrice(product.getPrice())
                    .totalPrice(itemTotal)
                    .build();
            orderItems.add(orderItem);
        }

        order.setTotalAmount(total);
        order.setItems(orderItems);

        order = orderRepository.save(order);
        log.info("Order created: id={} orderNumber={} userId={} total={} correlationId={}",
                order.getId(), order.getOrderNumber(), userId, total, correlationId);

        // Build OrderCreatedEvent for outbox
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .correlationId(correlationId)
                .timestamp(Instant.now())
                .eventType("OrderCreated")
                .orderId(order.getId())
                .orderNumber(order.getOrderNumber())
                .userId(userId)
                .totalAmount(total)
                .currency(order.getCurrency())
                .items(orderItems.stream().map(oi -> OrderCreatedEvent.OrderItem.builder()
                        .productId(oi.getProduct().getId())
                        .productName(oi.getProduct().getName())
                        .quantity(oi.getQuantity())
                        .unitPrice(oi.getUnitPrice())
                        .build()).toList())
                .build();

        // Save to outbox (transactional outbox pattern)
        outboxService.saveEvent("Order", order.getId().toString(), "OrderCreated", event, orderCreatedTopic, correlationId);

        return orderMapper.toDtoWithItems(order);
    }

    @Transactional(readOnly = true)
    public OrderDto getOrder(Long orderId, Long currentUserId, boolean isAdmin) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found: " + orderId));

        if (!isAdmin && !order.getUser().getId().equals(currentUserId)) {
            throw new BusinessException("Access denied to order: " + orderId);
        }

        return orderMapper.toDtoWithItems(order);
    }

    @Transactional(readOnly = true)
    public Page<OrderDto> getUserOrders(Long userId, Pageable pageable) {
        return orderRepository.findByUserId(userId, pageable)
                .map(orderMapper::toDtoWithItems);
    }

    @Transactional(readOnly = true)
    public Page<OrderDto> getAllOrders(Pageable pageable) {
        return orderRepository.findAll(pageable).map(orderMapper::toDtoWithItems);
    }

    @Transactional
    public OrderDto cancelOrder(Long orderId, Long currentUserId, boolean isAdmin) {
        Order order = orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found: " + orderId));

        if (!isAdmin && !order.getUser().getId().equals(currentUserId)) {
            throw new BusinessException("Access denied to order: " + orderId);
        }

        if (order.getStatus() == OrderStatus.COMPLETED || order.getStatus() == OrderStatus.CANCELLED) {
            throw new InvalidOrderStateException("Cannot cancel order in status: " + order.getStatus());
        }

        if (!order.getStatus().canTransitionTo(OrderStatus.CANCELLED)) {
            throw new InvalidOrderStateException(
                    String.format("Invalid transition from %s to %s", order.getStatus(), OrderStatus.CANCELLED));
        }

        order.setStatus(OrderStatus.CANCELLED);
        order = orderRepository.save(order);
        log.info("Order cancelled: id={} orderNumber={}", order.getId(), order.getOrderNumber());

        return orderMapper.toDtoWithItems(order);
    }

    @Transactional
    public Order updateOrderStatus(Long orderId, OrderStatus newStatus) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found: " + orderId));

        if (!order.getStatus().canTransitionTo(newStatus)) {
            throw new InvalidOrderStateException(
                    String.format("Invalid transition from %s to %s for order %s",
                            order.getStatus(), newStatus, order.getOrderNumber()));
        }

        order.setStatus(newStatus);
        order = orderRepository.save(order);
        log.info("Order status updated: orderNumber={} {} -> {}", order.getOrderNumber(), order.getStatus(), newStatus);
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrderEntity(Long orderId) {
        return orderRepository.findByIdWithItems(orderId)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found: " + orderId));
    }
}
