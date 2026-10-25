package com.ordersystem.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ordersystem.dto.OrderDto;
import com.ordersystem.entity.Order;
import com.ordersystem.entity.Product;
import com.ordersystem.entity.User;
import com.ordersystem.enums.OrderStatus;
import com.ordersystem.exception.InvalidOrderStateException;
import com.ordersystem.kafka.event.OrderCreatedEvent;
import com.ordersystem.mapper.OrderMapper;
import com.ordersystem.metrics.OrderMetrics;
import com.ordersystem.outbox.OutboxService;
import com.ordersystem.repository.OrderRepository;
import com.ordersystem.repository.ProductRepository;
import com.ordersystem.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.access.AccessDeniedException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private ProductRepository productRepository;
    @Mock private UserRepository userRepository;
    @Mock private OrderMapper orderMapper;
    @Mock private OutboxService outboxService;
    @Mock private OrderMetrics orderMetrics;

    private OrderService service;
    private User user;
    private Product product;

    @BeforeEach
    void setUp() {
        service = new OrderService(orderRepository, productRepository, userRepository,
                orderMapper, outboxService, new ObjectMapper(), orderMetrics);
        user = User.builder().id(7L).username("buyer").build();
        product = Product.builder().id(1L).name("Widget").price(new BigDecimal("10.00")).active(true).build();
    }

    private OrderDto.CreateOrderRequest request(OrderDto.CreateOrderRequest.OrderItemRequest... items) {
        var r = new OrderDto.CreateOrderRequest();
        r.setItems(List.of(items));
        return r;
    }

    private OrderDto.CreateOrderRequest.OrderItemRequest item(long productId, int qty) {
        var i = new OrderDto.CreateOrderRequest.OrderItemRequest();
        i.setProductId(productId);
        i.setQuantity(qty);
        return i;
    }

    @Test
    void createOrderPersistsOrderAndOutboxEventAtomically() {
        when(userRepository.findById(7L)).thenReturn(Optional.of(user));
        when(productRepository.findById(1L)).thenReturn(Optional.of(product));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            o.setId(42L);
            return o;
        });
        when(orderMapper.toDtoWithItems(any(Order.class)))
                .thenReturn(OrderDto.builder().id(42L).status(OrderStatus.CREATED).build());

        OrderDto result = service.createOrder(7L, request(item(1L, 3)));

        assertEquals(42L, result.getId());
        // The outbox write is the point of the pattern: it must happen in the
        // same call (same transaction) as the order insert, with a stable id.
        ArgumentCaptor<Object> payload = ArgumentCaptor.forClass(Object.class);
        verify(outboxService).saveEvent(eq("Order"), eq("42"), eq("OrderCreated"),
                payload.capture(), any(), any());
        OrderCreatedEvent event = (OrderCreatedEvent) payload.getValue();
        assertNotNull(event.getEventId());
        assertEquals(new BigDecimal("30.00"), event.getTotalAmount());
        verify(orderMetrics).orderCreated();
    }

    @Test
    void createOrderMergesDuplicateProductLines() {
        when(userRepository.findById(7L)).thenReturn(Optional.of(user));
        when(productRepository.findById(1L)).thenReturn(Optional.of(product));
        when(orderRepository.save(any(Order.class))).thenAnswer(inv -> {
            Order o = inv.getArgument(0);
            o.setId(1L);
            return o;
        });
        when(orderMapper.toDtoWithItems(any(Order.class))).thenReturn(OrderDto.builder().id(1L).build());

        service.createOrder(7L, request(item(1L, 2), item(1L, 3)));

        ArgumentCaptor<Order> saved = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(saved.capture());
        assertEquals(1, saved.getValue().getItems().size(), "duplicate lines must be merged");
        assertEquals(5, saved.getValue().getItems().get(0).getQuantity());
        assertEquals(new BigDecimal("50.00"), saved.getValue().getTotalAmount());
    }

    @Test
    void createOrderRejectsInactiveProductAndWritesNoOutboxEvent() {
        product.setActive(false);
        when(userRepository.findById(7L)).thenReturn(Optional.of(user));
        when(productRepository.findById(1L)).thenReturn(Optional.of(product));

        assertThrows(com.ordersystem.exception.BusinessException.class,
                () -> service.createOrder(7L, request(item(1L, 1))));
        verify(outboxService, never()).saveEvent(any(), any(), any(), any(), any(), any());
        verify(orderRepository, never()).save(any());
    }

    @Test
    void getOrderForAnotherUserIsForbiddenNotBadRequest() {
        Order order = Order.builder().id(5L).user(User.builder().id(99L).build()).build();
        when(orderRepository.findByIdWithItems(5L)).thenReturn(Optional.of(order));
        assertThrows(AccessDeniedException.class, () -> service.getOrder(5L, 7L, false));
        // ...but an admin may read it.
        when(orderMapper.toDtoWithItems(order)).thenReturn(OrderDto.builder().id(5L).build());
        assertEquals(5L, service.getOrder(5L, 7L, true).getId());
    }

    @Test
    void invalidStatusTransitionIsRejected() {
        Order order = Order.builder().id(5L).status(OrderStatus.CREATED).orderNumber("ORD-1").build();
        when(orderRepository.findById(5L)).thenReturn(Optional.of(order));
        assertThrows(InvalidOrderStateException.class,
                () -> service.updateOrderStatus(5L, OrderStatus.COMPLETED));
        verify(orderRepository, never()).save(any());
    }

    @Test
    void completedOrderCannotBeCancelled() {
        Order order = Order.builder().id(5L).status(OrderStatus.COMPLETED)
                .user(User.builder().id(7L).build()).build();
        when(orderRepository.findByIdWithItems(5L)).thenReturn(Optional.of(order));
        assertThrows(InvalidOrderStateException.class, () -> service.cancelOrder(5L, 7L, false));
    }
}
