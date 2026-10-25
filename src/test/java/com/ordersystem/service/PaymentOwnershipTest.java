package com.ordersystem.service;

import com.ordersystem.entity.Order;
import com.ordersystem.entity.Payment;
import com.ordersystem.entity.User;
import com.ordersystem.enums.PaymentStatus;
import com.ordersystem.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentOwnershipTest {

    @Mock private PaymentRepository paymentRepository;

    private Payment paymentOwnedBy(long userId) {
        Order order = Order.builder().id(10L).orderNumber("ORD-10")
                .user(User.builder().id(userId).build()).build();
        return Payment.builder().id(1L).order(order).amount(new BigDecimal("50.00"))
                .status(PaymentStatus.COMPLETED).build();
    }

    private PaymentService service() {
        PaymentService s = new PaymentService(paymentRepository);
        ReflectionTestUtils.setField(s, "failureRate", 0.0);
        ReflectionTestUtils.setField(s, "processingDelayMs", 0L);
        return s;
    }

    @Test
    void ownerAndAdminCanReadPayment() {
        when(paymentRepository.findByOrderId(10L)).thenReturn(Optional.of(paymentOwnedBy(7L)));
        assertEquals(10L, service().getPaymentByOrderId(10L, 7L, false).getOrderId());
        assertEquals(10L, service().getPaymentByOrderId(10L, 123L, true).getOrderId());
    }

    @Test
    void otherUserCannotReadPaymentIdorRegression() {
        when(paymentRepository.findByOrderId(10L)).thenReturn(Optional.of(paymentOwnedBy(7L)));
        assertThrows(AccessDeniedException.class,
                () -> service().getPaymentByOrderId(10L, 8L, false));
    }
}
