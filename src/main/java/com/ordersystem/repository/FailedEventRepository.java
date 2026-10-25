package com.ordersystem.repository;

import com.ordersystem.entity.FailedEvent;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedEventRepository extends JpaRepository<FailedEvent, Long> {
    Page<FailedEvent> findByOrderByCreatedAtDesc(Pageable pageable);
    Page<FailedEvent> findByTopic(String topic, Pageable pageable);
    Page<FailedEvent> findByOrderId(String orderId, Pageable pageable);
}
