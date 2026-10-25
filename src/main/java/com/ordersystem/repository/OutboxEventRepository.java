package com.ordersystem.repository;

import com.ordersystem.entity.OutboxEvent;
import com.ordersystem.enums.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findByStatusAndNextRetryAtBeforeOrderByCreatedAtAsc(OutboxStatus status, Instant now);
    List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus status);
    List<OutboxEvent> findByStatusAndNextRetryAtBefore(OutboxStatus status, Instant now);
}
