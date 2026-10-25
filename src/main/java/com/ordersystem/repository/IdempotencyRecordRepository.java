package com.ordersystem.repository;

import com.ordersystem.entity.IdempotencyRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface IdempotencyRecordRepository extends JpaRepository<IdempotencyRecord, Long> {
    boolean existsByEventIdAndConsumerName(String eventId, String consumerName);
    Optional<IdempotencyRecord> findByEventIdAndConsumerName(String eventId, String consumerName);
}
