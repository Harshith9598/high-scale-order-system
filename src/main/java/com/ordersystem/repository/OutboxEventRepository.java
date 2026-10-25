package com.ordersystem.repository;

import com.ordersystem.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Events ready to (re)publish: PENDING/FAILED whose retry time has passed,
     * plus PROCESSING rows whose claim lease (stored in next_retry_at when the
     * row is claimed) has expired — i.e. the owner crashed mid-publish.
     */
    @Query("""
            SELECT e FROM OutboxEvent e
            WHERE (e.status IN (com.ordersystem.enums.OutboxStatus.PENDING,
                                com.ordersystem.enums.OutboxStatus.FAILED)
                   AND (e.nextRetryAt IS NULL OR e.nextRetryAt <= :now))
               OR (e.status = com.ordersystem.enums.OutboxStatus.PROCESSING
                   AND e.nextRetryAt <= :now)
            ORDER BY e.createdAt ASC
            """)
    List<OutboxEvent> findDueEvents(@Param("now") Instant now, Pageable pageable);
}
