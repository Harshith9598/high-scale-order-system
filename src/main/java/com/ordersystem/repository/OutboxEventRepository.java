package com.ordersystem.repository;

import com.ordersystem.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;
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

    /**
     * Atomically claims one due row for this publisher. The
     * {@code version = :version} guard plus the due-status predicate make the
     * claim a single conditional UPDATE: when two publisher instances race
     * for the same row exactly one gets {@code 1} back; the other gets
     * {@code 0} and must skip the row. (Previously claiming relied on the
     * loser's detached {@code save()} throwing an optimistic-lock exception.)
     * Runs in its own transaction so the claim is committed before any Kafka
     * call is made.
     */
    @Modifying
    @Transactional
    @Query("""
            UPDATE OutboxEvent e
            SET e.status = com.ordersystem.enums.OutboxStatus.PROCESSING,
                e.nextRetryAt = :leaseUntil,
                e.version = e.version + 1
            WHERE e.id = :id AND e.version = :version
              AND ((e.status IN (com.ordersystem.enums.OutboxStatus.PENDING,
                                 com.ordersystem.enums.OutboxStatus.FAILED)
                    AND (e.nextRetryAt IS NULL OR e.nextRetryAt <= :now))
                   OR (e.status = com.ordersystem.enums.OutboxStatus.PROCESSING
                       AND e.nextRetryAt <= :now))
            """)
    int claim(@Param("id") Long id,
              @Param("version") Long version,
              @Param("now") Instant now,
              @Param("leaseUntil") Instant leaseUntil);
}
