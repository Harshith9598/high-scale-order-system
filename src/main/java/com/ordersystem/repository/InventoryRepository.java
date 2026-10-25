package com.ordersystem.repository;

import com.ordersystem.entity.Inventory;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InventoryRepository extends JpaRepository<Inventory, Long> {
    Optional<Inventory> findByProductId(Long productId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT i FROM Inventory i WHERE i.product.id = :productId")
    Optional<Inventory> findByProductIdWithPessimisticLock(@Param("productId") Long productId);

    @Lock(LockModeType.OPTIMISTIC)
    @Query("SELECT i FROM Inventory i WHERE i.product.id = :productId")
    Optional<Inventory> findByProductIdWithOptimisticLock(@Param("productId") Long productId);

    @Modifying
    @Query("UPDATE Inventory i SET i.quantityAvailable = i.quantityAvailable - :quantity, " +
            "i.quantityReserved = i.quantityReserved + :quantity, i.version = i.version + 1 " +
            "WHERE i.product.id = :productId AND i.quantityAvailable >= :quantity")
    int reserveInventory(@Param("productId") Long productId, @Param("quantity") int quantity);

    @Modifying
    @Query("UPDATE Inventory i SET i.quantityAvailable = i.quantityAvailable + :quantity, " +
            "i.quantityReserved = i.quantityReserved - :quantity, i.version = i.version + 1 " +
            "WHERE i.product.id = :productId AND i.quantityReserved >= :quantity")
    int releaseInventory(@Param("productId") Long productId, @Param("quantity") int quantity);

    @Modifying
    @Query("UPDATE Inventory i SET i.quantityReserved = i.quantityReserved - :quantity, i.version = i.version + 1 " +
            "WHERE i.product.id = :productId AND i.quantityReserved >= :quantity")
    int confirmReservation(@Param("productId") Long productId, @Param("quantity") int quantity);
}
