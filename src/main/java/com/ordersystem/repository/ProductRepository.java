package com.ordersystem.repository;

import com.ordersystem.entity.Product;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {
    Page<Product> findByActiveTrue(Pageable pageable);

    // CASTs matter: without them Hibernate binds a null :search as bytea and
    // PostgreSQL rejects lower(bytea) -- listing products without filters 500s.
    @Query("SELECT p FROM Product p WHERE p.active = true AND " +
            "(CAST(:category AS string) IS NULL OR p.category = :category) AND " +
            "(CAST(:search AS string) IS NULL OR LOWER(p.name) LIKE LOWER(CONCAT('%', CAST(:search AS string), '%')))")
    Page<Product> findByFilters(String category, String search, Pageable pageable);

    Page<Product> findByCategory(String category, Pageable pageable);
}
