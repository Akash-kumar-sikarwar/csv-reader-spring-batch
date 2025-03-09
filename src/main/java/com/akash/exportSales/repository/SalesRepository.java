package com.akash.exportSales.repository;

import com.akash.exportSales.entity.SalesEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SalesRepository extends JpaRepository<SalesEntity, Long> {

}
