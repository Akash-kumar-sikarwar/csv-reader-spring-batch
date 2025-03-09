package com.akash.exportSales.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Table(name = "sales_info_v2")
public class SalesEntity {
    @Id
    @GeneratedValue
    @Column(name = "sales_id")
    private Long salesId;
    @Column(name = "product_id")
    private Long productId;
    @Column(name = "customer_id")
    private Long customerId;
    @Column(name = "sale_date")
    @DateTimeFormat(pattern = "yyyy-MM-dd")
    private LocalDate saleDate;
    @Column(name = "sale_amount")
    private Long saleAmount;
    @Column(name = "store_location")
    private String storeLocation;
    @Column(name = "country")
    private String country;
}
