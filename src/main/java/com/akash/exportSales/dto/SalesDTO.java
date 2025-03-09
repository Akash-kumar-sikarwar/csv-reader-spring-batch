package com.akash.exportSales.dto;

import java.time.LocalDate;

public record SalesDTO(
        Long saleId,
        Long productId,
        Long customerId,
        LocalDate saleDate,
        Long saleAmount,
        String storeLocation,
        String country
) {
}
