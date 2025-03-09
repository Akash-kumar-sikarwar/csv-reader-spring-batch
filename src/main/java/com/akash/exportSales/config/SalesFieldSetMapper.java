package com.akash.exportSales.config;

import com.akash.exportSales.entity.SalesEntity;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class SalesFieldSetMapper implements FieldSetMapper<SalesEntity> {

    @Override
    public SalesEntity mapFieldSet(FieldSet fieldSet) throws BindException {
        String saleDate = fieldSet.readRawString("saleDate");
        return SalesEntity.builder()
                .salesId(fieldSet.readLong("saleId"))
                .productId(fieldSet.readLong("productId"))
                .customerId(fieldSet.readLong("customerId"))
                .saleDate(saleDate != null? formatDate(saleDate) : null)
                .saleAmount(fieldSet.readLong("saleAmount"))
                .storeLocation(fieldSet.readRawString("storeLocation"))
                .country(fieldSet.readRawString("country"))
                .build();
    }

    private LocalDate formatDate(String value){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return LocalDate.parse(value, formatter);
    }
}
