package com.akash.exportSales.processor;

import com.akash.exportSales.dto.SalesDTO;
import com.akash.exportSales.entity.SalesEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FileProcessor implements ItemProcessor<SalesEntity, SalesEntity> {
    @Override
    public SalesEntity process(SalesEntity item) throws Exception {
        item.setSalesId(null);
        if(item.getCountry().equalsIgnoreCase("Canada"))
            return null;
        return item;
    }
}
