package com.akash.exportSales.processor;

import com.akash.exportSales.dto.SalesDTO;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;


/*
    ItemProcessor: processes data btw reading and writing steps in a batch job. It transforms,
    validates, enriches, filters or applies business logic to the data
 */


@Component
@Slf4j
public class SalesProcessor implements ItemProcessor<SalesDTO, SalesDTO> {



    @Override
    public SalesDTO process(SalesDTO item) throws Exception {
        log.info("processing the item : " + item);
        if ("United States".equalsIgnoreCase(item.country())) {
            return item;
        }
        return null;
    }
}
