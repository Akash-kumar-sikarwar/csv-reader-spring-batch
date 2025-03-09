package com.akash.exportSales.config;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CsvFilePartitioner implements Partitioner {

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> partitions = new HashMap<>();

        int totalLines = 100000; // Total rows in the CSV (can be calculated dynamically)
        int chunkSize = totalLines / gridSize;

        for (int i = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("startLine", i * chunkSize);
            context.putInt("endLine", (i == gridSize - 1) ? totalLines : (i + 1) * chunkSize);
            partitions.put("partition" + i, context);
        }

        return partitions;
    }
}
