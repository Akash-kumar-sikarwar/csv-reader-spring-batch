package com.akash.exportSales.config;

import com.akash.exportSales.dto.SalesDTO;
import com.akash.exportSales.entity.SalesEntity;
import com.akash.exportSales.processor.FileProcessor;
import com.akash.exportSales.processor.SalesProcessor;
import com.akash.exportSales.repository.SalesRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.LocalDate;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class ExportSalesJobConfig {

    private final DataSource dataSource;
    private final SalesProcessor salesProcessor;
    private final JobRepository repository;
    private final PlatformTransactionManager transactionManager;
    private final SalesRepository salesRepository;


    @Bean
    public Job dbToFileJob(Step fromSalesTableToFile, Step importFile){
        return new JobBuilder("db to file job", repository)
                .incrementer(new RunIdIncrementer())
                .start(masterStep(partitionHandler(importFile)))
//                .next(importFile)
                .build();
    }
    @Bean
    public Step fromSalesTableToFile(FlatFileItemWriter<SalesDTO> flatFileItemWriter,
                                     JdbcCursorItemReader<SalesDTO> salesJdbcCursorItemReader){
        return new StepBuilder("from db to file", repository)
                .<SalesDTO, SalesDTO>chunk(2000, transactionManager)
                .reader(salesJdbcCursorItemReader)
                .processor(salesProcessor)
                .writer(flatFileItemWriter)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
//        taskExecutor.setConcurrencyLimit(10);
        return taskExecutor;
    }

    @Bean
    public TaskExecutorPartitionHandler partitionHandler(Step importFile) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(importFile);
        handler.setGridSize(5); // Number of parallel partitions (adjust based on available resources)
        handler.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return handler;
    }

    @Bean
    public Step masterStep(TaskExecutorPartitionHandler partitionHandler) {
        return new StepBuilder("masterStep", repository)
                .partitioner("importFile", new CsvFilePartitioner())
                .partitionHandler(partitionHandler)
                .build();
    }


    //JDBCCursorItemReader
    @Bean
    public JdbcCursorItemReader<SalesDTO> salesJdbcCursorItemReader(){
        var sql = "select sale_id, product_id, customer_id, sale_date, sale_amount, store_location, country from sales where processed = false";
        return new JdbcCursorItemReaderBuilder<SalesDTO>()
                .name("sales reader")
                .dataSource(dataSource)
                .sql(sql)
                .fetchSize(100)
                .rowMapper(new DataClassRowMapper<>(SalesDTO.class))
                .build(); //name required to uniquely identify its execution context which is essential for job
        //restart ability and tracking progress in spring batch
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<SalesDTO> flatFileItemWriter(@Value("#{jobParameters['output.file.name']}") String outputFile){
        return new FlatFileItemWriterBuilder<SalesDTO>()
                .name("sales writer")
                .resource(new FileSystemResource(outputFile))
                .headerCallback(writer -> writer.append("Header of File"))
                .delimited()
                .delimiter(";")
                .sourceType(SalesDTO.class)
                .names("saleId", "productId", "customerId", "saleDate", "saleAmount", "storeLocation", "country")
                .shouldDeleteIfEmpty(Boolean.TRUE)
                .append(Boolean.TRUE)
                .build();

    }


    /*
    ---------------------------------- Different Job and steps ----------------------------
     */


    @Bean
    public Step importFile(FlatFileItemReader<SalesEntity> flatFileItemReader,
                           FileProcessor fileProcessor,
                           RepositoryItemWriter repositoryItemWriter){
        return new StepBuilder("import data", repository)
                .<SalesEntity, SalesEntity>chunk(1000, transactionManager)
                .reader(flatFileItemReader)
                .processor(fileProcessor)
                .writer(repositoryItemWriter)
                .faultTolerant()
//                .skip(FlatFileParseException.class)
//                .skipLimit(50)
                .retry(SQLException.class)
                .retryLimit(3)
                .build();
    }

    @Bean
    public FlatFileItemReader<SalesEntity> flatFileItemReader() throws InstantiationException, IllegalAccessException {
        var fileName = LocalDate.now().toString().concat("_csv_file.csv");
        return new FlatFileItemReaderBuilder<SalesEntity>()
                .name("file reader")
                .linesToSkip(1)
                .resource(new FileSystemResource(fileName))
                .lineMapper(lineMapper())
                .build();
    }

    @Bean
    public LineMapper<SalesEntity> lineMapper() throws InstantiationException, IllegalAccessException {
        DefaultLineMapper<SalesEntity> lineMapper = new DefaultLineMapper<>();
        var lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(";");
        lineTokenizer.setStrict(Boolean.FALSE);
        lineTokenizer.setNames("saleId", "productId", "customerId", "saleDate", "saleAmount", "storeLocation", "country");
        lineMapper.setLineTokenizer(lineTokenizer);

        lineMapper.setFieldSetMapper(SalesFieldSetMapper.class.newInstance());
        return lineMapper;
    }

    @Bean
    public RepositoryItemWriter repositoryItemWriter(){
        return new RepositoryItemWriterBuilder<SalesEntity>()
                .repository(salesRepository)
                .methodName("save")
                .build();
    }

}
