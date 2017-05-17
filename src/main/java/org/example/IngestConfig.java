package org.example;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.batch.*;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.helper.LoggingObject;
import com.marklogic.spring.batch.Options;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.columnmap.DefaultStaxColumnMapSerializer;
import com.marklogic.spring.batch.columnmap.JacksonColumnMapSerializer;
import com.marklogic.spring.batch.config.support.OptionParserConfigurer;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import joptsimple.OptionParser;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spring Configuration that defines all the components we need to run our Spring Batch job.
 */
@EnableBatchProcessing
public class IngestConfig extends LoggingObject implements EnvironmentAware, OptionParserConfigurer {

	private Environment env;

	@Autowired
	private JobBuilderFactory jobBuilders;

	@Autowired
	private StepBuilderFactory stepBuilders;

	/**
	 * By implementing this method in OptionParserConfigurer, a client can run the Main program and ask for
	 * help and see all of our custom command line options.
	 *
	 * @param parser
	 */
	@Override
	public void configureOptionParser(OptionParser parser) {
		parser.accepts("api", "Set to xcc, rest, or dmsdk").withRequiredArg();
		parser.accepts("collections", "Comma-delimited sequence of collections to insert each document into").withRequiredArg();
		parser.accepts("file_thread_count", "Number of threads to use for reading files").withRequiredArg();
		parser.accepts("hosts", "Comma-delimited sequence of host names of MarkLogic nodes to write documents to").withRequiredArg();
		parser.accepts("permissions", "Comma-delimited sequence of permissions to apply to each document; role,capability,role,capability,etc").withRequiredArg();
		parser.accepts("root_local_name", "Name of the root element in each document written to MarkLogic").withRequiredArg();
		parser.accepts("row_count", "Number of rows to combine into a single record").withRequiredArg();
		parser.accepts("thread_count", "The number of threads to use for writing to MarkLogic").withRequiredArg();
	}

	/**
	 * Defines the Spring Batch job. This runs the partition step, which will then run an instance of the file reader
	 * step for every file that's found.
	 *
	 * @param step
	 * @return
	 */
	@Bean
	public Job job(@Qualifier("partitionStep") Step step) {
		return jobBuilders.get("ingestJob").start(step).build();
	}

	/**
	 * Thread pool that controls how many threads the partition step uses.
	 *
	 * @param fileThreadCount
	 * @return
	 */
	@Bean
	@JobScope
	public ThreadPoolTaskExecutor partitionTaskExecutor(@Value("#{jobParameters['file_thread_count'] ?: 4}") Integer fileThreadCount) {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setCorePoolSize(fileThreadCount);
		taskExecutor.setThreadNamePrefix("ingest-thread-");
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	/**
	 * Defines the partition step. Its job is to launch an instance of the file reading step for every file that's found.
	 *
	 * @param taskExecutor
	 * @param readFileStep
	 * @param inputFilePath
	 * @return
	 */
	@Bean
	@JobScope
	public Step partitionStep(
		ThreadPoolTaskExecutor taskExecutor,
		@Qualifier("readFileStep") Step readFileStep,
		@Value("#{jobParameters['input_file_path']}") String inputFilePath
	) {
		MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
		try {
			PathMatchingResourcePatternResolver pathResolver = new PathMatchingResourcePatternResolver();
			Resource[] resources = pathResolver.getResources(inputFilePath);
			partitioner.setResources(resources);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read from input file path: " + inputFilePath, e);
		}
		return stepBuilders.get("partitionStep")
			.partitioner(readFileStep)
			.partitioner("readFileStep", partitioner)
			.taskExecutor(taskExecutor)
			.build();
	}

	/**
	 * Defines the file reading step. This depends on our reader, processor, and writer to do all the work.
	 *
	 * @return
	 */
	@Bean
	public Step readFileStep(
		ItemReader<Map<String, Object>> reader,
		ItemProcessor<Map<String, Object>, DocumentWriteOperation> processor,
		ItemWriter<DocumentWriteOperation> writer
	) {
		// Determine the Spring Batch chunk size
		int chunkSize = 100;
		String prop = env.getProperty(Options.CHUNK_SIZE);
		if (prop != null) {
			chunkSize = Integer.parseInt(prop);
		}
		logger.info("Chunk size: " + chunkSize);
		return stepBuilders.get("readFileStep")
			.<Map<String, Object>, DocumentWriteOperation>chunk(chunkSize)
			.reader(reader)
			.processor(processor)
			.writer(writer)
			.build();
	}

	/**
	 * Reader for each file that's found.
	 *
	 * @param childRecordName
	 * @param rowCount
	 * @param fileName
	 * @return
	 */
	@Bean
	@StepScope
	public CustomFlatFileItemReader<Map<String, Object>> fileReader(
		@Value("#{jobParameters['child_record_name']}") String childRecordName,
		@Value("#{jobParameters['row_count'] ?: 1}") Integer rowCount,
		@Value("#{stepExecutionContext[fileName]}") String fileName
	) {
//		logger.info("Child record name: " + childRecordName);
//		logger.info("Row count: " + rowCount);
//		logger.info("File name: " + fileName);

		CustomFlatFileItemReader<Map<String, Object>> fileReader = new CustomFlatFileItemReader<>();
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		fileReader.setResource(resolver.getResource(fileName));

		// Use our custom mapper to convert a field set into a map
		ColumnMapFieldSetMapper fieldSetMapper = new ColumnMapFieldSetMapper();
		fieldSetMapper.setRecordName(childRecordName);
		DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
		lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
		lineMapper.setFieldSetMapper(fieldSetMapper);
		fileReader.setLineMapper(lineMapper);

		// Use MultiLinePolicy to control how many lines are combined into a single record
		fileReader.setRecordSeparatorPolicy(new MultiLinePolicy(rowCount));

		// Skip the first line, assuming it's the column labels
		fileReader.setLinesToSkip(1);

		// Process that skipped line so we can set field names for the LineMapper to use
		fileReader.setSkippedLinesCallback(new ColumnMapLineCallbackHandler(fieldSetMapper));
		return fileReader;
	}

	/**
	 * Processor for every file. Its job is to convert a column map into a MarkLogic DocumentWriteOperation.
	 *
	 * @param collections
	 * @param permissions
	 * @param rootLocalName
	 * @param documentType
	 * @return
	 */
	@Bean
	@StepScope
	public ColumnMapProcessor columnMapProcessor(
		@Value("#{jobParameters['collections']}") String collections,
		@Value("#{jobParameters['permissions']}") String permissions,
		@Value("#{jobParameters['root_local_name']}") String rootLocalName,
		@Value("#{jobParameters['document_type']}") String documentType
	) {
//		logger.info("Root local name: " + rootLocalName);
//		logger.info("Collections: " + collections);
//		logger.info("Permissions: " + permissions);

		// Processor - this is a very basic implementation for converting a column map to an XML or JSON string
		ColumnMapSerializer serializer = null;
		if (documentType != null && documentType.toLowerCase().equals("json")) {
			serializer = new JacksonColumnMapSerializer();
		} else {
			serializer = new DefaultStaxColumnMapSerializer();
		}
		ColumnMapProcessor processor = new ColumnMapProcessor(serializer);
		if (rootLocalName != null) {
			processor.setRootLocalName(rootLocalName);
		}
		if (collections != null) {
			processor.setCollections(collections.split(","));
		}
		if (permissions != null) {
			processor.setPermissions(permissions.split(","));
		}
		if (documentType != null && documentType.toLowerCase().equals("json")) {
			processor.setUriSuffix(".json");
		}
		return processor;
	}

	/**
	 * Defines the writer that's used to write objects to MarkLogic.
	 * 
	 * @param api
	 * @param hosts
	 * @param threadCount
	 * @return
	 */
	@Bean
	@StepScope
	public MarkLogicItemWriter markLogicItemWriter(
		@Value("#{jobParameters['api']}") String api,
		@Value("#{jobParameters['hosts']}") String hosts,
		@Value("#{jobParameters['thread_count'] ?: 4}") Integer threadCount
	) {
//		logger.info("API: " + api);
//		logger.info("Hosts: " + hosts);
//		logger.info("Thread count: " + threadCount);

		// Determine the Spring Batch chunk size
		int chunkSize = 100;
		String prop = env.getProperty(Options.CHUNK_SIZE);
		if (prop != null) {
			chunkSize = Integer.parseInt(prop);
		}

		// Writer - BatchWriter is from ml-javaclient-util, MarkLogicItemWriter is from
		// marklogic-spring-batch
		BatchWriter batchWriter;
		if ("xcc".equals(api)) {
			batchWriter = new XccBatchWriter(buildContentSources(hosts));
		} else if ("dmsdk".equals(api)) {
			// Only need one client for DMSDK
			batchWriter = new DataMovementBatchWriter(buildDatabaseClients(hosts).get(0));
		} else {
			batchWriter = new RestBatchWriter(buildDatabaseClients(hosts));
		}
		if (threadCount != null && threadCount > 0) {
			if (batchWriter instanceof BatchWriterSupport) {
				((BatchWriterSupport) batchWriter).setThreadCount(threadCount);
				((BatchWriterSupport) batchWriter).setWriteListener(new WriteListener() {
					@Override
					public void onWriteFailure(Throwable ex, List<? extends DocumentWriteOperation> items) {
						logger.info("Could not write items: " + ex.getMessage());
						for (DocumentWriteOperation op : items) {
							logger.info("Content: " + op.getContent());
						}
					}
				});
			} else if (batchWriter instanceof DataMovementBatchWriter) {
				((DataMovementBatchWriter) batchWriter).setThreadCount(threadCount);
				((DataMovementBatchWriter) batchWriter).setBatchSize(chunkSize);
			}
		}
		return new MarkLogicItemWriter(batchWriter);
	}

	/**
	 * Build a list of XCC ContentSource objects based on the value of hosts, which may be a comma-delimited string of
	 * host names.
	 *
	 * @param hosts
	 * @return
	 */
	protected List<ContentSource> buildContentSources(String hosts) {
		Integer port = Integer.parseInt(env.getProperty(Options.PORT));
		String username = env.getProperty(Options.USERNAME);
		String password = env.getProperty(Options.PASSWORD);
		String database = env.getProperty(Options.DATABASE);
//		logger.info("XCC username: " + username);
//		logger.info("XCC database: " + database);
		List<ContentSource> list = new ArrayList<>();
		if (hosts != null) {
			for (String host : hosts.split(",")) {
				logger.info("Creating content source for host: " + host);
				list.add(ContentSourceFactory.newContentSource(host, port, username, password, database));
			}
		} else {
			String host = env.getProperty(Options.HOST);
			logger.info("Creating content source for host: " + host);
			list.add(ContentSourceFactory.newContentSource(host, port, username, password, database));
		}
		return list;
	}

	/**
	 * Build a list of Java Client API DatabaseClient objects based on the value of hosts, which may be a
	 * comma-delimited string of host names.
	 *
	 * @param hosts
	 * @return
	 */
	protected List<DatabaseClient> buildDatabaseClients(String hosts) {
		Integer port = Integer.parseInt(env.getProperty(Options.PORT));
		String username = env.getProperty(Options.USERNAME);
		String password = env.getProperty(Options.PASSWORD);
		String database = env.getProperty(Options.DATABASE);
		String auth = env.getProperty(Options.AUTHENTICATION);
		DatabaseClientFactory.Authentication authentication = DatabaseClientFactory.Authentication.DIGEST;
		if (auth != null) {
			authentication = DatabaseClientFactory.Authentication.valueOf(auth.toUpperCase());
		}

//		logger.info("Client username: " + username);
//		logger.info("Client database: " + database);
//		logger.info("Client authentication: " + authentication.name());

		List<DatabaseClient> databaseClients = new ArrayList<>();
		if (hosts != null) {
			for (String host : hosts.split(",")) {
				logger.info("Creating client for host: " + host);
				databaseClients.add(DatabaseClientFactory.newClient(host, port, database, username, password, authentication));
			}
		} else {
			String host = env.getProperty(Options.HOST);
			logger.info("Creating client for host: " + host);
			databaseClients.add(DatabaseClientFactory.newClient(host, port, database, username, password, authentication));
		}

		return databaseClients;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.env = environment;
	}

}
