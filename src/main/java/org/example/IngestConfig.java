package org.example;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.batch.*;
import com.marklogic.client.batch.DataMovementBatchWriter;
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
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Spring Configuration object that defines the Spring Batch components we need - a Reader, an optional Processor, and
 * a Writer.
 */
@EnableBatchProcessing
public class IngestConfig extends LoggingObject implements EnvironmentAware, OptionParserConfigurer {

	private Environment env;

	/**
	 * By implementing this method in OptionParserConfigurer, a client can run the Main program and ask for
	 * help and see all of our custom command line options.
	 *
	 * @param parser
	 */
	@Override
	public void configureOptionParser(OptionParser parser) {
		parser.accepts("collections", "Comma-delimited sequence of collections to insert each document into").withRequiredArg();
		parser.accepts("hosts", "Comma-delimited sequence of host names of MarkLogic nodes to write documents to").withRequiredArg();
		parser.accepts("permissions", "Comma-delimited sequence of permissions to apply to each document; role,capability,role,capability,etc").withRequiredArg();
		parser.accepts("root_local_name", "Name of the root element in each document written to MarkLogic").withRequiredArg();
		parser.accepts("row_count", "Number of rows to combine into a single record").withRequiredArg();
		parser.accepts("thread_count", "The number of threads to use for writing to MarkLogic").withRequiredArg();
		parser.accepts("api", "Set to xcc, rest, or dmsdk").withRequiredArg();
	}

	/**
	 * Defines the Spring Batch job. All we need here is to give it a name.
	 *
	 * @param jobBuilderFactory
	 * @param step
	 * @return
	 */
	@Bean
	public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
		return jobBuilderFactory.get("ingestJob").start(step).build();
	}

	/**
	 * Defines the single step in our Spring Batch job. A key feature provided by marklogic-spring-batch is that
	 * command-line arguments can be referenced via the Value annotations shown below.
	 *
	 * @return
	 */
	@Bean
	@JobScope
	public Step step(StepBuilderFactory stepBuilderFactory,
	                 @Value("#{jobParameters['api']}") String api,
	                 @Value("#{jobParameters['collections']}") String collections,
	                 @Value("#{jobParameters['input_file_path']}") String inputFilePath,
	                 @Value("#{jobParameters['permissions']}") String permissions,
	                 @Value("#{jobParameters['hosts']}") String hosts,
	                 @Value("#{jobParameters['thread_count'] ?: 4}") Integer threadCount,
	                 @Value("#{jobParameters['root_local_name']}") String rootLocalName,
	                 @Value("#{jobParameters['child_record_name']}") String childRecordName,
	                 @Value("#{jobParameters['row_count'] ?: 1}") Integer rowCount,
	                 @Value("#{jobParameters['document_type']}") String documentType) {

		// Determine the Spring Batch chunk size
		int chunkSize = 100;
		String prop = env.getProperty(Options.CHUNK_SIZE);
		if (prop != null) {
			chunkSize = Integer.parseInt(prop);
		}

		logger.info("API: " + api);
		logger.info("Chunk size: " + env.getProperty(Options.CHUNK_SIZE));
		logger.info("Hosts: " + hosts);
		logger.info("Root local name: " + rootLocalName);
		logger.info("Child record name: " + childRecordName);
		logger.info("Collections: " + collections);
		logger.info("Permissions: " + permissions);
		logger.info("Thread count: " + threadCount);
		logger.info("Input file path: " + inputFilePath);
		logger.info("Row count: " + rowCount);

		// Reader - set up a flat file reader and customize it
		ItemReader<Map<String, Object>> reader = null;
		CustomFlatFileItemReader<Map<String, Object>> fileReader = new CustomFlatFileItemReader<>();
		fileReader.setResource(new FileSystemResource(inputFilePath));

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

		reader = fileReader;

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

		// Writer - BatchWriter is from ml-javaclient-util, MarkLogicItemWriter is from
		// marklogic-spring-batch
		BatchWriter batchWriter;
		if ("xcc".equals(api)) {
			batchWriter = new XccBatchWriter(buildContentSources(hosts));
		} else if ("dmsdk".equals(api)){
			// Only need one client for DMSDK
			batchWriter = new DataMovementBatchWriter(buildDatabaseClients(hosts).get(0));
		} else {
			batchWriter = new RestBatchWriter(buildDatabaseClients(hosts));
		}
		if (threadCount != null && threadCount > 0) {
			if (batchWriter instanceof BatchWriterSupport) {
				((BatchWriterSupport) batchWriter).setThreadCount(threadCount);
			} else if (batchWriter instanceof DataMovementBatchWriter) {
				((DataMovementBatchWriter)batchWriter).setThreadCount(threadCount);
				((DataMovementBatchWriter)batchWriter).setBatchSize(chunkSize);
			}
		}
		MarkLogicItemWriter writer = new MarkLogicItemWriter(batchWriter);

		// Run the job!
		logger.info("Initialized components, launching job");
		return stepBuilderFactory.get("step1")
			.<Map<String, Object>, DocumentWriteOperation>chunk(chunkSize)
			.reader(reader)
			.processor(processor)
			.writer(writer)
			.build();
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
		logger.info("XCC username: " + username);
		logger.info("XCC database: " + database);
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

		logger.info("Client username: " + username);
		logger.info("Client database: " + database);
		logger.info("Client authentication: " + authentication.name());

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
