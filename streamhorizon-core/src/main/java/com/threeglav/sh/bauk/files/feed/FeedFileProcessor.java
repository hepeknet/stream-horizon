package com.threeglav.sh.bauk.files.feed;

import gnu.trove.map.hash.THashMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.command.BaukCommandsExecutor;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.feed.DefaultFeedFileNameProcessor;
import com.threeglav.sh.bauk.feed.FeedFileNameProcessor;
import com.threeglav.sh.bauk.feed.FeedProcessor;
import com.threeglav.sh.bauk.feed.TextFileReaderComponent;
import com.threeglav.sh.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.sh.bauk.feed.processing.SingleThreadedFeedDataProcessor;
import com.threeglav.sh.bauk.files.BaukFile;
import com.threeglav.sh.bauk.files.FileProcessingErrorHandler;
import com.threeglav.sh.bauk.files.InputFeedProcessor;
import com.threeglav.sh.bauk.files.MoveFileErrorHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BulkLoadDefinition;
import com.threeglav.sh.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class FeedFileProcessor implements InputFeedProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private static final Chronology DEFAULT_CHRONOLOGY = ISOChronology.getInstance();

	private final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(BaukConstants.TIMESTAMP_TO_DATE_FORMAT);

	private final Map<String, String> implicitAttributes = new THashMap<String, String>(30);

	private final Feed factFeed;
	private final BaukConfiguration config;
	private final Meter inputFeedsProcessed;
	private final Histogram inputFeedProcessingTime;
	private final TextFileReaderComponent textFileReaderComponent;
	private final FeedDataProcessor feedDataProcessor;
	private final FeedProcessor feedCompletionProcessor;
	private final FeedProcessor beforeFeedProcessingProcessor;
	private FeedFileNameProcessor feedFileNameProcessor;
	private String fileExtension;
	private final boolean isDebugEnabled;
	private final String processorId;
	private final String multiInstanceProcessorId;
	private FileProcessingErrorHandler moveToArchiveFileProcessor;
	private final boolean executeRollbackSequence;
	private BaukCommandsExecutor feedProcessingFailureCommandsExecutor;
	private final boolean throughputTestingMode;
	private final String bulkOutDirectory;

	public FeedFileProcessor(final Feed factFeed, final BaukConfiguration config, final String fileMask) {
		if (factFeed == null) {
			throw new IllegalArgumentException("Fact feed must not be null");
		}
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		if (StringUtil.isEmpty(fileMask)) {
			throw new IllegalArgumentException("File mask must not be null or empty");
		}
		this.factFeed = factFeed;
		this.config = config;
		final String cleanFileMask = StringUtil.replaceAllNonASCII(fileMask);
		feedDataProcessor = new SingleThreadedFeedDataProcessor(factFeed, config);
		textFileReaderComponent = new TextFileReaderComponent(this.factFeed, this.config, feedDataProcessor, cleanFileMask);
		feedCompletionProcessor = this.createFeedCompletionProcessor();
		beforeFeedProcessingProcessor = this.createBeforeFeedProcessingProcessor();
		inputFeedsProcessed = MetricsUtil.createMeter("(" + cleanFileMask + ") - processed files count");
		inputFeedProcessingTime = MetricsUtil.createHistogram("(" + cleanFileMask + ") - processing time (millis)");
		this.initializeFeedFileNameProcessor();
		final int localProcessorId = this.calculateCurrentThreadId();
		processorId = String.valueOf(localProcessorId);
		log.debug("Number of feed processing instances is {}", processorId);
		final boolean isMultiInstance = ConfigurationProperties.isConfiguredPartitionedMultipleInstances();
		if (isMultiInstance) {
			final int multiInst = ConfigurationProperties.calculateMultiInstanceFeedProcessorId(localProcessorId, this.factFeed);
			multiInstanceProcessorId = String.valueOf(multiInst);
			log.info("Successfully set multi instance feed thread identifier to {}", multiInst);
		} else {
			multiInstanceProcessorId = "-1";
		}
		isDebugEnabled = log.isDebugEnabled();
		final String archiveFolderPath = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ARCHIVE_DIRECTORY_PARAM_NAME,
				factFeed.getArchiveDirectory());
		if (!StringUtil.isEmpty(archiveFolderPath)) {
			moveToArchiveFileProcessor = new MoveFileErrorHandler(archiveFolderPath);
			log.info("Will move all successfully processed files to {}", archiveFolderPath);
		}
		executeRollbackSequence = factFeed.getEvents() != null && factFeed.getEvents().getOnFeedProcessingFailure() != null
				&& !factFeed.getEvents().getOnFeedProcessingFailure().isEmpty();
		log.info("Will execute rollback commands for feed {} = {}", factFeed.getName(), executeRollbackSequence);
		if (executeRollbackSequence) {
			feedProcessingFailureCommandsExecutor = new BaukCommandsExecutor(factFeed, config, factFeed.getEvents().getOnFeedProcessingFailure());
		}
		throughputTestingMode = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);
		bulkOutDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.OUTPUT_DIRECTORY_PARAM_NAME,
				factFeed.getBulkOutputDirectory());
		this.validate();
	}

	private int calculateCurrentThreadId() {
		int currentCounter = COUNTER.getAndIncrement();
		boolean isJdbcLoadingOn = false;
		final BulkLoadDefinition bulkLoadDefinition = factFeed.getBulkLoadDefinition();
		if (bulkLoadDefinition != null) {
			isJdbcLoadingOn = BulkLoadDefinitionOutputType.JDBC.toString().equalsIgnoreCase(bulkLoadDefinition.getOutputType());
		}
		if (isJdbcLoadingOn) {
			log.debug("Checking whether to turn on jdbc thread partitioning");
			final int jdbcThreadPartitionCount = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.JDBC_THREADS_PARTITION_COUNT, -1);
			if (jdbcThreadPartitionCount > 0) {
				final int totalEtlThreads = factFeed.getThreadPoolSettings().getEtlProcessingThreadCount();
				if (totalEtlThreads > jdbcThreadPartitionCount) {
					log.info("Will turn on JDBC thread partitioning. {} ETL threads will be grouped in {} partitions", totalEtlThreads,
							jdbcThreadPartitionCount);
					currentCounter = BaukUtil.calculatePartition(totalEtlThreads, jdbcThreadPartitionCount, currentCounter);
				} else {
					log.warn(
							"Value for {} can not be higher than total number of etl threads. Will not turn on JDBC thread partitioning. Found value partition count {}",
							BaukEngineConfigurationConstants.JDBC_THREADS_PARTITION_COUNT, jdbcThreadPartitionCount);
				}
			} else {
				log.info("Will not turn on jdbc partitioning because {} is set to {}. Must be positive integer in order to turn partitioning on.",
						BaukEngineConfigurationConstants.JDBC_THREADS_PARTITION_COUNT, jdbcThreadPartitionCount);
			}
		}
		return currentCounter;
	}

	@Override
	public void process(final BaukFile file) throws IOException {
		final String fullFileName = file.getFullFilePath();
		if (isDebugEnabled) {
			log.debug("Trying to process {}", fullFileName);
		}
		InputStream inputStream = null;
		try {
			inputStream = file.getInputStream();
			this.processInputStream(inputStream, file);
		} finally {
			file.closeResources();
			IOUtils.closeQuietly(inputStream);
		}
		// if testing throughput no need to move or delete file
		if (!throughputTestingMode) {
			try {
				if (moveToArchiveFileProcessor != null && file.getPath() != null) {
					moveToArchiveFileProcessor.handleError(file.getPath(), null);
				} else {
					file.delete();
				}
			} catch (final IOException ie) {
				log.error("Exception while moving file to archive folder. Exiting!", ie);
				System.exit(-1);
			}
		}
	}

	private FeedProcessor createBeforeFeedProcessingProcessor() {
		FeedProcessor processor = null;
		if (factFeed.getEvents() != null && factFeed.getEvents().getBeforeFeedProcessing() != null
				&& !factFeed.getEvents().getBeforeFeedProcessing().isEmpty()) {
			log.debug("Will perform before feed processing for {}", factFeed.getName());
			processor = new FeedProcessor(factFeed, config, "BeforeFeedProcessor", factFeed.getEvents().getBeforeFeedProcessing());
		} else {
			log.debug("Will not perform any before feed processing for {}", factFeed.getName());
		}
		return processor;
	}

	private void initializeFeedFileNameProcessor() {
		String feedFileNameProcessorClassName = DefaultFeedFileNameProcessor.class.getName();
		final String configuredFileProcessorClass = factFeed.getFileNameProcessorClassName();
		if (!StringUtil.isEmpty(configuredFileProcessorClass)) {
			feedFileNameProcessorClassName = configuredFileProcessorClass;
			log.debug("Will try to use custom feed file name processor class {}", configuredFileProcessorClass);
		} else {
			log.debug("Will use default feed file name processor class {}", feedFileNameProcessorClassName);
		}
		final CustomProcessorResolver<FeedFileNameProcessor> feedFileNameProcessorInstanceResolver = new CustomProcessorResolver<>(
				feedFileNameProcessorClassName, FeedFileNameProcessor.class);
		feedFileNameProcessor = feedFileNameProcessorInstanceResolver.resolveInstance();
		if (feedFileNameProcessor != null) {
			try {
				feedFileNameProcessor.init(ConfigurationProperties.getEngineConfigurationProperties());
			} catch (final Exception exc) {
				log.error("Exception while trying to initialize custom feed file name processor {}. Details {}", feedFileNameProcessor,
						exc.getMessage());
				throw exc;
			}
		}
	}

	private void validate() {
		if (StringUtil.isEmpty(bulkOutDirectory)) {
			throw new IllegalStateException("Bulk output directory must not be null or empty!");
		}
		fileExtension = null;
		final BulkLoadDefinition bulkDefinition = factFeed.getBulkLoadDefinition();
		if (bulkDefinition != null) {
			fileExtension = bulkDefinition.getBulkLoadOutputExtension();
			if (fileExtension != null && !fileExtension.matches("[A-Za-z0-9]+")) {
				throw new IllegalStateException("Bulk file extension must contain only alpha-numerical characters. Currently it is set to ["
						+ fileExtension + "]");
			}
		}
		final boolean isEmptyExtension = StringUtil.isEmpty(fileExtension);
		if (isEmptyExtension
				&& (bulkDefinition.getOutputType().equalsIgnoreCase(BulkLoadDefinitionOutputType.FILE.toString())
						|| bulkDefinition.getOutputType().equalsIgnoreCase(BulkLoadDefinitionOutputType.ZIP.toString()) || bulkDefinition
						.getOutputType().equalsIgnoreCase(BulkLoadDefinitionOutputType.GZ.toString()))) {
			throw new IllegalStateException(
					"Extension for recognizing bulk output files is required to be specified in configuration file because file output will be generated. Problematic feed is ["
							+ factFeed.getName() + "]!");
		}
	}

	private FeedProcessor createFeedCompletionProcessor() {
		final FeedProcessor processor = null;
		if (factFeed.getEvents() != null && factFeed.getEvents().getAfterFeedProcessingCompletion() != null
				&& !factFeed.getEvents().getAfterFeedProcessingCompletion().isEmpty()) {
			log.debug("Found {} to be executed on feed completion for {}", factFeed.getEvents().getAfterFeedProcessingCompletion(),
					factFeed.getName());
			return new FeedProcessor(factFeed, config, "FeedCompletionProcessor", factFeed.getEvents().getAfterFeedProcessingCompletion());
		} else {
			log.debug("Did not find anything to execute on feed completion for feed {}", factFeed.getName());
		}
		return processor;
	}

	private void processStreamWithCompletion(final InputStream inputStream, final Map<String, String> globalAttributes) {
		try {
			final int numberOfLineProcessed = textFileReaderComponent.process(inputStream, globalAttributes);
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED, String.valueOf(numberOfLineProcessed));
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG, "S");
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION, "");
		} catch (final Exception exc) {
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION, exc.getMessage());
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG, "F");
			globalAttributes.put(BaukConstants.COMPLETION_ATTRIBUTE_NUMBER_OF_ROWS_IN_FEED, "0");
			throw exc;
		} finally {
			if (isDebugEnabled) {
				log.debug("Global attributes (including completion attributes) for {} are {}", factFeed.getName(), globalAttributes);
			}
			feedCompletionProcessor.process(globalAttributes);
		}
	}

	private void processInputStream(final InputStream inputStream, final BaukFile file) {
		final long start = System.currentTimeMillis();
		if (isDebugEnabled) {
			log.debug("Received filePath={}", file.getFullFilePath());
		}
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(file);
		try {
			if (beforeFeedProcessingProcessor != null) {
				beforeFeedProcessingProcessor.process(globalAttributes);
			}
			if (feedCompletionProcessor == null) {
				textFileReaderComponent.process(inputStream, globalAttributes);
			} else {
				this.processStreamWithCompletion(inputStream, globalAttributes);
			}
		} catch (final Exception exc) {
			if (executeRollbackSequence) {
				log.info("Executing rollback commands for feed {}", factFeed.getName());
				feedProcessingFailureCommandsExecutor.executeBaukCommandSequence(globalAttributes,
						"rollback command sequence for feed " + factFeed.getName());
			}
			throw exc;
		} finally {
			IOUtils.closeQuietly(inputStream);
			final long total = System.currentTimeMillis() - start;
			if (inputFeedsProcessed != null) {
				inputFeedsProcessed.mark();
			}
			if (inputFeedProcessingTime != null) {
				inputFeedProcessingTime.update(total);
			}
			if (isDebugEnabled) {
				log.debug("Finished processing [{}] in {}ms", file.getFullFilePath(), total);
			}
		}
	}

	private String getBulkOutputFileName(final String inputFeedFileName) {
		return factFeed.getName() + "_" + StringUtil.getFileNameWithoutExtension(inputFeedFileName) + "." + fileExtension;
	}

	private void clearImplicitAttributes() {
		implicitAttributes.clear();
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID, processorId);
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_FEED_PROCESSOR_ID, multiInstanceProcessorId);
		BaukUtil.populateEngineImplicitAttributes(implicitAttributes);
	}

	private Map<String, String> createImplicitGlobalAttributes(final BaukFile file) {
		if (isDebugEnabled) {
			log.debug("All date/time values will be in format {}", BaukConstants.TIMESTAMP_TO_DATE_FORMAT);
		}
		this.clearImplicitAttributes();
		final Map<String, String> attributes = implicitAttributes;
		final String fileNameOnly = file.getFileNameOnly();
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME, fileNameOnly);
		try {
			final Map<String, String> parsedAttributesFromFeedFileName = feedFileNameProcessor.parseFeedFileName(fileNameOnly);
			if (parsedAttributesFromFeedFileName != null && !parsedAttributesFromFeedFileName.isEmpty()) {
				attributes.putAll(parsedAttributesFromFeedFileName);
			} else {
				log.info("Null or empty attributes returned by feed file name parser!");
			}
		} catch (final Exception exc) {
			log.error("Exception while parsing feed file name using processor. Passed feed file name is {}. Details {}", fileNameOnly,
					exc.getMessage());
			throw exc;
		}
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH, file.getFullFilePath());
		final DateTime fileLastModified = new DateTime(file.getLastModifiedTime(), DEFAULT_CHRONOLOGY);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP, "" + file.getLastModifiedTime());
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_DATE_TIME, DATE_FORMATTER.print(fileLastModified));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE, String.valueOf(file.getSize()));
		final DateTime now = new DateTime(DEFAULT_CHRONOLOGY);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_TIMESTAMP, "" + now.getMillis());
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_DATE_TIME, DATE_FORMATTER.print(now));
		final String bulkOutputFileNameOnly = this.getBulkOutputFileName(fileNameOnly);
		final String bulkOutputFileFullPath = bulkOutDirectory + "/" + bulkOutputFileNameOnly;
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, bulkOutputFileNameOnly);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH, bulkOutputFileFullPath);
		if (isDebugEnabled) {
			log.debug("Created global attributes {}", attributes);
		}
		return attributes;
	}

}
