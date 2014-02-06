package com.threeglav.bauk.files.feed;

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
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.command.BaukCommandsExecutor;
import com.threeglav.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.bauk.feed.BeforeFeedProcessingProcessor;
import com.threeglav.bauk.feed.DefaultFeedFileNameProcessor;
import com.threeglav.bauk.feed.FeedCompletionProcessor;
import com.threeglav.bauk.feed.FeedFileNameProcessor;
import com.threeglav.bauk.feed.TextFileReaderComponent;
import com.threeglav.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.bauk.feed.processing.SingleThreadedFeedDataProcessor;
import com.threeglav.bauk.files.BaukFile;
import com.threeglav.bauk.files.FileProcessingErrorHandler;
import com.threeglav.bauk.files.FileProcessor;
import com.threeglav.bauk.files.MoveFileErrorHandler;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class FeedFileProcessor implements FileProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private static final Chronology DEFAULT_CHRONOLOGY = ISOChronology.getInstance();

	private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(BaukConstants.TIMESTAMP_TO_DATE_FORMAT);

	private final Map<String, String> implicitAttributes = new THashMap<String, String>(30);

	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private final Meter inputFeedsProcessed;
	private final Histogram inputFeedProcessingTime;
	private final TextFileReaderComponent textFileReaderComponent;
	private final FeedDataProcessor feedDataProcessor;
	private final FeedCompletionProcessor feedCompletionProcessor;
	private final BeforeFeedProcessingProcessor beforeFeedProcessingProcessor;
	private FeedFileNameProcessor feedFileNameProcessor;
	private String fileExtension;
	private final boolean isDebugEnabled;
	private final String processorId;
	private FileProcessingErrorHandler moveToArchiveFileProcessor;
	private final boolean executeRollbackSequence;
	private final BaukCommandsExecutor baukCommandsExec;
	private final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);

	public FeedFileProcessor(final FactFeed factFeed, final BaukConfiguration config, final String fileMask) {
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
		this.validate();
		final String cleanFileMask = StringUtil.replaceAllNonASCII(fileMask);
		feedDataProcessor = new SingleThreadedFeedDataProcessor(factFeed, config, cleanFileMask);
		textFileReaderComponent = new TextFileReaderComponent(this.factFeed, this.config, feedDataProcessor, cleanFileMask);
		feedCompletionProcessor = this.createFeedCompletionProcessor();
		beforeFeedProcessingProcessor = this.createBeforeFeedProcessingProcessor();
		inputFeedsProcessed = MetricsUtil.createMeter("(" + cleanFileMask + ") - processed files count");
		inputFeedProcessingTime = MetricsUtil.createHistogram("(" + cleanFileMask + ") - processing time (millis)");
		this.initializeFeedFileNameProcessor();
		processorId = String.valueOf(COUNTER.incrementAndGet());
		log.info("Number of instances is {}", processorId);
		isDebugEnabled = log.isDebugEnabled();
		final String archiveFolderPath = config.getArchiveDirectory();
		if (!StringUtil.isEmpty(archiveFolderPath)) {
			moveToArchiveFileProcessor = new MoveFileErrorHandler(archiveFolderPath);
			log.info("Will move all successfully processed files to {}", archiveFolderPath);
		}
		executeRollbackSequence = factFeed.getOnFeedProcessingFailure() != null && !factFeed.getOnFeedProcessingFailure().isEmpty();
		log.info("Will execute rollback commands for feed {} = {}", factFeed.getName(), executeRollbackSequence);
		baukCommandsExec = new BaukCommandsExecutor(factFeed, config);
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
			IOUtils.closeQuietly(inputStream);
		}
		// if testing throughput no need to move or delete file
		if (!throughputTestingMode) {
			try {
				if (moveToArchiveFileProcessor != null) {
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

	private BeforeFeedProcessingProcessor createBeforeFeedProcessingProcessor() {
		BeforeFeedProcessingProcessor processor = null;
		if (factFeed.getBeforeFeedProcessing() != null && !factFeed.getBeforeFeedProcessing().isEmpty()) {
			log.debug("Will perform before feed processing for {}", factFeed.getName());
			processor = new BeforeFeedProcessingProcessor(factFeed, config);
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
	}

	private void validate() {
		if (StringUtil.isEmpty(config.getBulkOutputDirectory())) {
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
		if (isEmptyExtension && bulkDefinition.getOutputType() != BulkLoadDefinitionOutputType.NONE) {
			throw new IllegalStateException(
					"Extension for recognizing bulk output file is required to be specified in configuration file because output will be generated Feed "
							+ factFeed.getName() + "!");
		}
	}

	private FeedCompletionProcessor createFeedCompletionProcessor() {
		final FeedCompletionProcessor processor = null;
		if (factFeed.getAfterFeedProcessingCompletion() != null && !factFeed.getAfterFeedProcessingCompletion().isEmpty()) {
			log.debug("Found {} to be executed on feed completion for {}", factFeed.getAfterFeedProcessingCompletion(), factFeed.getName());
			return new FeedCompletionProcessor(factFeed, config);
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
		}
		if (isDebugEnabled) {
			log.debug("Global attributes (including completion attributes) for {} are {}", factFeed.getName(), globalAttributes);
		}
		feedCompletionProcessor.process(globalAttributes);
	}

	private void processInputStream(final InputStream inputStream, final BaukFile file) {
		final long start = System.currentTimeMillis();
		if (isDebugEnabled) {
			log.debug("Received filePath={}", file.getFullFilePath());
		}
		try {
			final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(file);
			if (beforeFeedProcessingProcessor != null) {
				beforeFeedProcessingProcessor.processAndGenerateNewAttributes(globalAttributes);
			}
			if (feedCompletionProcessor == null) {
				textFileReaderComponent.process(inputStream, globalAttributes);
			} else {
				this.processStreamWithCompletion(inputStream, globalAttributes);
			}
		} catch (final Exception exc) {
			if (executeRollbackSequence) {
				log.info("Executing rollback commands for feed {}", factFeed.getName());
				baukCommandsExec.executeBaukCommandSequence(factFeed.getOnFeedProcessingFailure(), implicitAttributes,
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

	private String getOutputFilePath(final String inputFileName) {
		if (isDebugEnabled) {
			log.debug("Will use {} file extension for bulk files", fileExtension);
		}
		return config.getBulkOutputDirectory() + "/" + factFeed.getName() + "_" + StringUtil.getFileNameWithoutExtension(inputFileName) + "."
				+ fileExtension;
	}

	private void clearImplicitAttributes() {
		implicitAttributes.clear();
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID, processorId);
	}

	private Map<String, String> createImplicitGlobalAttributes(final BaukFile file) {
		if (isDebugEnabled) {
			log.debug("All date/time values will be in format {}", BaukConstants.TIMESTAMP_TO_DATE_FORMAT);
		}
		this.clearImplicitAttributes();
		final Map<String, String> attributes = implicitAttributes;
		final String fileNameOnly = file.getFileNameOnly();
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_NAME, fileNameOnly);
		final Map<String, String> parsedAttributesFromFeedFileName = feedFileNameProcessor.parseFeedFileName(fileNameOnly);
		if (parsedAttributesFromFeedFileName != null && !parsedAttributesFromFeedFileName.isEmpty()) {
			attributes.putAll(parsedAttributesFromFeedFileName);
		} else {
			log.info("Null or empty attributes returned by feed file name parser!");
		}
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FULL_FILE_PATH, file.getFullFilePath());
		final DateTime fileLastModified = new DateTime(file.getLastModifiedTime(), DEFAULT_CHRONOLOGY);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_TIMESTAMP, "" + file.getLastModifiedTime());
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_RECEIVED_DATE_TIME, DATE_FORMATTER.print(fileLastModified));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_INPUT_FEED_FILE_SIZE, String.valueOf(file.getSize()));
		final DateTime now = new DateTime(DEFAULT_CHRONOLOGY);
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_TIMESTAMP, "" + now.getMillis());
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_STARTED_DATE_TIME, DATE_FORMATTER.print(now));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH, this.getOutputFilePath(fileNameOnly));
		if (isDebugEnabled) {
			log.debug("Created global attributes {}", attributes);
		}
		return attributes;
	}

}
