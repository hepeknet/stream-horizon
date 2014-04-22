package com.threeglav.sh.bauk.files.bulk;

import gnu.trove.map.hash.THashMap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.command.BaukCommandsExecutor;
import com.threeglav.sh.bauk.dimension.db.DataSourceProvider;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.feed.DefaultFeedFileNameProcessor;
import com.threeglav.sh.bauk.feed.FeedFileNameProcessor;
import com.threeglav.sh.bauk.files.BaukFile;
import com.threeglav.sh.bauk.files.InputFeedProcessor;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class BulkFileProcessor extends ConfigAware implements InputFeedProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private final boolean deleteBulkFileAfterLoading;

	private BulkFileSubmissionRecorder fileSubmissionRecorder;
	private final String processorId;
	private final String partitionedProcessorId;
	private final String multiInstanceProcessorId;
	private final boolean isDebugEnabled;
	private final boolean outputProcessingStatistics;
	private final boolean shouldExecuteOnBulkLoadSuccess;
	private final boolean shouldExecuteOnBulkLoadFailure;
	private final boolean shouldExecuteOnBulkLoadCompletion;
	private FeedFileNameProcessor feedFileNameProcessor;
	private String currentThreadName;
	private final boolean recordFileSubmissionAttempts;
	private final ArrayList<BaukCommand> bulkInsertCommands;
	private final BaukCommandsExecutor bulkInsertCommandsExecutor;
	private BaukCommandsExecutor bulkLoadSuccessCommandsExecutor;
	private BaukCommandsExecutor bulkLoadFailureCommandsExecutor;
	private BaukCommandsExecutor bulkLoadCompletionCommandsExecutor;
	private final FactFeed factFeed;

	public BulkFileProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		this.factFeed = factFeed;
		final int currentCounterValue = COUNTER.getAndIncrement();
		processorId = String.valueOf(currentCounterValue);
		final int calculatedPartitionedProcId = this.calculateCurrentThreadId(currentCounterValue);
		if (calculatedPartitionedProcId < 0) {
			partitionedProcessorId = null;
		} else {
			partitionedProcessorId = String.valueOf(calculatedPartitionedProcId);
		}
		isDebugEnabled = log.isDebugEnabled();
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME, false);
		log.debug("Current processing id is {}", processorId);
		final boolean isMultiInstance = ConfigurationProperties.isConfiguredPartitionedMultipleInstances();
		if (isMultiInstance) {
			final int multiInst = ConfigurationProperties.calculateMultiInstanceFeedProcessorId(currentCounterValue, this.factFeed);
			multiInstanceProcessorId = String.valueOf(multiInst);
			log.info("Successfully set multi instance feed thread identifier to {}", multiInst);
		} else {
			multiInstanceProcessorId = "-1";
		}
		bulkInsertCommands = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert();
		if (bulkInsertCommands == null || bulkInsertCommands.isEmpty()) {
			throw new IllegalStateException("Could not find any bulk load insert statements for bulk loading files - for feed "
					+ this.getFactFeed().getName() + "!");
		}
		bulkInsertCommandsExecutor = new BaukCommandsExecutor(factFeed, config, bulkInsertCommands);
		final ArrayList<BaukCommand> onBulkLoadSuccess = this.getFactFeed().getBulkLoadDefinition().getAfterBulkLoadSuccess();
		shouldExecuteOnBulkLoadSuccess = onBulkLoadSuccess != null && !onBulkLoadSuccess.isEmpty();
		if (shouldExecuteOnBulkLoadSuccess) {
			bulkLoadSuccessCommandsExecutor = new BaukCommandsExecutor(factFeed, config, onBulkLoadSuccess);
		}
		final ArrayList<BaukCommand> onBulkLoadFail = this.getFactFeed().getBulkLoadDefinition().getOnBulkLoadFailure();
		shouldExecuteOnBulkLoadFailure = onBulkLoadFail != null && !onBulkLoadFail.isEmpty();
		if (shouldExecuteOnBulkLoadFailure) {
			bulkLoadFailureCommandsExecutor = new BaukCommandsExecutor(factFeed, config, onBulkLoadFail);
		}
		final ArrayList<BaukCommand> onBulkLoadCompletion = this.getFactFeed().getBulkLoadDefinition().getOnBulkLoadCompletion();
		shouldExecuteOnBulkLoadCompletion = onBulkLoadCompletion != null && !onBulkLoadCompletion.isEmpty();
		if (shouldExecuteOnBulkLoadCompletion) {
			bulkLoadCompletionCommandsExecutor = new BaukCommandsExecutor(factFeed, config, onBulkLoadCompletion);
		}
		deleteBulkFileAfterLoading = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.DELETE_BULK_LOADED_FILES_PARAM_NAME,
				true);
		if (!deleteBulkFileAfterLoading) {
			log.info("Will not delete bulk files after loading!");
		}
		recordFileSubmissionAttempts = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BULK_FILE_RECORD_FILE_SUBMISSIONS,
				false);
		if (!recordFileSubmissionAttempts) {
			log.warn("Engine will not record bulk file submission attempts.");
		} else {
			fileSubmissionRecorder = new BulkFileSubmissionRecorder();
			log.info("Engine will record all bulk file submission attempts.");
		}
		this.initializeFeedFileNameProcessor();
	}

	private int calculateCurrentThreadId(final int currentCounterValue) {
		int currentCounter = -1;
		log.debug("Checking whether to turn on bulk loading thread partitioning");
		final int bulkLoadThreadPartitionCount = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.BULK_LOADING_THREADS_PARTITION_COUNT, -1);
		if (bulkLoadThreadPartitionCount > 0) {
			final int totalBulkLoadingThreads = factFeed.getThreadPoolSettings().getDatabaseProcessingThreadCount();
			if (totalBulkLoadingThreads > bulkLoadThreadPartitionCount) {
				log.info("Will turn on bulk loading thread partitioning. {} bulk loading threads will be grouped in {} partitions",
						totalBulkLoadingThreads, bulkLoadThreadPartitionCount);
				currentCounter = BaukUtil.calculatePartition(totalBulkLoadingThreads, bulkLoadThreadPartitionCount, currentCounter);
			} else {
				log.warn(
						"Value for {} can not be higher than the total number of DB threads. Will not turn on bulk loading thread partitioning. Found value partition count {}",
						BaukEngineConfigurationConstants.BULK_LOADING_THREADS_PARTITION_COUNT, bulkLoadThreadPartitionCount);
			}
		} else {
			log.info("Will not turn on bulk load thread partitioning because {} is set to {} - must be positive integer",
					BaukEngineConfigurationConstants.BULK_LOADING_THREADS_PARTITION_COUNT, bulkLoadThreadPartitionCount);
		}
		return currentCounter;
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

	private final String getCurrentThreadName() {
		if (currentThreadName == null) {
			currentThreadName = Thread.currentThread().getName();
		}
		return currentThreadName;
	}

	@Override
	public void process(final BaukFile file) {
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(file);
		final String bulkLoadFilePath = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		if (isDebugEnabled) {
			log.debug("Bulk loading {}", bulkLoadFilePath);
		}
		final String fileNameOnly = file.getFileNameOnly();
		if (recordFileSubmissionAttempts) {
			this.recordFileLoadingAttempt(globalAttributes, fileNameOnly);
		}
		this.executeBulkLoadingCommandSequence(globalAttributes);
		if (recordFileSubmissionAttempts) {
			fileSubmissionRecorder.deleteLoadedFile(fileNameOnly);
		}
		if (deleteBulkFileAfterLoading) {
			try {
				file.delete();
			} catch (final IOException ie) {
				log.error("Exception while deleting bulk file", ie);
			}
		}
	}

	private void recordFileLoadingAttempt(final Map<String, String> globalAttributes, final String fileNameOnly) {
		final boolean alreadySubmitted = fileSubmissionRecorder.wasAlreadySubmitted(fileNameOnly);
		if (alreadySubmitted) {
			if (isDebugEnabled) {
				log.debug("File [{}] was already submitted for loading previously. Will set {} to {}", fileNameOnly,
						BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
			}
			globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
		} else {
			if (isDebugEnabled) {
				log.debug("File [{}] was not submitted for loading before. Will set {} to {}", fileNameOnly,
						BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
			}
			globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
			fileSubmissionRecorder.recordSubmissionAttempt(fileNameOnly);
		}
	}

	private void executeBulkLoadingCommandSequence(final Map<String, String> globalAttributes) {
		final long start = System.currentTimeMillis();
		final Connection connection = null;
		final PreparedStatement preparedStatement = null;
		try {
			bulkInsertCommandsExecutor.executeBaukCommandSequence(globalAttributes, "Bulk insert for " + this.getFactFeed().getName());
			EngineRegistry.registerSuccessfulBulkFileLoad();
			globalAttributes
					.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FINISHED_PROCESSING_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
			globalAttributes.put(BaukConstants.BULK_COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG, "S");
		} catch (final Exception exc) {
			globalAttributes.put(BaukConstants.BULK_COMPLETION_ATTRIBUTE_SUCCESS_FAILURE_FLAG, "F");
			globalAttributes.put(BaukConstants.BULK_COMPLETION_ATTRIBUTE_ERROR_DESCRIPTION, exc.getMessage());
			EngineRegistry.registerFailedBulkFile();
			log.error("Exception while trying to load bulk file using JDBC. Available context attributes are {}", globalAttributes);
			if (shouldExecuteOnBulkLoadFailure) {
				log.info("Bulk loading failed. Executing rollback procedure as configured...");
				bulkLoadFailureCommandsExecutor.executeBaukCommandSequence(globalAttributes, "Bulk-load-failure commands for "
						+ this.getFactFeed().getName());
			}
			throw new RuntimeException("Exception while executing bulk insert for " + this.getFactFeed().getName(), exc);
		} finally {
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
			if (shouldExecuteOnBulkLoadCompletion) {
				bulkLoadCompletionCommandsExecutor.executeBaukCommandSequence(globalAttributes, "Bulk-load-completion commands for "
						+ this.getFactFeed().getName());
			}
		}
		if (outputProcessingStatistics) {
			final long totalMillis = System.currentTimeMillis() - start;
			final float totalSec = totalMillis / 1000f;
			String message = this.getCurrentThreadName() + " - Bulk loading of file took " + totalMillis + "ms";
			if (totalMillis > 1000) {
				message += " (" + BaukUtil.DEC_FORMAT.format(totalSec) + " sec)";
			}
			BaukUtil.logBulkLoadEngineMessage(message);
		}
		if (shouldExecuteOnBulkLoadSuccess) {
			if (isDebugEnabled) {
				log.debug("Will execute on-success sql actions. Global attributes {}", globalAttributes);
			}
			bulkLoadSuccessCommandsExecutor.executeBaukCommandSequence(globalAttributes, "On bulk load success command-sequence");
		}
	}

	private Map<String, String> createImplicitGlobalAttributes(final BaukFile file) {
		final String fileNameOnly = file.getFileNameOnly();
		final Map<String, String> implicitAttributes = new THashMap<String, String>();
		BaukUtil.populateEngineImplicitAttributes(implicitAttributes);
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID, processorId);
		if (partitionedProcessorId != null) {
			implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_PARTITIONED_ID, partitionedProcessorId);
		}
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_MULTI_INSTANCE_BULK_PROCESSOR_ID, multiInstanceProcessorId);
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, fileNameOnly);
		final String inputFileAbsolutePath = file.getFullFilePath();
		try {
			final Map<String, String> parsedAttributesFromFeedFileName = feedFileNameProcessor.parseFeedFileName(fileNameOnly);
			if (parsedAttributesFromFeedFileName != null && !parsedAttributesFromFeedFileName.isEmpty()) {
				implicitAttributes.putAll(parsedAttributesFromFeedFileName);
			} else {
				log.info("Null or empty attributes returned by feed file name parser!");
			}
		} catch (final Exception exc) {
			log.error("Exception while parsing feed file name using processor. Passed feed file name is {}. Details {}", fileNameOnly,
					exc.getMessage());
			throw exc;
		}
		implicitAttributes.put(com.threeglav.sh.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH,
				StringUtil.fixFilePath(inputFileAbsolutePath));
		implicitAttributes.put(com.threeglav.sh.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_FOR_PROCESSING_TIMESTAMP,
				String.valueOf(file.getLastModifiedTime()));
		implicitAttributes.put(com.threeglav.sh.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_STARTED_PROCESSING_TIMESTAMP,
				String.valueOf(System.currentTimeMillis()));
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID, processorId);
		if (isDebugEnabled) {
			log.debug("Created global attributes {}", implicitAttributes);
		}
		return implicitAttributes;
	}

}
