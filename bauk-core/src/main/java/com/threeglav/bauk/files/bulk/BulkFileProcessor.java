package com.threeglav.bauk.files.bulk;

import gnu.trove.map.hash.THashMap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.BaukEngineConfigurationConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.command.BaukCommandsExecutor;
import com.threeglav.bauk.dimension.db.DataSourceProvider;
import com.threeglav.bauk.files.BaukFile;
import com.threeglav.bauk.files.FileProcessor;
import com.threeglav.bauk.model.BaukCommand;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileProcessor extends ConfigAware implements FileProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private final boolean deleteBulkFileAfterLoading;

	private final BulkFileSubmissionRecorder fileSubmissionRecorder;
	private final String processorId;
	private final boolean isDebugEnabled;
	private final boolean outputProcessingStatistics;
	private final boolean shouldExecuteOnBulkLoadSuccess;
	private final boolean shouldExecuteOnBulkLoadFailure;
	private String currentThreadName;
	private final boolean recordFileSubmissionAttempts;
	private final ArrayList<BaukCommand> bulkInsertCommands;
	private final BaukCommandsExecutor bulkInsertCommandsExecutor;
	private BaukCommandsExecutor bulkLoadSuccessCommandsExecutor;
	private BaukCommandsExecutor bulkLoadFailureCommandsExecutor;

	public BulkFileProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		fileSubmissionRecorder = new BulkFileSubmissionRecorder();
		processorId = String.valueOf(COUNTER.incrementAndGet());
		isDebugEnabled = log.isDebugEnabled();
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME, false);
		log.debug("Current processing id is {}", processorId);
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
		deleteBulkFileAfterLoading = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.DELETE_BULK_LOADED_FILES_PARAM_NAME,
				true);
		if (!deleteBulkFileAfterLoading) {
			log.info("Will not delete bulk files after loading!");
		}
		recordFileSubmissionAttempts = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BULK_FILE_RECORD_FILE_SUBMISSIONS,
				true);
		if (!recordFileSubmissionAttempts) {
			log.warn("Engine will not record bulk file submission attempts.");
		} else {
			log.info("Engine will record all bulk file submission attempts.");
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
		this.recordFileLoadingAttempt(globalAttributes, fileNameOnly);
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
		if (recordFileSubmissionAttempts) {
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
	}

	private void executeBulkLoadingCommandSequence(final Map<String, String> globalAttributes) {
		final long start = System.currentTimeMillis();
		final Connection connection = null;
		final PreparedStatement preparedStatement = null;
		try {
			bulkInsertCommandsExecutor.executeBaukCommandSequence(globalAttributes, "Bulk insert for " + this.getFactFeed().getName());
			EngineRegistry.registerSuccessfulBulkFileLoad();
		} catch (final Exception exc) {
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
		}
		if (outputProcessingStatistics) {
			final long totalBulkLoadedFiles = EngineRegistry.getSuccessfulBulkFilesCount();
			final long totalMillis = System.currentTimeMillis() - start;
			final float totalSec = totalMillis / 1000;
			String message = this.getCurrentThreadName() + " - Bulk loading of file took " + totalMillis + "ms";
			if (totalMillis > 1000) {
				message += " (" + totalSec + " sec)";
			}
			message += ". In total bulk loaded " + totalBulkLoadedFiles + " files so far";
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
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID, processorId);
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, fileNameOnly);
		final String inputFileAbsolutePath = file.getFullFilePath();
		implicitAttributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH,
				StringUtil.fixFilePath(inputFileAbsolutePath));
		implicitAttributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_FOR_PROCESSING_TIMESTAMP,
				String.valueOf(file.getLastModifiedTime()));
		implicitAttributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FINISHED_PROCESSING_TIMESTAMP,
				String.valueOf(System.currentTimeMillis()));
		implicitAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_PROCESSOR_ID, processorId);
		if (isDebugEnabled) {
			log.debug("Created global attributes {}", implicitAttributes);
		}
		return implicitAttributes;
	}

}
