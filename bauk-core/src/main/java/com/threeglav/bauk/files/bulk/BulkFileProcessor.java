package com.threeglav.bauk.files.bulk;

import gnu.trove.map.hash.THashMap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

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
import com.threeglav.bauk.util.StatefulAttributeReplacer;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileProcessor extends ConfigAware implements FileProcessor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private final boolean deleteBulkFileAfterLoading;

	private final String dbStringLiteral;
	private final String dbStringEscapeLiteral;
	private final BulkFileSubmissionRecorder fileSubmissionRecorder;
	private final String processorId;
	private final boolean isDebugEnabled;
	private final boolean outputProcessingStatistics;
	private final String bulkLoadStatement;
	private final boolean shouldExecuteOnBulkLoadSuccess;
	private final BaukCommandsExecutor commandsExecutor;
	private final boolean shouldExecuteOnBulkLoadFailure;
	private final StatefulAttributeReplacer statefulReplacer;
	private final DataSource dataSource;
	private String currentThreadName;
	private final boolean recordFileSubmissionAttempts;

	public BulkFileProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		dbStringEscapeLiteral = this.getConfig().getDatabaseStringEscapeLiteral();
		fileSubmissionRecorder = new BulkFileSubmissionRecorder();
		processorId = String.valueOf(COUNTER.incrementAndGet());
		isDebugEnabled = log.isDebugEnabled();
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME, false);
		log.debug("Current processing id is {}", processorId);
		final String insertStatement = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsertStatement();
		if (StringUtil.isEmpty(insertStatement)) {
			throw new IllegalStateException("Could not find insert statement for bulk loading files - for feed " + this.getFactFeed().getName() + "!");
		}
		bulkLoadStatement = insertStatement;
		final ArrayList<BaukCommand> onBulkLoadSuccess = this.getFactFeed().getBulkLoadDefinition().getAfterBulkLoadSuccess();
		shouldExecuteOnBulkLoadSuccess = onBulkLoadSuccess != null && !onBulkLoadSuccess.isEmpty();
		final ArrayList<BaukCommand> onBulkLoadFail = this.getFactFeed().getBulkLoadDefinition().getOnBulkLoadFailure();
		shouldExecuteOnBulkLoadFailure = onBulkLoadFail != null && !onBulkLoadFail.isEmpty();
		commandsExecutor = new BaukCommandsExecutor(factFeed, config);
		statefulReplacer = new StatefulAttributeReplacer(bulkLoadStatement, dbStringLiteral, dbStringEscapeLiteral);
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
		dataSource = DataSourceProvider.getDataSource(this.getConfig());
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
					log.debug("File [{}] was already submitted for loading previously. Will set {} to {}",
							BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
				}
				globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
			} else {
				if (isDebugEnabled) {
					log.debug("File [{}] was not submitted for loading before. Will set {} to {}",
							BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
				}
				globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
				fileSubmissionRecorder.recordSubmissionAttempt(fileNameOnly);
			}
		}
	}

	private void executeBulkLoadingCommandSequence(final Map<String, String> globalAttributes) {
		final long start = System.currentTimeMillis();
		if (isDebugEnabled) {
			log.debug("Insert statement for bulk loading files is {}", bulkLoadStatement);
		}
		final String replacedStatement = statefulReplacer.replaceAttributes(globalAttributes);
		if (isDebugEnabled) {
			log.debug("Statement to execute is {}", replacedStatement);
		}
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try {
			connection = dataSource.getConnection();
			preparedStatement = connection.prepareStatement(replacedStatement);
			preparedStatement.execute();
			EngineRegistry.registerSuccessfulBulkFileLoad();
		} catch (final Exception exc) {
			EngineRegistry.registerFailedBulkFile();
			log.error(
					"Exception while trying to load bulk file using JDBC. Fully prepared load statement is {}. Available context attributes are {}",
					replacedStatement, globalAttributes);
			if (shouldExecuteOnBulkLoadFailure) {
				log.info("Bulk loading failed. Executing rollback procedure as configured...");
				commandsExecutor.executeBaukCommandSequence(this.getFactFeed().getBulkLoadDefinition().getOnBulkLoadFailure(), globalAttributes,
						"Bulk-load-failure commands for " + this.getFactFeed().getName());
			}
			throw new RuntimeException("Exception while executing " + replacedStatement, exc);
		} finally {
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
		}
		if (isDebugEnabled) {
			log.debug("Successfully executed statement {}", replacedStatement);
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
			this.executeOnSuccessBulkLoad(globalAttributes);
		}
	}

	private void executeOnSuccessBulkLoad(final Map<String, String> globalAttributes) {
		final ArrayList<BaukCommand> onBulkLoadSuccess = this.getFactFeed().getBulkLoadDefinition().getAfterBulkLoadSuccess();
		if (isDebugEnabled) {
			log.debug("Will execute on-success sql actions. Global attributes {}", globalAttributes);
		}
		commandsExecutor.executeBaukCommandSequence(onBulkLoadSuccess, globalAttributes, "On bulk load success command-sequence");
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
