package com.threeglav.bauk.camel;

import gnu.trove.map.hash.THashMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.camel.bulk.BulkFileSubmissionRecorder;
import com.threeglav.bauk.model.AfterBulkLoadSuccess;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileProcessor extends ConfigAware implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final AtomicInteger COUNTER = new AtomicInteger(0);

	private final Meter successfullyLoadedBulkFilesMeter;
	private final String dbStringLiteral;
	private final String dbStringEscapeLiteral;
	private final BulkFileSubmissionRecorder fileSubmissionRecorder;
	private final String statementDescription;
	private final String processorId;
	private final boolean isDebugEnabled;
	private final boolean outputProcessingStatistics;
	private static final AtomicLong TOTAL_BULK_LOADED_FILES = new AtomicLong(0);

	public BulkFileProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		dbStringEscapeLiteral = this.getConfig().getDatabaseStringEscapeLiteral();
		fileSubmissionRecorder = new BulkFileSubmissionRecorder();
		successfullyLoadedBulkFilesMeter = MetricsUtil.createMeter("Successfully loaded bulk files");
		statementDescription = "BulkFileProcessor for " + this.getFactFeed().getName();
		processorId = String.valueOf(COUNTER.incrementAndGet());
		isDebugEnabled = log.isDebugEnabled();
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME,
				false);
		log.debug("Current processing id is {}", processorId);
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final boolean shutdownStarted = BaukUtil.shutdownStarted();
		if (shutdownStarted) {
			log.warn("Shutdown started. Will not accept any more files for processing!");
			return;
		}
		final Map<String, String> globalAttributes = this.createImplicitGlobalAttributes(exchange);
		final String bulkLoadFilePath = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH);
		log.debug("Processing {}", bulkLoadFilePath);
		final String insertStatement = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsertStatement();
		if (StringUtil.isEmpty(insertStatement)) {
			log.info("Could not find insert statement for bulk loading files!");
			return;
		}
		final String fileNameOnly = (String) exchange.getIn().getHeader("CamelFileNameOnly");
		final boolean alreadySubmitted = fileSubmissionRecorder.wasAlreadySubmitted(fileNameOnly);
		if (alreadySubmitted) {
			log.debug("File [{}] was already submitted for loading previously. Will set {} to {}",
					BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
			globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_TRUE_VALUE);
		} else {
			log.debug("File [{}] was not submitted for loading before. Will set {} to {}",
					BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
			globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_ALREADY_SUBMITTED, BaukConstants.ALREADY_SUBMITTED_FALSE_VALUE);
			fileSubmissionRecorder.recordSubmissionAttempt(fileNameOnly);
		}
		this.executeBulkLoadingCommandSequence(globalAttributes, insertStatement);
		fileSubmissionRecorder.deleteLoadedFile(fileNameOnly);
	}

	private void executeBulkLoadingCommandSequence(final Map<String, String> globalAttributes, final String insertStatement) {
		final long start = System.currentTimeMillis();
		if (isDebugEnabled) {
			log.debug("Insert statement for bulk loading files is {}", insertStatement);
		}
		final String replacedStatement = StringUtil.replaceAllAttributes(insertStatement, globalAttributes, dbStringLiteral, dbStringEscapeLiteral);
		if (isDebugEnabled) {
			log.debug("Statement to execute is {}", replacedStatement);
		}
		try {
			this.getDbHandler().executeInsertOrUpdateStatement(replacedStatement, statementDescription);
		} catch (final Exception exc) {
			log.error(
					"Exception while trying to load bulk file using JDBC. Fully prepared load statement is {}. Available context attributes are {}",
					replacedStatement, globalAttributes);
			throw exc;
		}
		if (isDebugEnabled) {
			log.debug("Successfully executed statement {}", replacedStatement);
		}
		final long totalBulkLoadedFiles = TOTAL_BULK_LOADED_FILES.incrementAndGet();
		if (outputProcessingStatistics) {
			final long total = System.currentTimeMillis() - start;
			final float totalSec = total / 1000;
			final String message = "Bulk loading of file took in total " + total + "ms (" + totalSec + " sec). In total bulk loaded "
					+ totalBulkLoadedFiles + " files so far";
			BaukUtil.logBulkLoadEngineMessage(message);
		}
		this.executeOnSuccessBulkLoad(globalAttributes);
		if (successfullyLoadedBulkFilesMeter != null) {
			successfullyLoadedBulkFilesMeter.mark();
		}
	}

	private void executeOnSuccessBulkLoad(final Map<String, String> globalAttributes) {
		final AfterBulkLoadSuccess onBulkLoadSuccess = this.getFactFeed().getBulkLoadDefinition().getAfterBulkLoadSuccess();
		if (onBulkLoadSuccess != null) {
			if (onBulkLoadSuccess.getSqlStatements() != null) {
				if (isDebugEnabled) {
					log.debug("Will execute on-success sql actions. Global attributes {}", globalAttributes);
				}
				for (final String sqlStatement : onBulkLoadSuccess.getSqlStatements()) {
					final String replaced = StringUtil.replaceAllAttributes(sqlStatement, globalAttributes, dbStringLiteral, dbStringEscapeLiteral);
					if (isDebugEnabled) {
						log.debug("Trying to execute statement [{}]", replaced);
					}
					this.getDbHandler().executeInsertOrUpdateStatement(replaced, statementDescription);
					if (isDebugEnabled) {
						log.debug("Successfully finished execution of [{}]", replaced);
					}
				}
			}
		}
	}

	private Map<String, String> createImplicitGlobalAttributes(final Exchange exchange) {
		final Map<String, String> attributes = new THashMap<String, String>();
		final String fileNameOnly = (String) exchange.getIn().getHeader("CamelFileNameOnly");
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FILE_NAME, fileNameOnly);
		final String inputFileAbsolutePath = (String) exchange.getIn().getHeader("CamelFileAbsolutePath");
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_FULL_FILE_PATH, StringUtil.fixFilePath(inputFileAbsolutePath));
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_RECEIVED_TIMESTAMP,
				String.valueOf(exchange.getIn().getHeader("CamelFileLastModified")));
		attributes.put(com.threeglav.bauk.BaukConstants.IMPLICIT_ATTRIBUTE_BULK_FILE_PROCESSED_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
		attributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_PROCESSOR_ID, processorId);
		if (isDebugEnabled) {
			log.debug("Created global attributes {}", attributes);
		}
		return attributes;
	}

}
