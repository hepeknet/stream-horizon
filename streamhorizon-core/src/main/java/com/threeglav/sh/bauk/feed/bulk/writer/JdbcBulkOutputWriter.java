package com.threeglav.sh.bauk.feed.bulk.writer;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.util.StringUtils;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.dimension.db.DataSourceProvider;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukAttributeType;
import com.threeglav.sh.bauk.model.BaukCommand;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.CommandType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

public final class JdbcBulkOutputWriter extends AbstractBulkOutputWriter {

	private final String insertStatement;
	private final int[] sqlTypes;
	private PreparedStatement preparedStatement;
	private Connection connection;
	private final int warningThreshold;
	private final int batchSize;
	private int rowCounter = 0;
	private int perFeedRowCounter = 0;
	private final boolean outputProcessingStatistics;
	private String currentStatementWithReplacedValues;
	private final DataSource dataSource;
	private final StatefulAttributeReplacer statefulReplacer;
	private int batchCommitCounter;

	// used only for dumping batch update exception data, INFO level must be on
	private List<List<Object>> preparedStatementDebugData;

	public JdbcBulkOutputWriter(final Feed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukCommand> bulkInsertCommands = this.getFactFeed().getTarget().getBulkLoadInsert();
		if (bulkInsertCommands == null || bulkInsertCommands.isEmpty()) {
			throw new IllegalStateException("Could not find any bulk load insert statements for bulk loading files - for feed "
					+ this.getFactFeed().getName() + "!");
		} else if (bulkInsertCommands.size() > 1) {
			throw new IllegalStateException("Only one statement is allowed for jdbc bulk loading - for feed [" + this.getFactFeed().getName()
					+ "]! Currently have " + bulkInsertCommands.size() + " commands defined!");
		}
		final BaukCommand singleCommand = bulkInsertCommands.get(0);
		if (singleCommand.getType() != CommandType.SQL) {
			throw new IllegalArgumentException("For jdbc bulk loading command must be of type " + CommandType.SQL + ". Problematic feed is "
					+ this.getFactFeed().getName());
		}
		insertStatement = singleCommand.getCommand();
		if (StringUtil.isEmpty(insertStatement)) {
			throw new IllegalArgumentException("Unable to use jdbc bulk loader when insert statement is not specified");
		}
		final ArrayList<BaukAttribute> attributes = this.getFactFeed().getTargetFormatDefinition().getAttributes();
		if (attributes == null || attributes.isEmpty()) {
			throw new IllegalArgumentException("Attributes must not be null or empty");
		}
		sqlTypes = new int[attributes.size()];
		for (int i = 0; i < attributes.size(); i++) {
			final BaukAttribute attr = attributes.get(i);
			if (attr.getType() == null) {
				throw new IllegalArgumentException(
						"Unable to use jdbc bulk loader when attribute types are not declared for all declared attributes!");
			}
			sqlTypes[i] = this.convertTypeToInt(attr.getType());
			log.debug("For attribute at position {} will use sql type {}", i, attr.getType());
		}
		this.validate(attributes.size());
		warningThreshold = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
				BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);
		batchSize = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.JDBC_BULK_LOADING_BATCH_SIZE_PARAM_NAME,
				BaukEngineConfigurationConstants.JDBC_BULK_LOADING_BATCH_SIZE_DEFAULT);
		if (batchSize <= 0) {
			throw new IllegalArgumentException("JDBC batch size must not be <= 0");
		}
		log.info("Will use {} batch size for loading bulk data using JDBC", batchSize);
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME, false);
		dataSource = DataSourceProvider.getBulkJdbcDataSource(this.getConfig());
		statefulReplacer = new StatefulAttributeReplacer(insertStatement, this.getConfig().getDatabaseStringLiteral(), this.getConfig()
				.getDatabaseStringEscapeLiteral());
	}

	private void validate(final int attributesNumber) {
		final int occurenceOfPreparedStatementPlaceholders = StringUtils.countOccurrencesOf(insertStatement, "?");
		if (attributesNumber != occurenceOfPreparedStatementPlaceholders) {
			log.warn(
					"Found {} occurrences of ? in statement {} but found {} declared attributes! This is probably error in configuration! JDBC bulk loading might not work correctly!",
					occurenceOfPreparedStatementPlaceholders, insertStatement, attributesNumber);
		} else {
			log.debug("{} has {} of ? - same as number of declared attributes", insertStatement, occurenceOfPreparedStatementPlaceholders);
		}
	}

	private int convertTypeToInt(final BaukAttributeType type) {
		if (type == BaukAttributeType.STRING) {
			return Types.VARCHAR;
		} else if (type == BaukAttributeType.INT) {
			return Types.INTEGER;
		} else if (type == BaukAttributeType.FLOAT) {
			return Types.FLOAT;
		} else {
			throw new IllegalArgumentException("Unsupported attribute type " + type);
		}
	}

	private void initializePreparedStatement(final Map<String, String> globalAttributes) {
		try {
			final String statement = statefulReplacer.replaceAttributes(globalAttributes);
			currentStatementWithReplacedValues = statement;
			if (isDebugEnabled) {
				log.debug("After replacing all attributes insert statement looks like {}. Global attributes are {}", statement, globalAttributes);
			}
			connection = dataSource.getConnection();
			connection.setAutoCommit(false);
			preparedStatement = connection.prepareStatement(statement);
			if (isDebugEnabled) {
				log.debug("Successfully initialized prepared statement {}", statement);
			}
		} catch (final Exception e) {
			throw new RuntimeException("Exception while initializing prepared statement for loading bulk values using jdbc", e);
		}
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		rowCounter = 0;
		perFeedRowCounter = 0;
		batchCommitCounter = 0;
		this.initializePreparedStatement(globalAttributes);
		if (isInfoEnabled) {
			preparedStatementDebugData = new ArrayList<>();
		}
	}

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			rowCounter++;
			perFeedRowCounter++;
			if (isDebugEnabled) {
				log.debug("Populating jdbc statement - row {} - row in feed {}", rowCounter, perFeedRowCounter);
			}
			List<Object> debugData = null;
			if (isInfoEnabled) {
				debugData = new LinkedList<Object>();
			}
			for (int i = 0; i < resolvedData.length; i++) {
				final int position = i + 1;
				preparedStatement.setObject(position, resolvedData[i], sqlTypes[i]);
				if (isInfoEnabled) {
					debugData.add(resolvedData[i]);
				}
				// help GC
				resolvedData[i] = null;
			}
			if (isInfoEnabled) {
				preparedStatementDebugData.add(debugData);
			}
			preparedStatement.addBatch();
			if (rowCounter == batchSize) {
				if (isDebugEnabled) {
					log.debug("Executing jdbc batch of size {} - row in feed {}", batchSize, perFeedRowCounter);
				}
				this.doExecuteJdbcBatch(globalAttributes);
			}
			if (isDebugEnabled) {
				log.debug("Successfully populated jdbc statement. Current row number {}", rowCounter);
			}
		} catch (final Exception e) {
			log.error("Exception writing jdbc output", e);
			log.error("Prepared statement was {}", currentStatementWithReplacedValues);
			throw new RuntimeException("Problem while populating batch in JDBC statement", e);
		}
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
		if (preparedStatement == null) {
			return;
		}
		if (isDebugEnabled) {
			log.debug("Closing feed, inserting remaining batched data. Attributes are {}", globalAttributes);
		}
		try {
			final boolean batchExecuted = this.doExecuteJdbcBatch(globalAttributes);
			if (batchExecuted) {
				EngineRegistry.registerSuccessfulBulkFileLoad();
				globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_FINISHED_PROCESSING_TIMESTAMP,
						String.valueOf(System.currentTimeMillis()));
				if (outputProcessingStatistics) {
					final String message = this.getCurrentThreadName() + " - Finished bulk loading data using JDBC";
					BaukUtil.logBulkLoadEngineMessage(message);
				}
			}
		} finally {
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
		}
	}

	private boolean doExecuteJdbcBatch(final Map<String, String> globalAttributes) {
		try {
			final long start = System.currentTimeMillis();
			if (batchCommitCounter == 0) {
				// we start counting jdbc insert at the first execute to database (per feed)
				globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_STARTED_PROCESSING_TIMESTAMP,
						String.valueOf(System.currentTimeMillis()));
			}
			if (connection == null || connection.isClosed() || preparedStatement == null || preparedStatement.isClosed()) {
				return false;
			}
			try {
				final int[] values = preparedStatement.executeBatch();
				connection.commit();
				batchCommitCounter++;
				if (isDebugEnabled) {
					int count = 0;
					for (int i = 0; i < values.length; i++) {
						count += values[i];
					}
					log.debug("Result of batch insert of bulk values was {} for total of {} rows", count, values.length);
				}
			} catch (final BatchUpdateException bue) {
				this.dumpBulkOutputExceptionData(bue);
				throw bue;
			}
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("It took more than {} to execute jdbc insert for bulk data. Statement is {}", warningThreshold, insertStatement);
			}
			rowCounter = 0;
			return true;
		} catch (final Exception e) {
			try {
				if (connection != null && !connection.isClosed()) {
					connection.rollback();
				}
			} catch (final Exception exc) {
				log.error("Exception while performing rollback for batch loading", exc);
			}
			log.error("Exception while inserting bulk values using jdbc", e);
			log.error(
					"Exception caught while trying to commit and close all resources. Prepared statement was {}. Current row counter is {} - per feed row counter is {}. All available global attributes are {}",
					currentStatementWithReplacedValues, rowCounter, perFeedRowCounter, globalAttributes);
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
			throw new RuntimeException(e);
		} finally {
			try {
				if (preparedStatement != null && !preparedStatement.isClosed()) {
					preparedStatement.clearBatch();
				}
			} catch (final SQLException e) {
				log.error("Exception while clearing batch", e);
			}
		}
	}

	protected void dumpBulkOutputExceptionData(final BatchUpdateException bue) {
		final int[] updateCounts = bue.getUpdateCounts();
		log.error("Dumping result set for failed batch update");
		for (int i = 0; i < updateCounts.length; i++) {
			final int val = updateCounts[i];
			if (val == PreparedStatement.SUCCESS_NO_INFO) {
				log.error("Batch position {} - result of execution was successful. Number of updated rows was not provided by driver", i);
			} else if (val == PreparedStatement.EXECUTE_FAILED) {
				log.error("Batch position {} - result of execution failed", i);
				if (preparedStatementDebugData != null) {
					final List<Object> debugDataForRow = preparedStatementDebugData.get(i);
					if (debugDataForRow != null) {
						log.error("For batch position {} prepared statement data is {}", i, debugDataForRow.toString());
					}
				}
			} else {
				log.error("Batch position {} - result of execution was successful and in total {} rows were affected in the database", i, val);
			}
		}
	}

}
