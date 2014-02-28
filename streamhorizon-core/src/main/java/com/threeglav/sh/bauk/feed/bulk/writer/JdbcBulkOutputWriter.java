package com.threeglav.sh.bauk.feed.bulk.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
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
import com.threeglav.sh.bauk.model.FactFeed;
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
	private final boolean outputProcessingStatistics;
	private String currentStatementWithReplacedValues;
	private final DataSource dataSource;
	private final StatefulAttributeReplacer statefulReplacer;

	public JdbcBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		final ArrayList<BaukCommand> bulkInsertCommands = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsert();
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
		final ArrayList<BaukAttribute> attributes = this.getFactFeed().getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes();
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
		dataSource = DataSourceProvider.getDataSource(this.getConfig());
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
	public void initialize(final Map<String, String> globalAttributes) {
		rowCounter = 0;
		this.initializePreparedStatement(globalAttributes);
		globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_STARTED_PROCESSING_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
	}

	@Override
	public void doOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			rowCounter++;
			if (isDebugEnabled) {
				log.debug("Populating jdbc statement - row {}", rowCounter);
			}
			for (int i = 0; i < resolvedData.length; i++) {
				preparedStatement.setObject(i + 1, resolvedData[i], sqlTypes[i]);
				// help GC
				resolvedData[i] = null;
			}
			preparedStatement.addBatch();
			if (rowCounter == batchSize) {
				if (isDebugEnabled) {
					log.debug("Executing jdbc batch of size {}", batchSize);
				}
				this.doExecuteJdbcBatch(globalAttributes);
			}
			if (isDebugEnabled) {
				log.debug("Successfully populated jdbc statement. Current row number {}", rowCounter);
			}
		} catch (final Exception e) {
			log.error("Exception populating jdbc statement", e);
			log.error("Prepared statement was {}", currentStatementWithReplacedValues);
			throw new RuntimeException("Problem while populating batch in JDBC statement", e);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes, final boolean success) {
		if (preparedStatement == null) {
			throw new IllegalStateException("Prepared statement is null! Should not happen!");
		}
		if (isDebugEnabled) {
			log.debug("Closing feed, inserting remaining batched data. Attributes {}", globalAttributes);
		}
		try {
			this.doExecuteJdbcBatch(globalAttributes);
			EngineRegistry.registerSuccessfulBulkFileLoad();
			globalAttributes
					.put(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_JDBC_FINISHED_PROCESSING_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
			if (outputProcessingStatistics) {
				final String message = this.getCurrentThreadName() + " - Finished bulk loading data using JDBC. In total bulk loaded "
						+ EngineRegistry.getSuccessfulBulkFilesCount() + " files so far!";
				BaukUtil.logBulkLoadEngineMessage(message);
			}
		} finally {
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
		}
	}

	private void doExecuteJdbcBatch(final Map<String, String> globalAttributes) {
		try {
			final long start = System.currentTimeMillis();
			final int[] values = preparedStatement.executeBatch();
			connection.commit();
			if (isDebugEnabled) {
				int count = 0;
				for (int i = 0; i < values.length; i++) {
					count += values[i];
				}
				log.debug("Result of batch insert of bulk values was {} for total of {} rows", count, values.length);
			}
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("It took more than {} to execute jdbc insert for bulk data. Statement is {}", warningThreshold, insertStatement);
			}
			rowCounter = 0;
		} catch (final Exception e) {
			try {
				connection.rollback();
			} catch (final Exception exc) {
				log.error("Exception while performing rollback for batch loading", exc);
			}
			log.error("Exception while inserting bulk values using jdbc", e);
			log.error(
					"Exception caught while trying to commit and close all resources. Prepared statement was {}. Current row counter is {}. All available global attributes are {}",
					currentStatementWithReplacedValues, rowCounter, globalAttributes);
			DataSourceProvider.close(preparedStatement);
			DataSourceProvider.closeOnly(connection);
			throw new RuntimeException(e);
		} finally {
			try {
				preparedStatement.clearBatch();
			} catch (final SQLException e) {
				log.error("Exception while clearing batch", e);
			}
		}
	}

}
