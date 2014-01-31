package com.threeglav.bauk.feed.bulk.writer;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.springframework.util.StringUtils;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.db.DataSourceProvider;
import com.threeglav.bauk.model.BaukAttribute;
import com.threeglav.bauk.model.BaukAttributeType;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class JdbcBulkOutputWriter extends AbstractBulkOutputWriter {

	private final String insertStatement;
	private final int[] sqlTypes;
	private PreparedStatement preparedStatement;
	private final int warningThreshold;
	private final int batchSize;
	private int rowCounter = 0;
	private final boolean hasAnyGlobalAttributesToReplace;

	public JdbcBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		insertStatement = this.getFactFeed().getBulkLoadDefinition().getBulkLoadInsertStatement();
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
		warningThreshold = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
				SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);
		batchSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.JDBC_BULK_LOADING_BATCH_SIZE_PARAM_NAME,
				SystemConfigurationConstants.JDBC_BULK_LOADING_BATCH_SIZE_DEFAULT);
		final Set<String> allGlobalAttributesUsedInStatement = StringUtil.collectAllAttributesFromString(insertStatement);
		hasAnyGlobalAttributesToReplace = allGlobalAttributesUsedInStatement != null && !allGlobalAttributesUsedInStatement.isEmpty();
		log.info("Will use {} batch size for loading bulk data using JDBC", batchSize);
	}

	private void validate(final int attributesNumber) {
		final int occurenceOfPreparedStatementPlaceholders = StringUtils.countOccurrencesOf(insertStatement, "?");
		if (attributesNumber != occurenceOfPreparedStatementPlaceholders) {
			log.warn("Found {} occurrences of ? in statement {} but found {} declared attributes! This is probably error in configuration!",
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
		} else {
			return Types.FLOAT;
		}
	}

	private void initializePreparedStatement(final Map<String, String> globalAttributes) {
		try {
			String statement = insertStatement;
			if (hasAnyGlobalAttributesToReplace) {
				statement = StringUtil.replaceAllAttributes(statement, globalAttributes, this.getConfig().getDatabaseStringLiteral(), this
						.getConfig().getDatabaseStringEscapeLiteral());
				if (isDebugEnabled) {
					log.debug("After replacing all attributes insert statement looks like {}", statement);
				}
			}
			preparedStatement = DataSourceProvider.getDataSource(this.getConfig()).getConnection().prepareStatement(statement);
			log.info("Successfully initialized prepared statement {}", statement);
		} catch (final Exception e) {
			throw new RuntimeException("Exception while initializing prepared statement for loading bulk values using jdbc", e);
		}
	}

	@Override
	public void initialize(final Map<String, String> globalAttributes) {
		this.initializePreparedStatement(globalAttributes);
	}

	@Override
	public void doOutput(final Object[] resolvedData) {
		try {
			rowCounter++;
			if (isDebugEnabled) {
				log.debug("Populating jdbc statement - row {}", rowCounter);
			}
			for (int i = 0; i < resolvedData.length; i++) {
				preparedStatement.setObject(i + 1, resolvedData[i], sqlTypes[i]);
			}
			preparedStatement.addBatch();
			if (rowCounter == batchSize) {
				if (isDebugEnabled) {
					log.debug("Executing jdbc batch of size {}", batchSize);
				}
				this.doExecuteJdbcBatch();
			}
			if (isDebugEnabled) {
				log.debug("Successfully populated jdbc statement. Current row number {}", rowCounter);
			}
		} catch (final Exception e) {
			log.error("Exception populating jdbc statement", e);
			throw new RuntimeException("Problem while populating batch in JDBC statement", e);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
		if (preparedStatement == null) {
			throw new IllegalStateException("Prepared statement is null! Should not happen!");
		}
		if (isDebugEnabled) {
			log.debug("Closing feed, inserting remaining batched data. Attributes {}", globalAttributes);
		}
		this.doExecuteJdbcBatch();
	}

	private void doExecuteJdbcBatch() {
		try {
			final long start = System.currentTimeMillis();
			rowCounter = 0;
			final int[] values = preparedStatement.executeBatch();
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
		} catch (final Exception e) {
			log.error("Exception while insert bulk values using jdbc", e);
			throw new RuntimeException(e);
		}
	}

}
