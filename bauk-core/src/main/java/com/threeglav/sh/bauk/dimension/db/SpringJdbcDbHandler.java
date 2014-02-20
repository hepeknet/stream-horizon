package com.threeglav.sh.bauk.dimension.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dimension.DimensionKeysPair;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.util.StringUtil;

public final class SpringJdbcDbHandler implements DbHandler {

	private static final int DEFAULT_FETCH_SIZE = 10000;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final JdbcTemplate jdbcTemplate;
	private final int warningThreshold;
	private final boolean isDebugEnabled;

	public SpringJdbcDbHandler(final BaukConfiguration config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final DataSource ds = DataSourceProvider.getDataSource(config);
		jdbcTemplate = new JdbcTemplate(ds);
		final int fetchSize = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.PRE_CACHE_FETCH_SIZE_PARAM_NAME,
				DEFAULT_FETCH_SIZE);
		if (fetchSize <= 0) {
			throw new IllegalArgumentException(BaukEngineConfigurationConstants.PRE_CACHE_FETCH_SIZE_PARAM_NAME + " must be positive integer value!");
		}
		jdbcTemplate.setFetchSize(fetchSize);
		jdbcTemplate.setLazyInit(false);
		warningThreshold = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
				BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);
		log.debug("Will report any sql execution taking longer than {}ms", warningThreshold);
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public Long executeQueryStatementAndReturnKey(final String statement, final String dimensionName) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		try {
			if (isDebugEnabled) {
				log.debug("About to execute query statement [{}]. Will expect that it returns surrogate key as first field of type long", statement);
			}
			final long start = System.currentTimeMillis();
			final List<Long> queryResults = jdbcTemplate.query(statement, new RowMapper<Long>() {
				@Override
				public Long mapRow(final ResultSet resultSet, final int i) throws SQLException {
					return resultSet.getLong(1);
				}
			});
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Successfully executed {}. Results {}", statement, queryResults);
			}
			if (queryResults.size() == 1) {
				final Long res = queryResults.get(0);
				if (isDebugEnabled) {
					log.debug("Returned result is {}", res);
				}
				return res;
			} else if (queryResults.isEmpty()) {
				if (isDebugEnabled) {
					log.debug("Could not find any results after executing {}", statement);
				}
				return null;
			} else {
				log.warn("Found results {} after executing {}. Unable to process!", queryResults, statement);
				return null;
			}
		} catch (final Exception exc) {
			log.error("Exception while executing select statement for dimension {}. Statement is {}. Details {}", dimensionName, statement,
					exc.getMessage());
			log.error("Exception", exc);
			throw exc;
		}
	}

	@Override
	public Long executeInsertStatementAndReturnKey(final String statement, final String description) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		try {
			if (isDebugEnabled) {
				log.debug("About to execute insert statement [{}], Will expect that it returns surrogate key as first field of type long", statement);
			}
			final KeyHolder holder = new GeneratedKeyHolder();
			final long start = System.currentTimeMillis();
			jdbcTemplate.update(new PreparedStatementCreator() {

				@Override
				public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
					return connection.prepareStatement(statement, new int[] { 1 });
				}

			}, holder);
			final Number num = holder.getKey();
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Returned key after insertion is {}", num);
			}
			if (num == null) {
				return null;
			}
			return num.longValue();
		} catch (final DuplicateKeyException dke) {
			throw dke;
		} catch (final Exception exc) {
			final String message = "Exception while executing insert statement for " + description + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	@Override
	public void executeInsertOrUpdateStatement(final String statement, final String description) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		try {
			final long start = System.currentTimeMillis();
			if (isDebugEnabled) {
				log.debug("About to execute insert/update statement [{}]", statement);
			}
			final int res = jdbcTemplate.update(new PreparedStatementCreator() {

				@Override
				public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
					return connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
				}

			});
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Successfully executed {}. Returned value is {}. In total took {}ms to execute", statement, res, total);
			}
		} catch (final Exception exc) {
			final String message = "Exception while executing insert/update statement for " + description + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	@Override
	public List<DimensionKeysPair> queryForDimensionKeys(final String dimensionName, final String statement, final int numberOfNaturalKeyColumns) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null");
		}
		if (StringUtil.isEmpty(dimensionName)) {
			throw new IllegalArgumentException("Dimension name must not be null");
		}
		try {
			final int expectedTotalValues = numberOfNaturalKeyColumns + 1;
			final long start = System.currentTimeMillis();
			log.debug("About to execute query statement [{}]", statement);
			log.info(
					"Will expect in exactly {} results per row. First one should be surrogate key, others should be natural keys in order as defined in configuration!",
					expectedTotalValues);
			final List<DimensionKeysPair> allRows = jdbcTemplate.query(new BaukPreparedStatementCreator(statement), new DimensionKeysRowMapper(
					dimensionName, statement, expectedTotalValues));
			final int rowsReturned = allRows.size();
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			log.debug("Successfully executed {}. Returned {} rows in total. In total took {}ms to execute", statement, rowsReturned, total);
			return allRows;
		} catch (final Exception exc) {
			final String message = "Exception while querying for natural keys for dimension " + dimensionName + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	@Override
	public Map<String, String> executeSelectStatement(final String statement, final String description) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		try {
			if (isDebugEnabled) {
				log.debug("About to execute query statement [{}], Will return all results as string values", statement);
			}
			final long start = System.currentTimeMillis();
			final Map<String, Object> queryResults = jdbcTemplate.queryForMap(statement);
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Successfully executed {}. Results {}", statement, queryResults);
			}
			if (queryResults == null) {
				return null;
			}
			final Map<String, String> finalResults = new HashMap<String, String>();
			for (final Entry<String, Object> entry : queryResults.entrySet()) {
				finalResults.put(entry.getKey(), String.valueOf(entry.getValue()));
			}
			return finalResults;
		} catch (final Exception exc) {
			final String message = "Exception while executing select statement for " + description + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	private static final class BaukPreparedStatementCreator implements PreparedStatementCreator {

		private final String statement;

		public BaukPreparedStatementCreator(final String stat) {
			statement = stat;
		}

		@Override
		public PreparedStatement createPreparedStatement(final Connection con) throws SQLException {
			final PreparedStatement ps = con.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			ps.setFetchSize(DEFAULT_FETCH_SIZE);
			return ps;
		}

	}

	private static final class DimensionKeysRowMapper implements RowMapper<DimensionKeysPair> {

		private final String dimensionName;
		private final String statement;
		private final int expectedTotalValues;
		private boolean alreadyCheckedForColumnNumber = false;

		private final Logger log = LoggerFactory.getLogger(this.getClass());

		private DimensionKeysRowMapper(final String dimensionName, final String statement, final int expectedTotalValues) {
			this.dimensionName = dimensionName;
			this.statement = statement;
			this.expectedTotalValues = expectedTotalValues;
		}

		@Override
		public DimensionKeysPair mapRow(final ResultSet rs, final int rowNum) throws SQLException {
			if (!alreadyCheckedForColumnNumber) {
				final ResultSetMetaData rsmd = rs.getMetaData();
				final int columnsNumber = rsmd.getColumnCount();
				if (columnsNumber != expectedTotalValues) {
					log.error("For dimension {} sql statement {} does not return correct number of values", dimensionName, statement);
					throw new IllegalStateException("SQL statement for dimension " + dimensionName
							+ " should return surrogate key and all natural keys (in order as declared in configuration). In total expected "
							+ expectedTotalValues + " columns, but database query returned only " + columnsNumber + " values!");
				}
				alreadyCheckedForColumnNumber = true;
			}
			final DimensionKeysPair dkp = new DimensionKeysPair();
			dkp.surrogateKey = rs.getInt(1);
			final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
			for (int i = 2; i <= expectedTotalValues; i++) {
				if (i != 2) {
					sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
				}
				sb.append(rs.getString(i));
			}
			dkp.naturalKey = sb.toString();
			return dkp;
		}
	}

}
