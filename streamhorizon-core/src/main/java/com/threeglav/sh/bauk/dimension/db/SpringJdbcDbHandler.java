package com.threeglav.sh.bauk.dimension.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.support.TransactionTemplate;

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
	private final TransactionTemplate txTemplate;

	public SpringJdbcDbHandler(final BaukConfiguration config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final DataSource ds = DataSourceProvider.getBulkJdbcDataSource(config);
		jdbcTemplate = new JdbcTemplate(ds);
		final DataSourceTransactionManager dstm = new DataSourceTransactionManager(ds);
		txTemplate = new TransactionTemplate(dstm);
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
			final int numAffected = jdbcTemplate.update(new PreparedStatementCreator() {

				@Override
				public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
					return connection.prepareStatement(statement, new int[] { 1 });
				}

			}, holder);
			if (numAffected < 1) {
				log.warn("Statement [{}] affected {} rows in total after update", statement, numAffected);
			}
			final Number num = holder.getKey();
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Returned key after insertion is {}. Total affected rows {}", num, numAffected);
			}
			if (num == null) {
				return null;
			}
			final long val = num.longValue();
			if (val <= 0) {
				log.warn(
						"Primary key should not be non-positive integer. Statement {} generated primary key of value {}. Please check how you configured your tables!",
						statement, val);
			}
			return val;
		} catch (final DuplicateKeyException dke) {
			throw dke;
		} catch (final Exception exc) {
			final String message = "Exception while executing insert statement for " + description + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	@Override
	public int executeInsertOrUpdateStatement(final String statement, final String description) {
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
					return connection.prepareStatement(statement, Statement.NO_GENERATED_KEYS);
				}

			});
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Successfully executed {}. Returned value is {}. In total took {}ms to execute", statement, res, total);
			}
			return res;
		} catch (final DuplicateKeyException dke) {
			throw dke;
		} catch (final Exception exc) {
			final String message = "Exception while executing insert/update statement for " + description + ". Statement is " + statement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

	@Override
	public List<DimensionKeysPair> queryForDimensionKeys(final String dimensionName, final String statement, final int expectedTotalValues,
			final RowMapper<DimensionKeysPair> rowMapper) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null");
		}
		if (StringUtil.isEmpty(dimensionName)) {
			throw new IllegalArgumentException("Dimension name must not be null");
		}
		try {
			final long start = System.currentTimeMillis();
			log.debug("About to execute query statement [{}]", statement);
			log.info(
					"Will expect in exactly {} results per row. First one should be surrogate key, others should be natural keys in order as defined in configuration!",
					expectedTotalValues);
			final List<DimensionKeysPair> allRows = jdbcTemplate.query(new BaukPreparedStatementCreator(statement), rowMapper);
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
	public Map<String, String> executeSingleRowSelectStatement(final String statement, final String description) {
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

	@Override
	public TransactionTemplate getTransactionTemplate() {
		return txTemplate;
	}

	@Override
	public List<List<String>> selectAllRowsAsStrings(final String sqlStatement, final DataSource dataSource) {
		if (StringUtil.isEmpty(sqlStatement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		if (dataSource == null) {
			throw new IllegalArgumentException("Data source must not be null");
		}
		try {
			final JdbcTemplate jdTemp = new JdbcTemplate(dataSource);
			if (isDebugEnabled) {
				log.debug("About to execute query statement [{}], Will return all results as string values", sqlStatement);
			}
			final long start = System.currentTimeMillis();
			final List<List<String>> queryResults = jdTemp.query(sqlStatement, new RowMapper<List<String>>() {

				private int numberOfColumns = -1;

				@Override
				public List<String> mapRow(final ResultSet rs, final int rowNum) throws SQLException {
					if (numberOfColumns < 0) {
						final ResultSetMetaData rsmd = rs.getMetaData();
						numberOfColumns = rsmd.getColumnCount();
					}
					final List<String> values = new ArrayList<String>();
					for (int i = 1; i <= numberOfColumns; i++) {
						final String val = rs.getString(i);
						values.add(val);
					}
					return values;
				}

			});
			log.debug("In total found {} rows as result of execution of {}", queryResults.size(), sqlStatement);
			final long total = System.currentTimeMillis() - start;
			if (total > warningThreshold) {
				log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, sqlStatement, warningThreshold);
			}
			if (isDebugEnabled) {
				log.debug("Successfully executed {}. Results {}", sqlStatement, queryResults);
			}
			return queryResults;
		} catch (final Exception exc) {
			final String message = "Exception while executing select statement " + sqlStatement + ".";
			log.error(message, exc);
			throw new RuntimeException(message, exc);
		}
	}

}
