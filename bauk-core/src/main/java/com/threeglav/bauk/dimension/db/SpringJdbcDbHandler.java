package com.threeglav.bauk.dimension.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.util.StringUtil;

public class SpringJdbcDbHandler implements DbHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final JdbcTemplate jdbcTemplate;
	private final int warningThreshold;

	public SpringJdbcDbHandler(final Config config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final DataSource ds = DataSourceProvider.getDataSource(config);
		jdbcTemplate = new JdbcTemplate(ds);
		warningThreshold = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
				SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);
		log.debug("Will report any sql execution taking longer than {}ms", warningThreshold);
	}

	@Override
	public Long executeQueryStatementAndReturnKey(final String statement) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		log.debug("About to execute query statement [{}], Will expect that it returns surrogate key as first field of type long", statement);
		final long start = System.currentTimeMillis();
		final List<Long> query = jdbcTemplate.query(statement, new RowMapper<Long>() {
			@Override
			public Long mapRow(final ResultSet resultSet, final int i) throws SQLException {
				return resultSet.getLong(1);
			}
		});
		final long total = System.currentTimeMillis() - start;
		if (total > warningThreshold) {
			log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
		}
		if (query.size() == 1) {
			final Long res = query.get(0);
			log.debug("Returned result is {}", res);
			return res;
		}
		if (query.isEmpty()) {
			return null;
		}
		return null;
	}

	@Override
	public Long executeInsertStatementAndReturnKey(final String statement) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		log.debug("About to execute insert statement [{}], Will expect that it returns surrogate key as first field of type long", statement);
		final KeyHolder holder = new GeneratedKeyHolder();
		final long start = System.currentTimeMillis();
		jdbcTemplate.update(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
				return connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
			}

		}, holder);
		final Number num = holder.getKey();
		final long total = System.currentTimeMillis() - start;
		if (total > warningThreshold) {
			log.warn("Took {}ms to execute {}. More than configured threshold {}ms", total, statement, warningThreshold);
		}
		log.debug("Returned key after insertion is {}", num);
		if (num == null) {
			return null;
		}
		return num.longValue();
	}

	@Override
	public void executeInsertStatement(final String statement) {
		if (StringUtil.isEmpty(statement)) {
			throw new IllegalArgumentException("Statement must not be null or empty!");
		}
		final long start = System.currentTimeMillis();
		log.debug("About to execute insert statement [{}]");
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
		log.debug("Successfully executed {}. Returned value is {}. In total took {}ms to execute", statement, res, total);
	}

}
