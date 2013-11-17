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

import com.threeglav.bauk.model.Config;

public class SpringJdbcDbHandler implements DbHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	protected JdbcTemplate jdbcTemplate;

	public SpringJdbcDbHandler(final Config config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final DataSource ds = DataSourceProvider.getDataSource(config);
		this.jdbcTemplate = new JdbcTemplate(ds);
	}

	@Override
	public Long executeQueryStatementAndReturnKey(final String statement) {
		this.log.debug("About to execute query statement [{}], Will expect that it returns surrogate key as first field of type long", statement);
		final List<Long> query = this.jdbcTemplate.query(statement, new RowMapper<Long>() {
			@Override
			public Long mapRow(final ResultSet resultSet, final int i) throws SQLException {
				return resultSet.getLong(1);
			}
		});
		if (query.size() == 1) {
			final Long res = query.get(0);
			this.log.debug("Returned result is {}", res);
			return res;
		}
		if (query.isEmpty()) {
			return null;
		}
		return null;
	}

	@Override
	public Long executeInsertStatementAndReturnKey(final String statement) {
		this.log.debug("About to execute insert statement [{}], Will expect that it returns surrogate key as first field of type long", statement);
		final KeyHolder holder = new GeneratedKeyHolder();
		this.jdbcTemplate.update(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
				final PreparedStatement ps = connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
				return ps;
			}
		}, holder);
		final Number num = holder.getKey();
		this.log.debug("Returned key after insertion is {}", num);
		if (num == null) {
			return null;
		}
		return num.longValue();
	}

	@Override
	public void executeInsertStatement(final String statement) {
		this.log.debug("About to execute insert statement [{}]");
		final int res = this.jdbcTemplate.update(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(final Connection connection) throws SQLException {
				final PreparedStatement ps = connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
				return ps;
			}
		});
		this.log.debug("Successfully executed {}. Returned value is {}", statement, res);
	}

}
