package com.threeglav.bauk.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;

public abstract class InMemoryDbUtil {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryDbUtil.class);

	public static String getJdbcUrl(final String databaseName) {
		final String dbDataFolder = ConfigurationProperties.getDbDataFolder();
		// cache size in KB
		final String url = "jdbc:h2:" + dbDataFolder + "/" + databaseName + ";DB_CLOSE_DELAY=20;CACHE_SIZE=131072";
		return url;
	}

	public static void executeUpdateStatement(final String sql, final DataSource ds) {
		if (StringUtil.isEmpty(sql)) {
			throw new IllegalArgumentException("sql must not be null or empty string");
		}
		if (ds == null) {
			throw new IllegalArgumentException("DataSource must not be null");
		}
		LOG.debug("Executing {}", sql);
		try (Connection conn = ds.getConnection(); Statement stat = conn.createStatement()) {
			stat.executeUpdate(sql);
			LOG.debug("Successfully executed {}", sql);
			conn.commit();
		} catch (final SQLException e) {
			LOG.info("Exception while executing {}. Details {}", sql, e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public static Object executePreparedStatement(final String sql, final DataSource ds, final String parameter) {
		if (StringUtil.isEmpty(sql)) {
			throw new IllegalArgumentException("sql must not be null or empty string");
		}
		if (ds == null) {
			throw new IllegalArgumentException("DataSource must not be null");
		}
		LOG.debug("Executing {}, parameter is [{}]", sql, parameter);
		try (Connection conn = ds.getConnection(); PreparedStatement stat = conn.prepareStatement(sql)) {
			if (parameter != null) {
				stat.setString(1, parameter);
			}
			final boolean returnedValues = stat.execute();
			if (returnedValues) {
				try (ResultSet rs = stat.getResultSet()) {
					if (rs.next()) {
						final Object res = rs.getObject(1);
						LOG.debug("Successfully executed {}. Returned {}", sql, res);
						return res;
					}
				}
			}
			LOG.debug("Successfully executed {}. Nothing to return", sql);
			return null;
		} catch (final SQLException e) {
			LOG.error("Exception while executing {}. Details {}", sql, e.getMessage());
			throw new RuntimeException(e);
		}
	}

}
