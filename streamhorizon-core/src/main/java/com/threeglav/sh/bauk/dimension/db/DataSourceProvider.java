package com.threeglav.sh.bauk.dimension.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.ConnectionProperties;
import com.threeglav.sh.bauk.util.StringUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceProvider {

	private static final Logger log = LoggerFactory.getLogger(DataSourceProvider.class);

	private static final DataSourceProvider INSTANCE = new DataSourceProvider();

	private static final Map<String, HikariDataSource> JDBC_FEED_SOURCES = new HashMap<String, HikariDataSource>();

	private HikariDataSource warehouseDataSource;

	private HikariDataSource bulkSubmissionDataSource;

	private DataSourceProvider() {

	}

	private HikariDataSource createWhDataSource(final BaukConfiguration config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final ConnectionProperties connectionProperties = config.getConnectionProperties();
		if (connectionProperties == null) {
			throw new IllegalArgumentException("Unable to connect to database. Connection properties not defined!");
		}
		final String jdbcUrl = connectionProperties.getJdbcUrl();
		if (StringUtil.isEmpty(jdbcUrl)) {
			throw new IllegalArgumentException("Unable to connect to database. JDBC URL not defined!");
		}
		if (connectionProperties.getJdbcPoolSize() <= 0) {
			throw new IllegalArgumentException("JDBC pool size must be positive integer. Provided value is " + connectionProperties.getJdbcPoolSize());
		}
		try {
			final HikariConfig hc = new HikariConfig();
			final String jdbcProgramName = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.JDBC_CLIENT_INFO_PROGRAM_NAME_PARAM_NAME, null);
			if (!StringUtil.isEmpty(jdbcProgramName)) {
				log.info("Setting client info value to [{}]", jdbcProgramName);
				hc.setConnectionCustomizerClassName(BaukConnectionCustomizer.class.getName());
			}
			hc.setMaximumPoolSize(connectionProperties.getJdbcPoolSize());
			hc.setInitializationFailFast(true);
			hc.setRegisterMbeans(false);
			setDataSourceProperties(jdbcUrl, hc);
			log.info("JDBC URL is {}", jdbcUrl);
			if (!StringUtil.isEmpty(connectionProperties.getJdbcUserName())) {
				log.debug("Will access database as user [{}]", connectionProperties.getJdbcUserName());
				hc.addDataSourceProperty("user", connectionProperties.getJdbcUserName());
			}
			if (!StringUtil.isEmpty(connectionProperties.getJdbcPassword())) {
				log.debug("JDBC password set");
				hc.addDataSourceProperty("password", connectionProperties.getJdbcPassword());
			}
			final HikariDataSource ds = new HikariDataSource(hc);
			return ds;
		} catch (final Exception e) {
			log.error("Exception starting connection pool", e);
			throw new RuntimeException("Unable to start connection pool", e);
		}
	}

	public static synchronized void shutdown() {
		if (INSTANCE.warehouseDataSource != null) {
			try {
				INSTANCE.warehouseDataSource.shutdown();
			} catch (final Exception ignored) {
				// ignore
			}
		}
		if (INSTANCE.bulkSubmissionDataSource != null) {
			try {
				INSTANCE.bulkSubmissionDataSource.shutdown();
			} catch (final Exception ignored) {
				// ignore
			}
		}
		if (!JDBC_FEED_SOURCES.isEmpty()) {
			for (final HikariDataSource hds : JDBC_FEED_SOURCES.values()) {
				try {
					hds.shutdown();
				} catch (final Exception ignored) {
					// ignore
				}
			}
		}
	}

	private static void setDataSourceProperties(final String jdbcUrl, final HikariConfig hc) {
		if (jdbcUrl == null) {
			throw new IllegalArgumentException("jdbc url must not be null");
		}
		final String urlLower = jdbcUrl.toLowerCase();
		if (urlLower.startsWith("jdbc:oracle")) {
			hc.setDataSourceClassName("oracle.jdbc.pool.OracleDataSource");
			hc.addDataSourceProperty("URL", jdbcUrl);
		} else if (urlLower.startsWith("jdbc:sqlserver")) {
			hc.setDataSourceClassName("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
			hc.addDataSourceProperty("URL", jdbcUrl);
		} else if (urlLower.startsWith("jdbc:mysql")) {
			hc.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
			hc.addDataSourceProperty("url", jdbcUrl);
		} else if (urlLower.startsWith("jdbc:h2")) {
			hc.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
			hc.addDataSourceProperty("URL", jdbcUrl);
		} else if (urlLower.startsWith("jdbc:jtds")) {
			hc.setDataSourceClassName("net.sourceforge.jtds.jdbcx.JtdsDataSource");
			final Properties props = StringUtil.parserUrlProperties(jdbcUrl);
			hc.setDataSourceProperties(props);
			hc.setJdbc4ConnectionTest(false);
			hc.setConnectionTestQuery("SELECT 1");
		}
	}

	public static synchronized DataSource getBulkJdbcDataSource(final BaukConfiguration config) {
		if (INSTANCE.warehouseDataSource == null) {
			INSTANCE.warehouseDataSource = INSTANCE.createWhDataSource(config);
		}
		return INSTANCE.warehouseDataSource;
	}

	public static synchronized DataSource getBulkSubmissionDataSource(final String jdbcUrl) {
		if (INSTANCE.bulkSubmissionDataSource == null) {
			if (StringUtil.isEmpty(jdbcUrl)) {
				throw new IllegalArgumentException("JDBC url must not be null or empty");
			}
			final HikariConfig config = new HikariConfig();
			// TODO: check if this needs to be configurable
			config.setMaximumPoolSize(100);
			config.setRegisterMbeans(false);
			setDataSourceProperties(jdbcUrl, config);
			final HikariDataSource ds = new HikariDataSource(config);
			INSTANCE.bulkSubmissionDataSource = ds;
		}
		return INSTANCE.bulkSubmissionDataSource;
	}

	public static synchronized DataSource getJdbcFeedSourceDataSource(final String jdbcUrl) {
		HikariDataSource hds = JDBC_FEED_SOURCES.get(jdbcUrl);
		if (hds == null) {
			if (StringUtil.isEmpty(jdbcUrl)) {
				throw new IllegalArgumentException("JDBC url must not be null or empty");
			}
			final HikariConfig config = new HikariConfig();
			// TODO: check if this needs to be configurable
			config.setMaximumPoolSize(10);
			config.setRegisterMbeans(false);
			setDataSourceProperties(jdbcUrl, config);
			hds = new HikariDataSource(config);
			JDBC_FEED_SOURCES.put(jdbcUrl, hds);
		}
		return hds;
	}

	public static void close(final Connection connection) {
		if (connection != null) {
			try {
				connection.commit();
			} catch (final SQLException se) {
				log.error("Exception while commiting connection", se);
			}
			try {
				connection.close();
			} catch (final SQLException se) {
				log.error("Exception while closing connection", se);
			}
		}
	}

	public static void closeOnly(final Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (final SQLException se) {
				log.error("Exception while closing connection", se);
			}
		}
	}

	public static void close(final PreparedStatement ps) {
		if (ps != null) {
			try {
				ps.close();
			} catch (final SQLException se) {
				log.error("Exception while closing prepared statement", se);
			}
		}
	}

}
