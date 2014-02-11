package com.threeglav.bauk.dimension.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
import com.threeglav.bauk.BaukEngineConfigurationConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.ConnectionProperties;
import com.threeglav.bauk.util.StringUtil;

public class DataSourceProvider {

	// as recommended in bonecp documentation
	private static final int DEFAULT_PARTITION_COUNT = 3;

	private static final int DEFAULT_ACQUIRE_INCREMENT = 10;

	private static final Logger log = LoggerFactory.getLogger(DataSourceProvider.class);
	private final BoneCPDataSource dataSource = new BoneCPDataSource();

	private static final DataSourceProvider INSTANCE = new DataSourceProvider();

	private DataSourceProvider() {

	}

	private DataSource createWhDataSource(final BaukConfiguration config) {
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		final ConnectionProperties connectionProperties = config.getConnectionProperties();
		if (connectionProperties == null) {
			throw new IllegalArgumentException("Unable to connect to database. Connection properties not defined!");
		}
		final String jdbUrl = connectionProperties.getJdbcUrl();
		if (StringUtil.isEmpty(jdbUrl)) {
			throw new IllegalArgumentException("Unable to connect to database. JDBC URL not defined!");
		}
		if (connectionProperties.getJdbcPoolSize() <= 0) {
			throw new IllegalArgumentException("JDBC pool size must be positive integer. Provided value is " + connectionProperties.getJdbcPoolSize());
		}
		try {
			final Properties clientInfoProperties = new Properties();
			final String jdbcProgramName = ConfigurationProperties.getSystemProperty(
					BaukEngineConfigurationConstants.JDBC_CLIENT_INFO_PROGRAM_NAME_PARAM_NAME, null);
			if (!StringUtil.isEmpty(jdbcProgramName)) {
				log.info("Setting client info value to [{}]", jdbcProgramName);
				clientInfoProperties.put("v$session.program", jdbcProgramName);
			}
			dataSource.setDriverProperties(clientInfoProperties);
			log.info("JDBC URL is {}", connectionProperties.getJdbcUrl());
			dataSource.setJdbcUrl(connectionProperties.getJdbcUrl());
			if (!StringUtil.isEmpty(connectionProperties.getJdbcUserName())) {
				log.debug("Will access database as user [{}]", connectionProperties.getJdbcUserName());
				dataSource.setUsername(connectionProperties.getJdbcUserName());
			}
			if (!StringUtil.isEmpty(connectionProperties.getJdbcPassword())) {
				log.debug("JDBC password set");
				dataSource.setPassword(connectionProperties.getJdbcPassword());
			}
			dataSource.setPartitionCount(DEFAULT_PARTITION_COUNT);
			final int connectionsPerPartition = connectionProperties.getJdbcPoolSize() / DEFAULT_PARTITION_COUNT;
			log.info("Will use {} partitions and {} connections per partition", DEFAULT_PARTITION_COUNT, connectionsPerPartition);
			dataSource.setMinConnectionsPerPartition(connectionsPerPartition);
			dataSource.setMaxConnectionsPerPartition(connectionsPerPartition);
			dataSource.setAcquireIncrement(DEFAULT_ACQUIRE_INCREMENT);
			dataSource.setDisableJMX(true);
			dataSource.setStatisticsEnabled(false);
			return dataSource;
		} catch (final Exception e) {
			log.error("Exception starting connection pool", e);
			throw new RuntimeException("Unable to start connection pool", e);
		}
	}

	public static DataSource getDataSource(final BaukConfiguration config) {
		return INSTANCE.createWhDataSource(config);
	}

	public static DataSource getSimpleDataSource(final String jdbcUrl) {
		if (StringUtil.isEmpty(jdbcUrl)) {
			throw new IllegalArgumentException("JDBC url must not be null or empty");
		}
		final BoneCPDataSource dataSource = new BoneCPDataSource();
		dataSource.setAcquireIncrement(DEFAULT_ACQUIRE_INCREMENT);
		dataSource.setDisableJMX(true);
		dataSource.setStatisticsEnabled(false);
		dataSource.setJdbcUrl(jdbcUrl);
		return dataSource;
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
