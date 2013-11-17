package com.threeglav.bauk.dimension.db;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.ConnectionProperties;
import com.threeglav.bauk.util.StringUtil;

class DataSourceProvider {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final BoneCPDataSource dataSource = new BoneCPDataSource();

	private static final DataSourceProvider INSTANCE = new DataSourceProvider();

	private DataSourceProvider() {

	}

	DataSource createDataSource(final Config config) {
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
		try {
			dataSource.setJdbcUrl(connectionProperties.getJdbcUrl());
			if (!StringUtil.isEmpty(connectionProperties.getJdbcUserName())) {
				log.debug("Will access database as user [{}]", connectionProperties.getJdbcUserName());
				dataSource.setUsername(connectionProperties.getJdbcUserName());
			} else {
				log.warn("JDBC username not specified");
			}
			if (!StringUtil.isEmpty(connectionProperties.getJdbcPassword())) {
				log.debug("JDBC password set");
				dataSource.setPassword(connectionProperties.getJdbcPassword());
			} else {
				log.warn("JDBC password not specified");
			}
			// pds.setDriverClass(config.getString("jdbc.driverClass"));
			// pds.setJdbcUrl(config.getString("jdbc.url"));
			// pds.setMinConnectionsPerPartition(config.getInt("jdbc.poolsize") / config.getInt("jdbc.partitions"));
			// pds.setMaxConnectionsPerPartition(config.getInt("jdbc.poolsize") / config.getInt("jdbc.partitions"));
			// pds.setPartitionCount(config.getInt("jdbc.partitions"));
			dataSource.setAcquireIncrement(5);
			return dataSource;
		} catch (final Exception e) {
			log.error("Exception starting connection pool", e);
			throw new RuntimeException("Unable to start connection pool", e);
		}
	}

	public static DataSource getDataSource(final Config config) {
		return INSTANCE.createDataSource(config);
	}
}
