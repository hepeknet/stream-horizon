package com.threeglav.bauk.dimension.db;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.util.StringUtil;

public class DataSourceProvider {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final BoneCPDataSource pds = new BoneCPDataSource();

	private static final DataSourceProvider INSTANCE = new DataSourceProvider();

	private DataSourceProvider() {

	}

	private DataSource createDataSource(final Config config) {
		if (config.getConnectionProperties() == null) {
			throw new IllegalArgumentException("Unable to connect to database. Connection properties not defined!");
		}
		final String jdbUrl = config.getConnectionProperties().getJdbcUrl();
		if (StringUtil.isEmpty(jdbUrl)) {
			throw new IllegalArgumentException("Unable to connect to database. JDBC URL not defined!");
		}
		try {
			pds.setJdbcUrl(config.getConnectionProperties().getJdbcUrl());
			// pds.setDriverClass(config.getString("jdbc.driverClass"));
			// pds.setJdbcUrl(config.getString("jdbc.url"));
			// pds.setUsername(config.getString("jdbc.username"));
			// pds.setPassword(config.getString("jdbc.password"));
			// pds.setMinConnectionsPerPartition(config.getInt("jdbc.poolsize") / config.getInt("jdbc.partitions"));
			// pds.setMaxConnectionsPerPartition(config.getInt("jdbc.poolsize") / config.getInt("jdbc.partitions"));
			// pds.setPartitionCount(config.getInt("jdbc.partitions"));
			pds.setAcquireIncrement(5);
			return pds;
		} catch (final Exception e) {
			log.error("Exception starting connection pool", e);
			throw new RuntimeException("Unable to start connection pool", e);
		}
	}

	public static DataSource getDataSource(final Config config) {
		return INSTANCE.createDataSource(config);
	}
}
