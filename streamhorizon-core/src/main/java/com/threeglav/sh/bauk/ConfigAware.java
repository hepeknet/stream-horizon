package com.threeglav.sh.bauk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.dimension.db.DbHandler;
import com.threeglav.sh.bauk.dimension.db.SpringJdbcDbHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Feed;

public abstract class ConfigAware {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Feed factFeed;
	private final BaukConfiguration config;
	private DbHandler dbHandler;
	protected final boolean isDebugEnabled;
	protected final boolean isTraceEnabled;
	protected final boolean isInfoEnabled;

	public ConfigAware(final Feed factFeed, final BaukConfiguration config) {
		if (factFeed == null) {
			throw new IllegalArgumentException("Fact feed must not be null");
		}
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		this.factFeed = factFeed;
		this.config = config;
		// help JIT remove dead code
		isDebugEnabled = log.isDebugEnabled();
		isTraceEnabled = log.isTraceEnabled();
		isInfoEnabled = log.isInfoEnabled();
	}

	protected Feed getFactFeed() {
		return factFeed;
	}

	protected BaukConfiguration getConfig() {
		return config;
	}

	public synchronized DbHandler getDbHandler() {
		if (dbHandler == null) {
			dbHandler = new SpringJdbcDbHandler(config);
		}
		return dbHandler;
	}

}
