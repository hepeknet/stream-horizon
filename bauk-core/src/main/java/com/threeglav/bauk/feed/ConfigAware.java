package com.threeglav.bauk.feed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;

public class ConfigAware {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final FactFeed factFeed;
	private final Config config;

	public ConfigAware(final FactFeed factFeed, final Config config) {
		if (factFeed == null) {
			throw new IllegalArgumentException("Fact feed must not be null");
		}
		this.factFeed = factFeed;
		if (config == null) {
			throw new IllegalArgumentException("Config must not be null");
		}
		this.config = config;
	}

	protected FactFeed getFactFeed() {
		return this.factFeed;
	}

	protected Config getConfig() {
		return this.config;
	}

}
