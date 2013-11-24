package com.threeglav.bauk.feed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public class ConfigAware {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final FactFeed factFeed;
	private final BaukConfiguration config;

	public ConfigAware(final FactFeed factFeed, final BaukConfiguration config) {
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

	protected BaukConfiguration getConfig() {
		return this.config;
	}

}
