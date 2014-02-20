package com.threeglav.sh.bauk.events;

import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.util.StringUtil;

public class EngineEvents {

	private static final Logger LOG = LoggerFactory.getLogger(EngineEvents.class);

	public static enum PROCESSING_EVENT {
		PAUSE, CONTINUE
	}

	static class FlushDimensionObservable extends Observable {
		public void flushDimension(final String dimensionName) {
			this.setChanged();
			this.notifyObservers(dimensionName);
		}
	}

	private static final FlushDimensionObservable FLUSH_DIMENSION_CACHE = new FlushDimensionObservable();

	public static void registerForFlushDimensionCache(final Observer observer) {
		FLUSH_DIMENSION_CACHE.addObserver(observer);
		LOG.debug("Added observer for flush dimension cache events");
	}

	public static void notifyFlushDimensionCache(final String dimensionName) {
		if (StringUtil.isEmpty(dimensionName)) {
			throw new IllegalArgumentException("Dimension name must not be null or empty!");
		}
		final int observerCount = FLUSH_DIMENSION_CACHE.countObservers();
		LOG.debug("Flushing dimension {}. Currently there are {} observers watching for this event", dimensionName, observerCount);
		FLUSH_DIMENSION_CACHE.flushDimension(dimensionName);
	}

}
