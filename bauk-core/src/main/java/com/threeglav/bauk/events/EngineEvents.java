package com.threeglav.bauk.events;

import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.StringUtil;

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

	static class ProcessingStatusEventsObservable extends Observable {
		public void notifyToContinueProcessing() {
			this.setChanged();
			this.notifyObservers(PROCESSING_EVENT.CONTINUE);
		}

		public void notifyToPauseProcessing() {
			this.setChanged();
			this.notifyObservers(PROCESSING_EVENT.PAUSE);
		}
	}

	private static final ProcessingStatusEventsObservable PROCESSING_STATUS_EVENTS = new ProcessingStatusEventsObservable();

	private static final FlushDimensionObservable FLUSH_DIMENSION_CACHE = new FlushDimensionObservable();

	public static void notifyPauseProcessing() {
		LOG.debug("Notifying interested observers about processing pause event");
		PROCESSING_STATUS_EVENTS.notifyToPauseProcessing();
	}

	public static void notifyContinueProcessing() {
		LOG.debug("Notifying interested observers about continue processing event");
		PROCESSING_STATUS_EVENTS.notifyToContinueProcessing();
	}

	public static void registerForProcessingEvents(final Observer observer) {
		PROCESSING_STATUS_EVENTS.addObserver(observer);
		LOG.debug("Added observer for engine events");
	}

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
