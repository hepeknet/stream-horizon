package com.threeglav.bauk.dimension;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.events.EngineEvents;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

/**
 * For every thread there is one instance of this class
 * 
 * @author Borisa
 * 
 */
public final class CachePreviousUsedRowPerThreadDimensionHandler implements BulkLoadOutputValueHandler, Observer {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String NOT_SET_PREVIOUS_KEY_VALUE = "___";

	private final DimensionHandler delegate;
	private String previouslyUsedKey = NOT_SET_PREVIOUS_KEY_VALUE;
	private Integer previouslyUsedValue;
	private final Counter turboCacheHits;
	private final Counter turboCacheMisses;
	private final String dimensionName;
	private final StringBuilder reusedForPerformance = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);

	public CachePreviousUsedRowPerThreadDimensionHandler(final BulkLoadOutputValueHandler delegate) {
		this.delegate = (DimensionHandler) delegate;
		dimensionName = this.delegate.getDimension().getName();
		turboCacheHits = MetricsUtil.createCounter("Dimension [" + dimensionName + "] - turbo cache hits", true);
		turboCacheMisses = MetricsUtil.createCounter("Dimension [" + dimensionName + "] - turbo cache misses", true);
		log.info("Will cache previously used values for dimension {}", dimensionName);
		EngineEvents.registerForFlushDimensionCache(this);
	}

	@Override
	public void calculatePerFeedValues(final Map<String, String> globalValues) {
		delegate.calculatePerFeedValues(globalValues);
	}

	@Override
	public Object getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		reusedForPerformance.setLength(0);
		final String lookupKey = delegate.buildNaturalKeyForCacheLookup(parsedLine, globalValues, reusedForPerformance);
		if (previouslyUsedKey.equals(lookupKey)) {
			if (turboCacheHits != null) {
				turboCacheHits.inc();
			}
			return previouslyUsedValue;
		} else {
			final Integer surrogateKey = (Integer) delegate.getBulkLoadValueByPrecalculatedLookupKey(parsedLine, globalValues, lookupKey);
			previouslyUsedKey = lookupKey;
			previouslyUsedValue = surrogateKey;
			if (turboCacheMisses != null) {
				turboCacheMisses.inc();
			}
			return surrogateKey;
		}
	}

	@Override
	public Object getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return delegate.getLastLineBulkLoadValue(parsedLine, globalValues);
	}

	@Override
	public void closeCurrentFeed() {
		delegate.closeCurrentFeed();
	}

	@Override
	public void update(final Observable o, final Object arg) {
		if (dimensionName.equals(arg)) {
			log.info("Asked to clear previously used kay and value for dimension {}", dimensionName);
			previouslyUsedKey = NOT_SET_PREVIOUS_KEY_VALUE;
			previouslyUsedValue = null;
		}
	}

}
