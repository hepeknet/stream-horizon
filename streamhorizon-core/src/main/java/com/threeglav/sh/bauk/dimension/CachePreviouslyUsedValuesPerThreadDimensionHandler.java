package com.threeglav.sh.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.events.EngineEvents;
import com.threeglav.sh.bauk.util.StringUtil;

/**
 * For every thread there is one instance of this class
 * 
 * @author Borisa
 * 
 */
public final class CachePreviouslyUsedValuesPerThreadDimensionHandler implements BulkLoadOutputValueHandler, Observer {

	private static final int MAX_LOCALLY_CACHED_VALUES = 100000;

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String NOT_SET_PREVIOUS_KEY_VALUE = "___";

	private final InsertOnlyDimensionHandler delegate;
	private String previouslyUsedKey = NOT_SET_PREVIOUS_KEY_VALUE;
	private Integer previouslyUsedValue;
	private final String dimensionName;
	private final StringBuilder reusedForPerformance = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);

	private final Map<String, Integer> perThreadCachedValues = new THashMap<>(5000);

	public CachePreviouslyUsedValuesPerThreadDimensionHandler(final BulkLoadOutputValueHandler delegate) {
		this.delegate = (InsertOnlyDimensionHandler) delegate;
		dimensionName = this.delegate.getDimension().getName();
		log.info("Will cache previously used values for dimension {}", dimensionName);
		EngineEvents.registerForFlushDimensionCache(this);
	}

	@Override
	public Integer getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		reusedForPerformance.setLength(0);
		final String lookupKey = delegate.buildNaturalKeyForCacheLookup(parsedLine, globalValues, reusedForPerformance);
		if (previouslyUsedKey.equals(lookupKey)) {
			return previouslyUsedValue;
		} else {
			final Integer cached = perThreadCachedValues.get(lookupKey);
			if (cached != null) {
				return cached;
			}
			final Integer surrogateKey = delegate.getBulkLoadValueByPrecalculatedLookupKey(parsedLine, globalValues, lookupKey);
			previouslyUsedKey = lookupKey;
			previouslyUsedValue = surrogateKey;
			perThreadCachedValues.put(lookupKey, surrogateKey);
			return surrogateKey;
		}
	}

	@Override
	public Integer getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return delegate.getLastLineBulkLoadValue(parsedLine, globalValues);
	}

	@Override
	public void closeCurrentFeed() {
		delegate.closeCurrentFeed();
		if (perThreadCachedValues.size() > MAX_LOCALLY_CACHED_VALUES) {
			perThreadCachedValues.clear();
		}
	}

	@Override
	public void update(final Observable o, final Object arg) {
		if (dimensionName.equals(arg)) {
			log.info("Asked to clear previously used kay and value for dimension {}", dimensionName);
			previouslyUsedKey = NOT_SET_PREVIOUS_KEY_VALUE;
			previouslyUsedValue = null;
			perThreadCachedValues.clear();
		}
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return true;
	}

}
