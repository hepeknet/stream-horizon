package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.events.EngineEvents;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.util.MetricsUtil;

public final class DimensionCache implements Observer {

	private static final int NO_ENTRY_INT_VALUE = Integer.MIN_VALUE;

	private static final boolean LOCAL_CACHE_DISABLED = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_DISABLED, false);

	private static final int MAX_ELEMENTS_LOCAL_MAP = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME, SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT);

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private TObjectIntHashMap<String> localCache;

	private final int maxElementsInLocalCache;

	private final CacheInstance cacheInstance;

	private Counter localCacheClearCounter;

	private final Counter dimensionCacheFlushCounter;

	private final Dimension dimension;

	private final boolean isDebugEnabled;

	static {
		System.setProperty("gnu.trove.no_entry.int", "MIN_VALUE");
	}

	public DimensionCache(final CacheInstance cacheInstance, final Dimension dimension) {
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		int maxElementsInLocalCacheForDimension = MAX_ELEMENTS_LOCAL_MAP;
		if (dimension.getLocalCacheMaxSize() != null) {
			maxElementsInLocalCacheForDimension = dimension.getLocalCacheMaxSize().intValue();
		}
		log.info("For dimension {} local cache will hold at most {} elements", dimension.getName(), maxElementsInLocalCacheForDimension);
		this.cacheInstance = cacheInstance;
		if (!LOCAL_CACHE_DISABLED) {
			localCacheClearCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - fast cache resets", false);
			localCache = new TObjectIntHashMap<>(maxElementsInLocalCacheForDimension);
		}
		dimensionCacheFlushCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - cache flush executions", false);
		maxElementsInLocalCache = maxElementsInLocalCacheForDimension;
		isDebugEnabled = log.isDebugEnabled();
		EngineEvents.registerForFlushDimensionCache(this);
	}

	public Integer getSurrogateKeyFromCache(final String cacheKey) {
		if (!LOCAL_CACHE_DISABLED) {
			final int locallyCachedValue = localCache.get(cacheKey);
			if (locallyCachedValue != NO_ENTRY_INT_VALUE) {
				return locallyCachedValue;
			}
		}
		final Integer cachedValue = cacheInstance.getSurrogateKey(cacheKey);
		if (cachedValue != null) {
			if (!LOCAL_CACHE_DISABLED) {
				this.putInLocalCache(cacheKey, cachedValue);
			}
		}
		return cachedValue;
	}

	public int putAllInCache(final List<DimensionKeysPair> values) {
		final int batchSize = 10000;
		final Iterator<DimensionKeysPair> iter = values.iterator();
		final Map<String, Integer> valuesToCache = new HashMap<>();
		int valuesCached = 0;
		while (iter.hasNext()) {
			if (valuesToCache.size() == batchSize) {
				cacheInstance.putAll(valuesToCache);
				valuesCached += valuesToCache.size();
				valuesToCache.clear();
			}
			final DimensionKeysPair row = iter.next();
			final int surrogateKeyValue = row.surrogateKey;
			final String naturalKeyValue = row.naturalKey;
			valuesToCache.put(naturalKeyValue, surrogateKeyValue);
		}
		if (!valuesToCache.isEmpty()) {
			cacheInstance.putAll(valuesToCache);
			valuesCached += valuesToCache.size();
			valuesToCache.clear();
		}
		return valuesCached;
	}

	public void putInCache(final String cacheKey, final int cachedValue) {
		cacheInstance.put(cacheKey, cachedValue);
		if (!LOCAL_CACHE_DISABLED) {
			this.putInLocalCache(cacheKey, cachedValue);
		}
	}

	private void putInLocalCache(final String cacheKey, final int cachedValue) {
		if (localCache.size() > maxElementsInLocalCache) {
			if (isDebugEnabled) {
				log.debug("Local cache for dimension {} has more than {} elements. Have to clear it!", dimension.getName(), maxElementsInLocalCache);
			}
			localCache.clear();
			if (localCacheClearCounter != null) {
				localCacheClearCounter.inc();
			}
		}
		localCache.putIfAbsent(cacheKey, cachedValue);
	}

	@Override
	public void update(final Observable o, final Object arg) {
		final String dimensionName = (String) arg;
		log.debug("Got request to flush cache for dimension {}", dimensionName);
		if (dimensionName.equals(dimension.getName())) {
			log.debug("Matches with dimension I am responsible for {}", dimensionName);
			if (!LOCAL_CACHE_DISABLED) {
				localCache.clear();
			}
			cacheInstance.clear();
			if (dimensionCacheFlushCounter != null) {
				dimensionCacheFlushCounter.inc();
			}
			log.info("Cleared caches for dimension {}", dimensionName);
		}
	}

}
