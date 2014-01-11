package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.util.MetricsUtil;

public final class DimensionCache {

	private static final int NO_ENTRY_INT_VALUE = Integer.MIN_VALUE;

	private static final boolean LOCAL_CACHE_DISABLED = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_DISABLED, false);

	private static final boolean PER_THREAD_CACHING_ENABLED = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.ENABLE_PER_THREAD_CACHING_PARAM_NAME, false);

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private TObjectIntHashMap<String> localCache;

	private final int maxElementsInLocalCache;

	private final String dimensionName;

	private static final ThreadLocal<KeyValuePair> lastUsedPerThreadPair = new ThreadLocal<KeyValuePair>() {

		@Override
		protected KeyValuePair initialValue() {
			return new KeyValuePair();
		}

	};

	private final CacheInstance cacheInstance;

	private Counter localCacheClearCounter;

	static {
		System.setProperty("gnu.trove.no_entry.int", "MIN_VALUE");
	}

	public DimensionCache(final CacheInstance cacheInstance, final String routeIdentifier, final String dimName, final int localCacheMaxSize) {
		this.cacheInstance = cacheInstance;
		if (!LOCAL_CACHE_DISABLED) {
			localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimName + "] - local cache clear times",
					false);
			localCache = new TObjectIntHashMap<>(localCacheMaxSize);
		}
		maxElementsInLocalCache = localCacheMaxSize;
		dimensionName = dimName;
	}

	public Integer getSurrogateKeyFromCache(final String cacheKey) {
		KeyValuePair kvp = null;
		if (PER_THREAD_CACHING_ENABLED) {
			kvp = lastUsedPerThreadPair.get();
			if (cacheKey.equals(kvp.key)) {
				return kvp.value;
			}
		}
		if (!LOCAL_CACHE_DISABLED) {
			final int locallyCachedValue = localCache.get(cacheKey);
			if (locallyCachedValue != NO_ENTRY_INT_VALUE) {
				if (PER_THREAD_CACHING_ENABLED) {
					kvp.key = cacheKey;
					kvp.value = locallyCachedValue;
					lastUsedPerThreadPair.set(kvp);
				}
				return locallyCachedValue;
			}
		}
		final Integer cachedValue = cacheInstance.getSurrogateKey(cacheKey);
		if (cachedValue != null) {
			if (!LOCAL_CACHE_DISABLED) {
				this.putInLocalCache(cacheKey, cachedValue);
				if (PER_THREAD_CACHING_ENABLED) {
					kvp.key = cacheKey;
					kvp.value = cachedValue;
					lastUsedPerThreadPair.set(kvp);
				}
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
			log.debug("Local cache for dimension {} has more than {} elements. Have to clear it!", dimensionName, maxElementsInLocalCache);
			localCache.clear();
			if (localCacheClearCounter != null) {
				localCacheClearCounter.inc();
			}
		}
		localCache.putIfAbsent(cacheKey, cachedValue);
	}

	static class KeyValuePair {
		public String key;
		public int value;
	}

}
