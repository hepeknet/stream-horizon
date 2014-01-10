package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.TObjectIntHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.util.MetricsUtil;

public final class DimensionCache {

	private static final int NO_ENTRY_INT_VALUE = Integer.MIN_VALUE;

	private static final int MAX_ELEMENTS_LOCAL_MAP = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME, SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT);

	private static final boolean LOCAL_CACHE_DISABLED = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_DISABLED, false);

	private static final boolean PER_THREAD_CACHING_ENABLED = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.ENABLE_PER_THREAD_CACHING_PARAM_NAME, false);

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final TObjectIntHashMap<String> localCache = new TObjectIntHashMap<>(MAX_ELEMENTS_LOCAL_MAP);

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

	public DimensionCache(final CacheInstance cacheInstance, final String routeIdentifier, final String dimeName) {
		this.cacheInstance = cacheInstance;
		if (!LOCAL_CACHE_DISABLED) {
			localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimeName + "] - local cache clear times",
					false);
		}
	}

	public Integer getSurrogateKeyFromCache(final String cacheKey) {
		KeyValuePair kvp = null;
		if (!PER_THREAD_CACHING_ENABLED) {
			kvp = lastUsedPerThreadPair.get();
			if (cacheKey.equals(kvp.key)) {
				return kvp.value;
			}
		}
		if (!LOCAL_CACHE_DISABLED) {
			final int locallyCachedValue = localCache.get(cacheKey);
			if (locallyCachedValue != NO_ENTRY_INT_VALUE) {
				if (!PER_THREAD_CACHING_ENABLED) {
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
				if (!PER_THREAD_CACHING_ENABLED) {
					kvp.key = cacheKey;
					kvp.value = cachedValue;
					lastUsedPerThreadPair.set(kvp);
				}
			}
		}
		return cachedValue;
	}

	public void putInCache(final String cacheKey, final int cachedValue) {
		cacheInstance.put(cacheKey, cachedValue);
		if (!LOCAL_CACHE_DISABLED) {
			this.putInLocalCache(cacheKey, cachedValue);
		}
	}

	private void putInLocalCache(final String cacheKey, final int cachedValue) {
		if (localCache.size() > MAX_ELEMENTS_LOCAL_MAP) {
			log.debug("Local cache has more than {} elements. Have to clear it!", MAX_ELEMENTS_LOCAL_MAP);
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
