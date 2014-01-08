package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.util.MetricsUtil;

public final class DimensionCache {

	private static final int MAX_ELEMENTS_LOCAL_MAP = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME, SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT);

	private static final boolean localCacheDisabled = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_DISABLED, false);

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final THashMap<String, String> localCache = new THashMap<>(MAX_ELEMENTS_LOCAL_MAP);

	private final CacheInstance cacheInstance;

	private Counter localCacheClearCounter;

	public DimensionCache(final CacheInstance cacheInstance, final String routeIdentifier, final String dimeName) {
		this.cacheInstance = cacheInstance;
		if (!localCacheDisabled) {
			localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimeName + "] - local cache clear times",
					false);
		}
	}

	public String getSurrogateKeyFromCache(final String cacheKey) {
		String locallyCachedValue = null;
		if (!localCacheDisabled) {
			locallyCachedValue = localCache.get(cacheKey);
		}
		String lastAskedValue;
		if (locallyCachedValue != null) {
			lastAskedValue = locallyCachedValue;
		} else {
			final String cachedValue = cacheInstance.getSurrogateKey(cacheKey);
			if (cachedValue != null) {
				if (!localCacheDisabled) {
					this.putInLocalCache(cacheKey, cachedValue);
				}
			}
			lastAskedValue = cachedValue;
		}
		return lastAskedValue;
	}

	public void putInCache(final String cacheKey, final String cachedValue) {
		cacheInstance.put(cacheKey, cachedValue);
		if (!localCacheDisabled) {
			this.putInLocalCache(cacheKey, cachedValue);
		}
	}

	private void putInLocalCache(final String cacheKey, final String cachedValue) {
		if (localCache.size() > MAX_ELEMENTS_LOCAL_MAP) {
			log.debug("Local cache has more than {} elements. Have to clear it!", MAX_ELEMENTS_LOCAL_MAP);
			localCache.clear();
			if (localCacheClearCounter != null) {
				localCacheClearCounter.inc();
			}
		}
		localCache.putIfAbsent(cacheKey, cachedValue);
	}

}
