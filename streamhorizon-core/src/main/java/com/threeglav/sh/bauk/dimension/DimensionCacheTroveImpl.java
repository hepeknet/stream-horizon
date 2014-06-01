package com.threeglav.sh.bauk.dimension;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.Map;

import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.Dimension;

public final class DimensionCacheTroveImpl extends AbstractDimensionCache {

	private static final int NO_ENTRY_INT_VALUE = Integer.valueOf(0);

	private final TObjectIntHashMap<String> localCache;

	public DimensionCacheTroveImpl(final CacheInstance cacheInstance, final Dimension dimension) {
		super(cacheInstance, dimension);
		// System.setProperty("gnu.trove.no_entry.int", "MIN_VALUE");
		localCache = new TObjectIntHashMap<>(maxElementsInLocalCache);
	}

	@Override
	protected Integer getFromLocalCache(final String cacheKey) {
		if (!LOCAL_CACHE_DISABLED) {
			final int locallyCachedValue = localCache.get(cacheKey);
			if (locallyCachedValue != NO_ENTRY_INT_VALUE) {
				return locallyCachedValue;
			}
		}
		return null;
	}

	@Override
	protected void putAllToLocalCache(final Map<String, Integer> valuesToCache) {
		if (!LOCAL_CACHE_DISABLED) {
			localCache.putAll(valuesToCache);
		}
	}

	@Override
	protected void putInLocalCache(final String cacheKey, final int cachedValue) {
		if (!LOCAL_CACHE_DISABLED) {
			localCache.putIfAbsent(cacheKey, cachedValue);
		}
	}

	@Override
	protected void clearLocalCache() {
		if (!LOCAL_CACHE_DISABLED) {
			localCache.clear();
		}
	}

	@Override
	protected void removeFromLocalCache(final String cacheKey) {
		if (!LOCAL_CACHE_DISABLED) {
			localCache.remove(cacheKey);
		}
	}

	@Override
	protected int getLocalCacheSize() {
		if (!LOCAL_CACHE_DISABLED) {
			return localCache.size();
		}
		return 0;
	}

}
