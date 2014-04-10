package com.threeglav.sh.bauk.dimension;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.Dimension;

public class DimensionCacheMapImpl extends AbstractDimensionCache {

	private final ConcurrentHashMap<String, Integer> localCache = new ConcurrentHashMap<>();

	public DimensionCacheMapImpl(final CacheInstance cacheInstance, final Dimension dimension) {
		super(cacheInstance, dimension);
	}

	@Override
	protected Integer getFromLocalCache(final String cacheKey) {
		return localCache.get(cacheKey);
	}

	@Override
	protected void putAllToLocalCache(final Map<String, Integer> valuesToCache) {
		localCache.putAll(valuesToCache);
	}

	@Override
	protected void putInLocalCache(final String cacheKey, final int cachedValue) {
		localCache.put(cacheKey, cachedValue);
	}

	@Override
	protected void clearLocalCache() {
		localCache.clear();
	}

	@Override
	protected void removeFromLocalCache(final String cacheKey) {
		localCache.remove(cacheKey);
	}

	@Override
	protected int getLocalCacheSize() {
		return localCache.size();
	}

}
