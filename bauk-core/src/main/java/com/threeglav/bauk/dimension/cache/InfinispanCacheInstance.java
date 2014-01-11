package com.threeglav.bauk.dimension.cache;

import java.util.Map;

import org.infinispan.Cache;

public class InfinispanCacheInstance implements CacheInstance {

	private final Cache<String, Integer> cache;

	public InfinispanCacheInstance(final Cache<String, Integer> cache) {
		this.cache = cache;
	}

	@Override
	public Integer getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final Integer surrogateKey) {
		cache.putIfAbsent(naturalKey, surrogateKey);
	}

	@Override
	public void putAll(final Map<String, Integer> values) {
		cache.putAll(values);
	}

}
