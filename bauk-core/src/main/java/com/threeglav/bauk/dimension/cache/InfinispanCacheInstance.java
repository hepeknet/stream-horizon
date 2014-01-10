package com.threeglav.bauk.dimension.cache;

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

}
