package com.threeglav.bauk.dimension.cache;

import org.infinispan.Cache;

public class InfinispanCacheInstance implements CacheInstance {

	private final Cache<String, String> cache;

	public InfinispanCacheInstance(final Cache<String, String> cache) {
		this.cache = cache;
	}

	@Override
	public String getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final String surrogateKey) {
		cache.putIfAbsent(naturalKey, surrogateKey);
	}

}
