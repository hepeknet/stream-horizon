package com.threeglav.bauk.dimension.cache;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;

public class InfinispanCacheInstance implements CacheInstance {

	private final AdvancedCache<String, Integer> cache;

	public InfinispanCacheInstance(final Cache<String, Integer> cache) {
		this.cache = cache.getAdvancedCache();
	}

	@Override
	public Integer getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final Integer surrogateKey) {
		cache.withFlags(Flag.IGNORE_RETURN_VALUES).putIfAbsent(naturalKey, surrogateKey);
	}

	@Override
	public void putAll(final Map<String, Integer> values) {
		cache.putAll(values);
	}

	@Override
	public void clear() {
		cache.clear();
	}

}
