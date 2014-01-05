package com.threeglav.bauk.dimension.cache;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class InfinispanCacheInstanceManager implements CacheInstanceManager {

	private static final EmbeddedCacheManager manager = new DefaultCacheManager();

	@Override
	public CacheInstance getCacheInstance(final String regionName) {
		manager.defineConfiguration(regionName, new ConfigurationBuilder().eviction().strategy(EvictionStrategy.LRU).maxEntries(20000000).build());
		final Cache<String, String> c = manager.getCache(regionName);
		return new InfinispanCacheInstance(c);
	}

}
