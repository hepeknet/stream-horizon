package com.threeglav.bauk.dimension.cache;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.threeglav.bauk.util.StringUtil;

public class HazelcastCacheInstance implements CacheInstance {

	private final IMap<String, String> cache;
	private static final HazelcastInstance HAZELCAST_INSTANCE = Hazelcast.newHazelcastInstance();

	public HazelcastCacheInstance(final String name) {
		if (StringUtil.isEmpty(name)) {
			throw new IllegalArgumentException("Name must not be null or empty");
		}
		cache = HAZELCAST_INSTANCE.getMap(name);
	}

	@Override
	public String getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final String surrogateKey) {
		cache.put(naturalKey, surrogateKey);
	}

}
