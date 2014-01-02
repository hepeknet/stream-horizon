package com.threeglav.bauk.dimension.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;
import com.threeglav.bauk.util.CacheUtil;
import com.threeglav.bauk.util.StringUtil;

public class HazelcastCacheInstance implements CacheInstance {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final IMap<String, String> cache;

	public HazelcastCacheInstance(final String name) {
		if (StringUtil.isEmpty(name)) {
			throw new IllegalArgumentException("Name must not be null or empty");
		}
		cache = CacheUtil.getHazelcastInstance().getMap(name);
	}

	@Override
	public String getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final String surrogateKey) {
		cache.put(naturalKey, surrogateKey);
		log.debug("Cached [{}] -> [{}]", naturalKey, surrogateKey);
	}

}
