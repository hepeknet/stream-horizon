package com.threeglav.sh.bauk.dimension.cache;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IMap;
import com.threeglav.sh.bauk.util.StringUtil;

public final class HazelcastCacheInstance implements CacheInstance {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final IMap<String, Integer> cache;

	private final boolean isDebugEnabled;

	public HazelcastCacheInstance(final String name) {
		if (StringUtil.isEmpty(name)) {
			throw new IllegalArgumentException("Name must not be null or empty");
		}
		cache = HazelcastCacheInstanceManager.getInstance().getMap(name);
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public Integer getSurrogateKey(final String naturalKey) {
		return cache.get(naturalKey);
	}

	@Override
	public void put(final String naturalKey, final Integer surrogateKey) {
		cache.set(naturalKey, surrogateKey);
		if (isDebugEnabled) {
			log.debug("Cached [{}] -> [{}]", naturalKey, surrogateKey);
		}
	}

	@Override
	public void putAll(final Map<String, Integer> values) {
		cache.putAll(values);
	}

	@Override
	public void clear() {
		cache.clear();
	}

	@Override
	public void remove(final String key) {
		cache.remove(key);
	}

}
