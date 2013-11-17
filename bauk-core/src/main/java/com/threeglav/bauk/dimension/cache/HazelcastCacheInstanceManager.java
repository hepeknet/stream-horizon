package com.threeglav.bauk.dimension.cache;

import com.threeglav.bauk.util.StringUtil;

public class HazelcastCacheInstanceManager implements CacheInstanceManager {

	@Override
	public CacheInstance getCacheInstance(final String regionName) {
		if (StringUtil.isEmpty(regionName)) {
			throw new IllegalArgumentException("Region name must not be null or empty");
		}
		return new HazelcastCacheInstance(regionName);
	}

}
