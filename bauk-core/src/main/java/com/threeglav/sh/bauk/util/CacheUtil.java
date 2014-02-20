package com.threeglav.sh.bauk.util;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.sh.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.sh.bauk.dimension.cache.InfinispanCacheInstanceManager;

public abstract class CacheUtil {

	private static CacheInstanceManager cacheInstanceManager;

	public synchronized static CacheInstanceManager getCacheInstanceManager() {
		if (cacheInstanceManager == null) {
			final String cacheProvider = ConfigurationProperties
					.getSystemProperty(BaukEngineConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME, "ispn");
			if ("hazelcast".equalsIgnoreCase(cacheProvider)) {
				BaukUtil.logEngineMessage("Using hazelcast cache provider! Set system property "
						+ BaukEngineConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME + " to value ispn to change this!");
				cacheInstanceManager = new HazelcastCacheInstanceManager();
			} else {
				BaukUtil.logEngineMessage("Using infinispan cache provider! Set system property "
						+ BaukEngineConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME + " to value hazelcast to change this!");
				cacheInstanceManager = new InfinispanCacheInstanceManager();
			}
		}
		return cacheInstanceManager;
	}

}
