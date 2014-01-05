package com.threeglav.bauk.util;

import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.bauk.dimension.cache.InfinispanCacheInstanceManager;

public abstract class CacheUtil {

	private static final HazelcastInstance HAZELCAST_INSTANCE = Hazelcast.newHazelcastInstance();

	public static HazelcastInstance getHazelcastInstance() {
		return HAZELCAST_INSTANCE;
	}

	public static void shutdownHazelcast() {
		try {
			HAZELCAST_INSTANCE.shutdown();
		} catch (final Exception ignored) {
			//
		}
	}

	public static int getNumberOfBaukInstances() {
		int numberOfInstances = 1;
		final Set<Member> members = getHazelcastInstance().getCluster().getMembers();
		if (members != null) {
			numberOfInstances = members.size();
		}
		return numberOfInstances;
	}

	public static CacheInstanceManager getCacheInstanceManager() {
		final String cacheProvider = System.getProperty(SystemConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME, "ispn");
		if ("hazelcast".equalsIgnoreCase(cacheProvider)) {
			BaukUtil.logEngineMessage("Will use hazelcast cache provider! Set system property "
					+ SystemConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME + " to value ispn to change this!");
			return new HazelcastCacheInstanceManager();
		} else {
			BaukUtil.logEngineMessage("Will use infinispan cache provider! Set system property "
					+ SystemConfigurationConstants.CACHE_PROVIDER_SYS_PARAM_NAME + " to value hazelcast to change this!");
			return new InfinispanCacheInstanceManager();
		}
	}

}
