package com.threeglav.sh.bauk.dimension.cache;

import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.threeglav.sh.bauk.util.StringUtil;

public class HazelcastCacheInstanceManager implements CacheInstanceManager {

	private static HazelcastInstance HAZELCAST_INSTANCE;

	synchronized static HazelcastInstance getInstance() {
		if (HAZELCAST_INSTANCE == null) {
			HAZELCAST_INSTANCE = Hazelcast.newHazelcastInstance();
		}
		return HAZELCAST_INSTANCE;
	}

	@Override
	public CacheInstance getCacheInstance(final String regionName) {
		if (StringUtil.isEmpty(regionName)) {
			throw new IllegalArgumentException("Region name must not be null or empty");
		}
		return new HazelcastCacheInstance(regionName);
	}

	@Override
	public void stop() {
		if (HAZELCAST_INSTANCE != null) {
			HAZELCAST_INSTANCE.shutdown();
		}
	}

	public static int getNumberOfBaukInstances() {
		int numberOfInstances = 1;
		final Set<Member> members = getInstance().getCluster().getMembers();
		if (members != null) {
			numberOfInstances = members.size();
		}
		return numberOfInstances;
	}

}
