package com.threeglav.bauk.util;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public abstract class CacheUtil {

	private static final HazelcastInstance HAZELCAST_INSTANCE = Hazelcast.newHazelcastInstance();

	public static HazelcastInstance getHazelcastInstance() {
		return HAZELCAST_INSTANCE;
	}

}
