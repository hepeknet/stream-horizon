package com.threeglav.bauk.util;

import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

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

}
