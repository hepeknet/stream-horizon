package com.threeglav.bauk;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EngineRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(EngineRegistry.class);

	public static final AtomicInteger CURRENT_IN_PROGRESS_JOBS = new AtomicInteger(0);

	public static void reportJobInProgress() {
		final int val = CURRENT_IN_PROGRESS_JOBS.incrementAndGet();
		LOG.debug("Job started. Currently have {} jobs in progress", val);
	}

	public static void reportFinishedJob() {
		final int val = CURRENT_IN_PROGRESS_JOBS.decrementAndGet();
		if (val < 0) {
			throw new IllegalStateException("Number of jobs in progress must not be < 0");
		}
		LOG.debug("One job finished. Currently have {} jobs in progress", val);
	}

}
