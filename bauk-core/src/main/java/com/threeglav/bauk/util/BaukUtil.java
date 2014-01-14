package com.threeglav.bauk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaukUtil {

	private static final Logger ENGINE_LOG = LoggerFactory.getLogger("feedEngineLogger");
	private static final Logger BULK_LOAD_ENGINE_LOG = LoggerFactory.getLogger("bulkLoadEngineLogger");

	private static volatile boolean shutdownStarted = false;

	public static void logEngineMessage(final String message) {
		ENGINE_LOG.info(message);
		System.out.println(message);
	}

	public static void logBulkLoadEngineMessage(final String message) {
		BULK_LOAD_ENGINE_LOG.info(message);
		System.out.println("BULK-LOAD " + message);
	}

	public static void startShutdown() {
		shutdownStarted = true;
	}

	public static boolean shutdownStarted() {
		return shutdownStarted;
	}

}
