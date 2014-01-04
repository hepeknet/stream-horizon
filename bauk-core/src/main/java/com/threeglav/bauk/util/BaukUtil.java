package com.threeglav.bauk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaukUtil {

	private static final Logger ENGINE_LOG = LoggerFactory.getLogger("engineLogger");

	private static volatile boolean shutdownStarted = false;

	public static void logEngineMessage(final String message) {
		ENGINE_LOG.info(message);
	}

	public static void startShutdown() {
		shutdownStarted = true;
	}

	public static boolean shutdownStarted() {
		return shutdownStarted;
	}

}
