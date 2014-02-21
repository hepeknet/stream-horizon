package com.threeglav.sh.bauk.util;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.main.BaukApplication;

public abstract class BaukUtil {

	private static String engineInstanceIdentifier;

	private static final Logger ENGINE_LOG = LoggerFactory.getLogger("feedEngineLogger");
	private static final Logger BULK_LOAD_ENGINE_LOG = LoggerFactory.getLogger("bulkLoadEngineLogger");

	private static BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();
	private static RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
	private static ExecutorService EXEC_SERVICE = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, blockingQueue, new BaukThreadFactory(
			"bauk-logging", "bauk-logging"), rejectedExecutionHandler);

	private static volatile boolean shutdownStarted = false;

	public static void logEngineMessage(final String message) {
		EXEC_SERVICE.submit(new Runnable() {
			@Override
			public void run() {
				ENGINE_LOG.info(message);
			}
		});
	}

	public static void logEngineMessageSync(final String message) {
		ENGINE_LOG.info(message);
	}

	public static void logBulkLoadEngineMessage(final String message) {
		EXEC_SERVICE.submit(new Runnable() {
			@Override
			public void run() {
				BULK_LOAD_ENGINE_LOG.info(message);
			}
		});
	}

	public static void startShutdown() {
		shutdownStarted = true;
		EXEC_SERVICE.shutdown();
	}

	public static boolean shutdownStarted() {
		return shutdownStarted;
	}

	public static void populateEngineImplicitAttributes(final Map<String, String> globalAttributes) {
		globalAttributes.put(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_START_TIME,
				String.valueOf(BaukApplication.getEngineInstanceStartTime()));
		globalAttributes.put(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_IDENTIFIER, getEngineInstanceIdentifier());
	}

	public static String getEngineInstanceIdentifier() {
		if (engineInstanceIdentifier == null) {
			engineInstanceIdentifier = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "");
		}
		return engineInstanceIdentifier;
	}

}
