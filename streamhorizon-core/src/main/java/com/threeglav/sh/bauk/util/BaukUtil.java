package com.threeglav.sh.bauk.util;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
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
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.io.BulkOutputWriter;
import com.threeglav.sh.bauk.main.StreamHorizonEngine;

public abstract class BaukUtil {

	private static final Logger LOG = LoggerFactory.getLogger(BaukUtil.class);

	public static final DecimalFormat DEC_FORMAT = new DecimalFormat("#########.#");

	private static String engineInstanceIdentifier;

	private static final Logger ENGINE_LOG = LoggerFactory.getLogger("feedEngineLogger");
	private static final Logger BULK_LOAD_ENGINE_LOG = LoggerFactory.getLogger("bulkLoadEngineLogger");

	private static BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>();
	private static RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
	private static ExecutorService LOGGING_EXEC_SERVICE = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS, blockingQueue,
			new BaukThreadFactory("engine-logging", "engine-logging"), rejectedExecutionHandler);

	private static volatile boolean shutdownStarted = false;

	public static void logEngineMessage(final String message) {
		LOGGING_EXEC_SERVICE.submit(new Runnable() {
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
		LOGGING_EXEC_SERVICE.submit(new Runnable() {
			@Override
			public void run() {
				final long loadedSoFar = EngineRegistry.getSuccessfulBulkFilesCount();
				final boolean shouldReportCount = ((loadedSoFar % 10) == 0);
				if (!shouldReportCount) {
					BULK_LOAD_ENGINE_LOG.info(message);
				} else {
					BULK_LOAD_ENGINE_LOG.info(message + ". Bulk loaded approximately " + loadedSoFar + " files so far");
				}
			}
		});
	}

	public static void startShutdown() {
		shutdownStarted = true;
		LOGGING_EXEC_SERVICE.shutdown();
	}

	public static boolean shutdownStarted() {
		return shutdownStarted;
	}

	public static void populateEngineImplicitAttributes(final Map<String, String> globalAttributes) {
		globalAttributes.put(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_START_TIME,
				String.valueOf(StreamHorizonEngine.getEngineInstanceStartTime()));
		globalAttributes.put(BaukConstants.ENGINE_IMPLICIT_ATTRIBUTE_INSTANCE_IDENTIFIER, getEngineInstanceIdentifier());
	}

	public static String getEngineInstanceIdentifier() {
		if (engineInstanceIdentifier == null) {
			engineInstanceIdentifier = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BAUK_INSTANCE_ID_PARAM_NAME, "");
		}
		return engineInstanceIdentifier;
	}

	public static BulkOutputWriter loadWriterByProtocol(final String protocol) throws InstantiationException, IllegalAccessException {
		if (StringUtil.isEmpty(protocol)) {
			throw new IllegalArgumentException("Protocol must not be null or empty!");
		}
		LOG.debug("Trying to find bulk output writer that understands protocol {}", protocol);
		final ServiceLoader<BulkOutputWriter> loader = ServiceLoader.load(BulkOutputWriter.class);
		final Iterator<BulkOutputWriter> iterator = loader.iterator();
		while (iterator.hasNext()) {
			final BulkOutputWriter bow = iterator.next();
			if (bow.understandsURI(protocol)) {
				final BulkOutputWriter bulkWriterInstance = bow.getClass().newInstance();
				LOG.debug("Found bulk output writer that understands protocol {}", protocol);
				return bulkWriterInstance;
			}
		}
		throw new IllegalStateException("Was not able to find bulk output writer that understands protocol [" + protocol + "]");
	}

	public static boolean isWindowsPlatform() {
		final String osName = System.getProperty("os.name");
		return osName != null && osName.toLowerCase().contains("windows");
	}

}
