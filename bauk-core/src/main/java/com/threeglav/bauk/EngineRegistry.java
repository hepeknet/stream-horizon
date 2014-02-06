package com.threeglav.bauk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.threeglav.bauk.util.MetricsUtil;

public abstract class EngineRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(EngineRegistry.class);

	private static final AtomicInteger FEED_PROCESSING_THREADS_COUNTER = new AtomicInteger(0);

	private static final int FLUSH_DIMENSION_CACHE_TIMEOUT_SECONDS = 60;

	private static CountDownLatch THREADS_REPORTED_PAUSED;

	private static CountDownLatch CONTINUE_PROCESSING_SIGNAL;

	private static final Meter PROCESSED_FEED_ROWS_METER = MetricsUtil.createMeter("Processed feed rows in total");

	private static final Counter SUCCESSFUL_INPUT_FILES_COUNTER = MetricsUtil.createCounter("Successfully processed feed files count");

	private static final Counter FAILED_FEED_FILES_COUNTER = MetricsUtil.createCounter("Failed processing feed files count");

	private static final Counter FAILED_BULK_FILES_COUNTER = MetricsUtil.createCounter("Failed bulk-loading files count");

	private static final Counter SUCCESSFUL_BULK_FILES_COUNTER = MetricsUtil.createCounter("Successfully bulk-loaded files count");

	private static AtomicBoolean SHOULD_PAUSE = new AtomicBoolean(false);

	public static void registerFeedProcessingThread() {
		final int val = FEED_PROCESSING_THREADS_COUNTER.incrementAndGet();
		LOG.info("In total registered {} feed processing threads so far", val);
	}

	public static boolean beginProcessingPause() {
		LOG.info("Will wait for {} feed processing threads to confirm pausing of processing");
		THREADS_REPORTED_PAUSED = new CountDownLatch(FEED_PROCESSING_THREADS_COUNTER.get());
		CONTINUE_PROCESSING_SIGNAL = new CountDownLatch(1);
		SHOULD_PAUSE.set(true);
		try {
			final boolean allPaused = THREADS_REPORTED_PAUSED.await(FLUSH_DIMENSION_CACHE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (allPaused) {
				LOG.info("All processing threads paused their work = {}", allPaused);
			} else {
				LOG.warn("Not all threads paused their work within {} seconds. Unable to flush dimension cache at this point!",
						FLUSH_DIMENSION_CACHE_TIMEOUT_SECONDS);
			}
			return allPaused;
		} catch (final InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static void endProcessingPause() {
		LOG.info("Ending processing pause");
		SHOULD_PAUSE.set(false);
		CONTINUE_PROCESSING_SIGNAL.countDown();
		LOG.info("Ended processing pause");
	}

	public static void pauseProcessingIfNeeded() {
		if (SHOULD_PAUSE.get()) {
			THREADS_REPORTED_PAUSED.countDown();
			LOG.info("Confirmed pausing of processing");
			try {
				CONTINUE_PROCESSING_SIGNAL.await();
			} catch (final InterruptedException e) {
				e.printStackTrace();
			}
			LOG.info("Continuing processing...");
		}
	}

	public static void registerProcessedFeedRows(final int rows) {
		if (PROCESSED_FEED_ROWS_METER != null) {
			PROCESSED_FEED_ROWS_METER.mark(rows);
			SUCCESSFUL_INPUT_FILES_COUNTER.inc();
		}
	}

	public static double getProcessedRowsInTheLastMinute() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return PROCESSED_FEED_ROWS_METER.getOneMinuteRate();
	}

	public static long getProcessedFeedRowsTotal() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return PROCESSED_FEED_ROWS_METER.getCount();
	}

	public static long getProcessedFeedFilesCount() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return SUCCESSFUL_INPUT_FILES_COUNTER.getCount();
	}

	public static void registerFailedFeedFile() {
		if (FAILED_FEED_FILES_COUNTER != null) {
			FAILED_FEED_FILES_COUNTER.inc();
		}
	}

	public static long getFailedFeedFilesCount() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return FAILED_FEED_FILES_COUNTER.getCount();
	}

	public static void registerFailedBulkFile() {
		if (FAILED_BULK_FILES_COUNTER != null) {
			FAILED_BULK_FILES_COUNTER.inc();
		}
	}

	public static long getFailedBulkFilesCount() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return FAILED_BULK_FILES_COUNTER.getCount();
	}

	public static void registerSuccessfulBulkFile() {
		if (SUCCESSFUL_BULK_FILES_COUNTER != null) {
			SUCCESSFUL_BULK_FILES_COUNTER.inc();
		}
	}

	public static long getSuccessfulBulkFilesCount() {
		if (MetricsUtil.isMetricsOff()) {
			return 0;
		}
		return SUCCESSFUL_BULK_FILES_COUNTER.getCount();
	}

}
