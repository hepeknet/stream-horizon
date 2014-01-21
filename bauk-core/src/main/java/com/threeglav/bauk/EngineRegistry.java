package com.threeglav.bauk;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EngineRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(EngineRegistry.class);

	private static final AtomicInteger FEED_PROCESSING_THREADS_COUNTER = new AtomicInteger(0);

	private static final int FLUSH_DIMENSION_CACHE_TIMEOUT_SECONDS = 60;

	private static CountDownLatch THREADS_REPORTED_PAUSED;

	private static CountDownLatch CONTINUE_PROCESSING_SIGNAL;

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

}
