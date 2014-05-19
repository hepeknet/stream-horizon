package com.threeglav.sh.bauk.files.feed;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.ThreadPoolSettings;
import com.threeglav.sh.bauk.util.BaukThreadFactory;

public abstract class AbstractFeedHandler implements FeedHandler {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	protected final List<Runnable> runnables = new LinkedList<>();
	private final ExecutorService EXEC_SERVICE;
	protected final FactFeed factFeed;
	protected final BaukConfiguration config;
	protected int feedProcessingThreads = ThreadPoolSettings.THREAD_POOL_DEFAULT_SIZE;

	public AbstractFeedHandler(final FactFeed factFeed, final BaukConfiguration config) {
		if (factFeed == null || config == null) {
			throw new IllegalArgumentException("Feed and config must not be null");
		}
		this.factFeed = factFeed;
		this.config = config;
		if (this.factFeed.getThreadPoolSettings() != null) {
			feedProcessingThreads = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.FEED_PROCESSING_THREADS_PARAM_NAME,
					factFeed.getThreadPoolSettings().getEtlProcessingThreadCount());
		}
		log.debug("Will use {} threads to process incoming files for {}", feedProcessingThreads, factFeed.getName());
		if (feedProcessingThreads > 0) {
			EXEC_SERVICE = Executors.newFixedThreadPool(feedProcessingThreads, new BaukThreadFactory("feedHandlingThreadGroup", "etl-thread-"
					+ factFeed.getName()));
		} else {
			EXEC_SERVICE = null;
		}
	}

	@Override
	public int start() {
		if (feedProcessingThreads > 0) {
			for (final Runnable r : runnables) {
				EXEC_SERVICE.submit(r);
			}
			final int size = runnables.size();
			runnables.clear();
			return size;
		}
		return 0;
	}

	@Override
	public void stop() {
		if (EXEC_SERVICE != null) {
			EXEC_SERVICE.shutdown();
			try {
				final boolean allDone = EXEC_SERVICE.awaitTermination(BaukConstants.WAIT_FOR_THREADS_TO_DIE_ON_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
				if (!allDone) {
					log.warn("Not all feed processing threads died within {} seconds after receiving shutdown signal. Will shutdown engine anyway!",
							BaukConstants.WAIT_FOR_THREADS_TO_DIE_ON_SHUTDOWN_SECONDS);
				}
			} catch (final InterruptedException e) {
				// ignore
			}
		}
	}

}
