package com.threeglav.bauk.feed.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.BaukThreadFactory;

public class MultiThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	private final Lock writeLock = new ReentrantLock();

	private final BlockingQueue<String> lineQueue = new LinkedBlockingQueue<>();
	private final ExecutorService executorService;
	private final AtomicInteger totalLinesOutputCounter = new AtomicInteger(0);
	private CountDownLatch allDone;
	private AtomicInteger expectedLines;
	private final int maxDrainedElements;
	private Map<String, String> globalAttributes;
	private final boolean isTraceEnabled;

	public MultiThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier,
			final int numberOfThreads) {
		super(factFeed, config, routeIdentifier);
		if (numberOfThreads < 2) {
			throw new IllegalArgumentException("Number of threads must be great than 1!");
		}
		final ThreadFactory threadFactory = new BaukThreadFactory("bauk-app", "feed-processing");
		executorService = Executors.newFixedThreadPool(numberOfThreads, threadFactory);
		for (int i = 0; i < numberOfThreads; i++) {
			executorService.submit(new FeedDataProcessingTask());
		}
		maxDrainedElements = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.MAX_DRAINED_ELEMENTS_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_MAX_DRAINED_ELEMENTS);
		// help JIT to remove dead code
		isTraceEnabled = log.isTraceEnabled();
	}

	@Override
	public void startFeed(final Map<String, String> globalAttributes) {
		if (totalLinesOutputCounter.get() != 0) {
			throw new IllegalStateException("Expected counter to be 0 but is " + totalLinesOutputCounter.get());
		}
		super.startFeed(globalAttributes);
		allDone = new CountDownLatch(1);
		expectedLines = null;
	}

	@Override
	public void closeFeed(final int expected, final Map<String, String> globalAttributes) {
		this.globalAttributes = globalAttributes;
		expectedLines = new AtomicInteger(expected);
		log.debug("Closing feed. Expected lines {}", expected);
		try {
			allDone.await();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
		log.debug("Successfully processed all {} lines", expectedLines);
		totalLinesOutputCounter.set(0);
		allDone = null;
		super.closeFeed(expected, globalAttributes);
	}

	@Override
	public void processLine(final String line, final Map<String, String> globalAttributes, final boolean isLastLine) {
		final boolean added = lineQueue.add(line);
		if (!added) {
			throw new IllegalStateException("Was not able to add line to queue! Switch to single threaded implementation!");
		}
	}

	private class FeedDataProcessingTask implements Callable<Void> {

		@Override
		public Void call() throws Exception {
			final List<String> drainedElements = new ArrayList<>(maxDrainedElements);
			while (true) {
				final int totalDrained = lineQueue.drainTo(drainedElements, maxDrainedElements);
				if (totalDrained > 0) {
					if (isTraceEnabled) {
						log.trace("In total drained {} elements", totalDrained);
					}
					this.processAllDrainedElements(drainedElements);
				} else {
					final String singleLine = lineQueue.poll(300, TimeUnit.MILLISECONDS);
					if (singleLine != null) {
						drainedElements.add(singleLine);
						this.processAllDrainedElements(drainedElements);
					} else {
						this.processAllDrainedElements(drainedElements);
					}
				}
			}
		}

		private void processAllDrainedElements(final List<String> drainedElements) {
			final int elementCount = drainedElements.size();
			for (int i = 0; i < elementCount; i++) {
				final String line = drainedElements.get(i);
				final String[] parsedData = feedParserComponent.parseData(line);
				final String lineForOutput = bulkoutputResolver.resolveValuesAsSingleLine(parsedData, globalAttributes, true);
				drainedElements.set(i, lineForOutput);
			}
			if (isTraceEnabled) {
				log.trace("Will output {} lines", elementCount);
			}
			try {
				writeLock.lock();
				for (int i = 0; i < elementCount; i++) {
					final String line = drainedElements.get(i);
					bulkOutputWriter.doOutput(line);
				}
				final int value = totalLinesOutputCounter.addAndGet(elementCount);
				if (isTraceEnabled) {
					log.trace("In total output {} lines so far. Current expected value is {}", value, expectedLines);
				}
				if (expectedLines != null) {
					if (value == expectedLines.get()) {
						log.debug("Processed all required {} lines. Notifying to close output file!", value);
						allDone.countDown();
					}
				}
			} finally {
				drainedElements.clear();
				writeLock.unlock();
			}
		}
	}

}
