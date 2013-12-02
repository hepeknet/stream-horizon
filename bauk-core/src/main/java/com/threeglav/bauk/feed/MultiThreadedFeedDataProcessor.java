package com.threeglav.bauk.feed;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public class MultiThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	private final Object bulkWriterLock = new Object();

	private final BlockingQueue<String> lineQueue = new LinkedBlockingQueue<>();
	private final ExecutorService executorService;
	private final AtomicInteger totalLinesOutputCounter = new AtomicInteger(0);
	private CountDownLatch allDone;
	private volatile AtomicInteger expectedLines;
	private final int maxDrainedElements;

	public MultiThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier,
			final int numberOfThreads) {
		super(factFeed, config, routeIdentifier);
		if (numberOfThreads < 1) {
			throw new IllegalArgumentException("Number of threads must not be non-positive integer!");
		}
		executorService = Executors.newFixedThreadPool(numberOfThreads);
		for (int i = 0; i < numberOfThreads; i++) {
			executorService.submit(new FeedDataProcessingTask());
		}
		maxDrainedElements = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.MAX_DRAINED_ELEMENTS_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_MAX_DRAINED_ELEMENTS);
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
	public void closeFeed(final int expected) {
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
		super.closeFeed(expected);
	}

	@Override
	public void processLine(final String line) {
		final boolean added = lineQueue.add(line);
		if (!added) {
			throw new IllegalStateException("Was not able to add line to queue! Switch to single threaded implementation!");
		}
	}

	private class FeedDataProcessingTask implements Callable<Void> {

		@Override
		public Void call() throws Exception {
			final List<String> drainedElements = new LinkedList<>();
			while (true) {
				final int totalDrained = lineQueue.drainTo(drainedElements, maxDrainedElements);
				if (totalDrained > 0) {
					log.debug("In total drained {} elements", totalDrained);
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
			final Iterator<String> listIterator = drainedElements.iterator();
			final String[] outputLines = new String[drainedElements.size()];
			int counter = 0;
			while (listIterator.hasNext()) {
				final String line = listIterator.next();
				listIterator.remove();
				final String[] parsedData = feedParserComponent.parseData(line);
				final String lineForOutput = bulkoutputResolver.resolveValues(parsedData, globalAttributes);
				outputLines[counter++] = lineForOutput;
			}
			drainedElements.clear();
			final int outputSize = outputLines.length;
			log.trace("Will output {} lines", outputSize);
			synchronized (bulkWriterLock) {
				for (final String str : outputLines) {
					bulkWriter.write(str);
					bulkWriter.write("\n");
				}
				final int value = totalLinesOutputCounter.addAndGet(outputSize);
				log.trace("In total output {} lines so far. Current expected value is {}", value, expectedLines);
				if (expectedLines != null) {
					if (value == expectedLines.get()) {
						log.debug("Processed all required {} lines. Notifying to close output file!", value);
						allDone.countDown();
					}
				}
			}
		}
	}

}
