package com.threeglav.bauk.feed.processing;


//public class MultiThreadedFeedDataProcessor extends AbstractFeedDataProcessor {
//
//	private static final int AWAIT_TIME_SECONDS = 10;
//
//	private final Lock writeLock = new ReentrantLock();
//
//	private final BlockingQueue<String> lineQueue = new LinkedBlockingQueue<>();
//	private final ExecutorService executorService;
//	private final AtomicInteger totalLinesOutputCounter = new AtomicInteger(0);
//	private CountDownLatch allDone;
//	private AtomicInteger expectedLines;
//	private final int maxDrainedElements;
//	private volatile Map<String, String> globalAttributes;
//
//	public MultiThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier,
//			final int numberOfThreads) {
//		super(factFeed, config, routeIdentifier);
//		if (numberOfThreads < 2) {
//			throw new IllegalArgumentException("Number of threads must be great than 1!");
//		}
//		final ThreadFactory threadFactory = new BaukThreadFactory("bauk-app", "feed-processing");
//		executorService = Executors.newFixedThreadPool(numberOfThreads, threadFactory);
//		for (int i = 0; i < numberOfThreads; i++) {
//			executorService.submit(new FeedDataProcessingTask());
//		}
//		maxDrainedElements = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.MAX_DRAINED_ELEMENTS_SYS_PARAM_NAME,
//				SystemConfigurationConstants.DEFAULT_MAX_DRAINED_ELEMENTS);
//	}
//
//	@Override
//	public void startFeed(final Map<String, String> globalAttributes) {
//		if (totalLinesOutputCounter.get() != 0) {
//			throw new IllegalStateException("Expected counter to be 0 but is " + totalLinesOutputCounter.get());
//		}
//		super.startFeed(globalAttributes);
//		this.globalAttributes = globalAttributes;
//		allDone = new CountDownLatch(1);
//		expectedLines = null;
//	}
//
//	@Override
//	public void closeFeed(final int expected, final Map<String, String> globalAttributes) {
//		this.globalAttributes = globalAttributes;
//		expectedLines = new AtomicInteger(expected);
//		log.debug("Closing feed. Expected lines {}", expected);
//		try {
//			final boolean allLinesProcessed = allDone.await(AWAIT_TIME_SECONDS, TimeUnit.SECONDS);
//			if (!allLinesProcessed) {
//				log.error("Not all lines were processed in {} seconds. Expected lines {}, so far processed {}", AWAIT_TIME_SECONDS, expected,
//						totalLinesOutputCounter.get());
//				throw new IllegalStateException("Waited too long to process " + expected + " lines!");
//			}
//		} catch (final InterruptedException e) {
//			e.printStackTrace();
//		}
//		log.debug("Successfully processed all {} lines", expectedLines);
//		totalLinesOutputCounter.set(0);
//		allDone = null;
//		super.closeFeed(expected, globalAttributes);
//	}
//
//	@Override
//	public void processLine(final String line, final Map<String, String> globalAttributes, final boolean isLastLine) {
//		final boolean added = lineQueue.add(line);
//		if (!added) {
//			throw new IllegalStateException("Was not able to add line to queue! Switch to single threaded implementation!");
//		}
//	}
//
//	private class FeedDataProcessingTask implements Callable<Void> {
//
//		@Override
//		public Void call() throws Exception {
//			final List<String> drainedElements = new ArrayList<>(maxDrainedElements);
//			try {
//				while (true) {
//					final int totalDrained = lineQueue.drainTo(drainedElements, maxDrainedElements);
//					if (totalDrained > 0) {
//						if (isTraceEnabled) {
//							log.trace("In total drained {} elements", totalDrained);
//						}
//						this.processAllDrainedElements(drainedElements);
//					} else {
//						final String singleLine = lineQueue.poll(300, TimeUnit.MILLISECONDS);
//						if (singleLine != null) {
//							drainedElements.add(singleLine);
//							this.processAllDrainedElements(drainedElements);
//						} else {
//							this.checkAllProcessedAndNotify();
//						}
//					}
//				}
//			} catch (final Exception exc) {
//				log.error("Exception while processing feed", exc);
//				throw exc;
//			}
//		}
//
//		private void processAllDrainedElements(final List<String> drainedElements) {
//			final int elementCount = drainedElements.size();
//			for (int i = 0; i < elementCount; i++) {
//				// final String line = drainedElements.get(i);
//				// final String[] parsedData = feedParserComponent.parseData(line);
//				// final String lineForOutput = bulkoutputResolver.resolveValuesAsSingleLine(parsedData,
//				// globalAttributes, true);
//				// drainedElements.set(i, lineForOutput);
//
//			}
//			if (isDebugEnabled) {
//				log.debug("Will output {} lines", elementCount);
//			}
//			this.writeOutputValues(drainedElements, elementCount);
//		}
//
//		private void writeOutputValues(final List<String> drainedElements, final int elementCount) {
//			try {
//				writeLock.lock();
//				if (elementCount > 0) {
//					for (int i = 0; i < elementCount; i++) {
//						drainedElements.get(i);
//						throw new IllegalStateException("Not implemented!");
//						// bulkOutputWriter.doOutput(line);
//					}
//					final int value = totalLinesOutputCounter.addAndGet(elementCount);
//					if (isDebugEnabled) {
//						log.debug("In total output {} lines so far. Current expected value is {}", value, expectedLines);
//					}
//				}
//				this.checkAllProcessedAndNotify();
//			} finally {
//				drainedElements.clear();
//				writeLock.unlock();
//			}
//		}
//
//		private void checkAllProcessedAndNotify() {
//			final int value = totalLinesOutputCounter.get();
//			if (expectedLines != null) {
//				if (value == expectedLines.get()) {
//					if (isDebugEnabled) {
//						log.debug("Processed all required {} lines. Notifying to close output file!", value);
//					}
//					allDone.countDown();
//				}
//			}
//		}
//
//	}
//
// }
