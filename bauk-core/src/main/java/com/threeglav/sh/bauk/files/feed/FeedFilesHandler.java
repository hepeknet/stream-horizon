package com.threeglav.sh.bauk.files.feed;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.files.FileAttributesHashedNameFilter;
import com.threeglav.sh.bauk.files.FileFindingHandler;
import com.threeglav.sh.bauk.files.FileProcessingErrorHandler;
import com.threeglav.sh.bauk.files.MoveFileErrorHandler;
import com.threeglav.sh.bauk.files.ThroughputTestingFileFindingHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.ThreadPoolSizes;
import com.threeglav.sh.bauk.util.BaukThreadFactory;

public class FeedFilesHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int feedProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;
	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int feedFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME,
			BaukEngineConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);

	private static final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);

	private final ExecutorService EXEC_SERVICE;

	private final List<Runnable> runnables = new LinkedList<>();

	public FeedFilesHandler(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		this.validate();
		if (this.factFeed.getThreadPoolSizes() != null) {
			feedProcessingThreads = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.FEED_PROCESSING_THREADS_PARAM_NAME,
					factFeed.getThreadPoolSizes().getFeedProcessingThreads());
		}
		log.debug("Will use {} threads to process incoming files for {}", feedProcessingThreads, factFeed.getName());
		moveToErrorFileProcessor = new MoveFileErrorHandler(config.getErrorDirectory());
		if (feedProcessingThreads > 0) {
			EXEC_SERVICE = Executors.newFixedThreadPool(feedProcessingThreads, new BaukThreadFactory("feedHandlingThreadGroup", "feed-processing-"
					+ factFeed.getName()));
		} else {
			EXEC_SERVICE = null;
		}
	}

	private void validate() {
		if (factFeed.getFileNameMasks() == null || factFeed.getFileNameMasks().isEmpty()) {
			throw new IllegalArgumentException("Could not find any file masks for " + factFeed.getName() + ". Check your configuration!");
		}
	}

	private void createSingleFileHandler(final int processingThreadId, final String fullFileMask) {
		final FeedFileProcessor bfp = new FeedFileProcessor(factFeed, config, fullFileMask);
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, processingThreadId, feedProcessingThreads,
				feedFileAcceptanceTimeoutMillis);
		Runnable ffh;
		final String sourceDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME,
				config.getSourceDirectory());
		if (throughputTestingMode) {
			ffh = new ThroughputTestingFileFindingHandler(sourceDirectory, bfp, fileFilter, moveToErrorFileProcessor);
		} else {
			ffh = new FileFindingHandler(sourceDirectory, bfp, fileFilter, moveToErrorFileProcessor);
		}
		runnables.add(ffh);
	}

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

	public void stop() {
		if (EXEC_SERVICE != null) {
			EXEC_SERVICE.shutdown();
		}
	}

	public void createFileHandlers() {
		if (feedProcessingThreads > 0) {
			for (final String fileMask : factFeed.getFileNameMasks()) {
				log.debug("Creating {} processing threads for {}", feedProcessingThreads, fileMask);
				for (int i = 0; i < feedProcessingThreads; i++) {
					this.createSingleFileHandler(i, fileMask);
					log.debug("Created processing thread #{} for {}", i, fileMask);
				}
				log.debug("Created in total {} threads for processing {}", feedProcessingThreads, fileMask);
			}
		}
	}
}
