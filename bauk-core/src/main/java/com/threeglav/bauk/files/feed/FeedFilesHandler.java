package com.threeglav.bauk.files.feed;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.files.FileAttributesHashedNameFilter;
import com.threeglav.bauk.files.FileFindingHandler;
import com.threeglav.bauk.files.FileProcessingErrorHandler;
import com.threeglav.bauk.files.MoveFileErrorHandler;
import com.threeglav.bauk.files.ThroughputTestingFileFindingHandler;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.ThreadPoolSizes;
import com.threeglav.bauk.util.BaukThreadFactory;
import com.threeglav.bauk.util.BaukUtil;

public class FeedFilesHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int feedProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;
	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int feedFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS,
			SystemConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);

	public FeedFilesHandler(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		this.validate();
		if (this.factFeed.getThreadPoolSizes() != null) {
			feedProcessingThreads = factFeed.getThreadPoolSizes().getFeedProcessingThreads();
		}
		log.debug("Will use {} threads to process incoming files for {}", feedProcessingThreads, factFeed.getName());
		moveToErrorFileProcessor = new MoveFileErrorHandler(config.getErrorDirectory());
	}

	private void validate() {
		if (factFeed.getFileNameMasks() == null || factFeed.getFileNameMasks().isEmpty()) {
			throw new IllegalArgumentException("Could not find any file masks for " + factFeed.getName() + ". Check your configuration!");
		}
	}

	private void createSingleFileHandler(final int routeId, final ExecutorService exec, final String fullFileMask) {
		final FeedFileProcessor bfp = new FeedFileProcessor(factFeed, config, fullFileMask);
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, routeId, feedProcessingThreads,
				feedFileAcceptanceTimeoutMillis);
		final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
				SystemConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);
		Runnable ffh;
		if (throughputTestingMode) {
			BaukUtil.logEngineMessageSync("ENGINE IS RUNNING IN THROUGHPUT TESTING MODE! ONLY ONE FILE WILL BE CACHED AND PROCESSED!!!");
			ffh = new ThroughputTestingFileFindingHandler(config.getSourceDirectory(), bfp, fileFilter, moveToErrorFileProcessor);
		} else {
			ffh = new FileFindingHandler(config.getSourceDirectory(), bfp, fileFilter, moveToErrorFileProcessor);
		}
		exec.execute(ffh);
	}

	public void startHandlingFiles() {
		if (feedProcessingThreads > 0) {
			final ExecutorService exec = Executors.newFixedThreadPool(feedProcessingThreads, new BaukThreadFactory("feedHandlingThreadGroup",
					"feed-processing-" + factFeed.getName()));
			for (final String fileMask : factFeed.getFileNameMasks()) {
				log.debug("Creating {} processing threads for {}", feedProcessingThreads, fileMask);
				for (int i = 0; i < feedProcessingThreads; i++) {
					this.createSingleFileHandler(i, exec, fileMask);
					log.debug("Created processing thread #{} for {}", i, fileMask);
				}
				log.debug("Created in total {} threads for processing {}", feedProcessingThreads, fileMask);
			}
		}
	}
}
