package com.threeglav.sh.bauk.files.bulk;

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
import com.threeglav.sh.bauk.files.FileAttributesHashedNameFilter;
import com.threeglav.sh.bauk.files.FileFindingHandler;
import com.threeglav.sh.bauk.files.FileProcessingErrorHandler;
import com.threeglav.sh.bauk.files.MoveFileErrorHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.ThreadPoolSettings;
import com.threeglav.sh.bauk.util.BaukThreadFactory;

public class BulkFilesHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int bulkProcessingThreads = ThreadPoolSettings.THREAD_POOL_DEFAULT_SIZE;
	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int bulkFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME,
			BaukEngineConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);

	private final ExecutorService EXEC_SERVICE;

	private final List<Runnable> runnables = new LinkedList<>();

	public BulkFilesHandler(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		if (factFeed.getThreadPoolSettings() != null) {
			bulkProcessingThreads = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BULK_PROCESSING_THREADS_PARAM_NAME,
					factFeed.getThreadPoolSettings().getDatabaseProcessingThreadCount());
		}
		log.debug("Will use {} threads to process bulk load files for {}", bulkProcessingThreads, factFeed.getName());
		if (bulkProcessingThreads <= 0) {
			log.info("For feed {} bulk processing set to use non-positive number of threads. Will not be started!", factFeed.getName());
		}
		this.validate();
		final String errorDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ERROR_DIRECTORY_PARAM_NAME,
				config.getErrorDirectory());
		moveToErrorFileProcessor = new MoveFileErrorHandler(errorDirectory);
		if (bulkProcessingThreads > 0) {
			EXEC_SERVICE = Executors.newFixedThreadPool(bulkProcessingThreads, new BaukThreadFactory("bulkProcessingThreads", "bulk-processing"));
		} else {
			EXEC_SERVICE = null;
		}
	}

	private void validate() {
		if (factFeed.getBulkLoadDefinition() == null && bulkProcessingThreads > 0) {
			throw new IllegalStateException(
					"Was not able to find bulk definition in configuration file but bulk processing threads set to positive value!");
		}
	}

	public void createFileHandlers() {
		if (bulkProcessingThreads > 0) {
			Executors.newFixedThreadPool(bulkProcessingThreads, new BaukThreadFactory("bulkProcessingThreads", "bulk-processing"));
			for (int i = 0; i < bulkProcessingThreads; i++) {
				this.createSingleFileHandler(i);
			}
			log.debug("Created in total {} bulk processing threads", bulkProcessingThreads);
		}
	}

	public int start() {
		if (bulkProcessingThreads > 0) {
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
		if (bulkProcessingThreads > 0) {
			EXEC_SERVICE.shutdown();
			try {
				final boolean allDone = EXEC_SERVICE.awaitTermination(BaukConstants.WAIT_FOR_THREADS_TO_DIE_ON_SHUTDOWN_SECONDS, TimeUnit.SECONDS);
				if (!allDone) {
					log.warn("Not all bulk processing threads died within {} seconds after receiving shutdown signal. Will shutdown engine anyway!",
							BaukConstants.WAIT_FOR_THREADS_TO_DIE_ON_SHUTDOWN_SECONDS);
				}
			} catch (final InterruptedException e) {
				// ignore
			}
		}
	}

	private void createSingleFileHandler(final int routeId) {
		final BulkFileProcessor bfp = new BulkFileProcessor(factFeed, config);
		final String fullFileMask = ".*" + factFeed.getBulkLoadDefinition().getBulkLoadOutputExtension();
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, routeId, bulkProcessingThreads,
				bulkFileAcceptanceTimeoutMillis);
		final String bulkOutDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.OUTPUT_DIRECTORY_PARAM_NAME,
				config.getBulkOutputDirectory());
		final FileFindingHandler ffh = new FileFindingHandler(bulkOutDirectory, bfp, fileFilter, moveToErrorFileProcessor);
		runnables.add(ffh);
	}
}
