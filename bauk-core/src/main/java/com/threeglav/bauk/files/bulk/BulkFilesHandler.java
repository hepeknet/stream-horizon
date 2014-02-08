package com.threeglav.bauk.files.bulk;

import java.util.LinkedList;
import java.util.List;
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
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.ThreadPoolSizes;
import com.threeglav.bauk.util.BaukThreadFactory;

public class BulkFilesHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final FactFeed factFeed;
	private final BaukConfiguration config;
	private int bulkProcessingThreads = ThreadPoolSizes.THREAD_POOL_DEFAULT_SIZE;
	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int bulkFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS,
			SystemConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);

	private final ExecutorService EXEC_SERVICE;

	private final List<Runnable> runnables = new LinkedList<>();

	public BulkFilesHandler(final FactFeed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		if (factFeed.getThreadPoolSizes() != null) {
			bulkProcessingThreads = factFeed.getThreadPoolSizes().getBulkLoadProcessingThreads();
		}
		log.debug("Will use {} threads to process bulk load files for {}", bulkProcessingThreads, factFeed.getName());
		if (bulkProcessingThreads <= 0) {
			log.info("For feed {} bulk processing set to use non-positive number of threads. Will not be started!", factFeed.getName());
		}
		this.validate();
		moveToErrorFileProcessor = new MoveFileErrorHandler(config.getErrorDirectory());
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
			log.debug("Created in total {} bulk processing routes", bulkProcessingThreads);
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
		}
	}

	private void createSingleFileHandler(final int routeId) {
		final BulkFileProcessor bfp = new BulkFileProcessor(factFeed, config);
		final String fullFileMask = ".*" + factFeed.getBulkLoadDefinition().getBulkLoadOutputExtension();
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, routeId, bulkProcessingThreads,
				bulkFileAcceptanceTimeoutMillis);
		final FileFindingHandler ffh = new FileFindingHandler(config.getBulkOutputDirectory(), bfp, fileFilter, moveToErrorFileProcessor);
		runnables.add(ffh);
	}
}
