package com.threeglav.bauk.files.bulk;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	}

	private void validate() {
		if (factFeed.getBulkLoadDefinition() == null && bulkProcessingThreads > 0) {
			throw new IllegalStateException(
					"Was not able to find bulk definition in configuration file but bulk processing threads set to positive value!");
		}
	}

	public void startHandlingFiles() {
		if (bulkProcessingThreads > 0) {
			final ExecutorService exec = Executors.newFixedThreadPool(bulkProcessingThreads, new BaukThreadFactory("bulkProcessingThreads",
					"bulk-processing"));
			for (int i = 0; i < bulkProcessingThreads; i++) {
				this.createSingleFileHandler(i, exec);
			}
			log.debug("Created in total {} bulk processing routes", bulkProcessingThreads);
		}
	}

	private void createSingleFileHandler(final int routeId, final ExecutorService exec) {
		final BulkFileProcessor bfp = new BulkFileProcessor(factFeed, config);
		final String fullFileMask = ".*" + factFeed.getBulkLoadDefinition().getBulkLoadOutputExtension();
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, routeId, bulkProcessingThreads);
		final FileFindingHandler ffh = new FileFindingHandler(config.getBulkOutputDirectory(), bfp, fileFilter, moveToErrorFileProcessor);
		exec.submit(ffh);
	}
}
