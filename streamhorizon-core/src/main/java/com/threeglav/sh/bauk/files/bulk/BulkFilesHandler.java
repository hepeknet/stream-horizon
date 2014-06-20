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
import com.threeglav.sh.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedTarget;
import com.threeglav.sh.bauk.model.ThreadPoolSettings;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;
import com.threeglav.sh.bauk.util.BaukThreadFactory;
import com.threeglav.sh.bauk.util.FeedUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class BulkFilesHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final Feed factFeed;
	private final BaukConfiguration config;
	private int bulkProcessingThreads = ThreadPoolSettings.THREAD_POOL_DEFAULT_SIZE;
	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int bulkFileAcceptanceTimeoutMillis;
	private final String bulkOutDirectory;
	private final String fileExtension;

	private final ExecutorService EXEC_SERVICE;

	private final List<Runnable> runnables = new LinkedList<>();

	public BulkFilesHandler(final Feed factFeed, final BaukConfiguration config) {
		this.factFeed = factFeed;
		this.config = config;
		if (factFeed.getThreadPoolSettings() != null) {
			bulkProcessingThreads = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.BULK_PROCESSING_THREADS_PARAM_NAME,
					factFeed.getThreadPoolSettings().getDatabaseProcessingThreadCount());
		}
		log.debug("Will use {} threads to process bulk load files for {}", bulkProcessingThreads, factFeed.getName());
		if (bulkProcessingThreads <= 0) {
			log.info("For feed {} bulk processing set to use non-positive number of threads. Will not be started!", factFeed.getName());
		} else {
			final boolean shouldBeEtlThreadsOnly = factFeed.isEtlOnlyFactFeed();
			if (shouldBeEtlThreadsOnly) {
				log.warn(
						"Fact feed {} should be running ETL threads only (because of declared outputType in configuration file) but number of DB threads is set to {}. Will not start DB threads!",
						factFeed.getName(), bulkProcessingThreads);
				bulkProcessingThreads = -1;
			}
		}
		bulkOutDirectory = FeedUtil.getConfiguredBulkOutputDirectory(factFeed);
		fileExtension = BaukPropertyUtil.getRequiredUniqueProperty(factFeed.getTarget().getProperties(), FeedTarget.FILE_TARGET_EXTENSION_PROP_NAME)
				.getValue();
		this.validate();
		final String errorDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ERROR_DIRECTORY_PARAM_NAME,
				factFeed.getErrorDirectory());
		moveToErrorFileProcessor = new MoveFileErrorHandler(errorDirectory);
		if (bulkProcessingThreads > 0) {
			EXEC_SERVICE = Executors.newFixedThreadPool(bulkProcessingThreads, new BaukThreadFactory("bulkProcessingThreads", "db-thread"));
		} else {
			EXEC_SERVICE = null;
		}
		bulkFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME,
				BaukEngineConfigurationConstants.BULK_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);
	}

	private void validate() {
		if (factFeed.getTargetFormatDefinition() == null && bulkProcessingThreads > 0) {
			throw new IllegalStateException(
					"Was not able to find bulk definition in configuration file but bulk processing threads set to positive value!");
		}
		if (StringUtil.isEmpty(bulkOutDirectory)) {
			throw new IllegalStateException("Bulk output directory must not be null or empty!");
		}
		if (fileExtension != null && !fileExtension.matches("[A-Za-z0-9]+")) {
			throw new IllegalStateException("Bulk file extension must contain only alpha-numerical characters. Currently it is set to ["
					+ fileExtension + "]");
		}
		final boolean isEmptyExtension = StringUtil.isEmpty(fileExtension);
		final String outputType = factFeed.getTarget().getType();
		if (isEmptyExtension
				&& (outputType.equalsIgnoreCase(BulkLoadDefinitionOutputType.FILE.toString())
						|| outputType.equalsIgnoreCase(BulkLoadDefinitionOutputType.ZIP.toString()) || outputType
							.equalsIgnoreCase(BulkLoadDefinitionOutputType.GZ.toString()))) {
			throw new IllegalStateException(
					"Extension for recognizing bulk output files is required to be specified in configuration file because file output will be generated. Problematic feed is ["
							+ factFeed.getName() + "]!");
		}
	}

	public void createFileHandlers() {
		if (bulkProcessingThreads > 0) {
			Executors.newFixedThreadPool(bulkProcessingThreads, new BaukThreadFactory("bulkProcessingThreads", "bulk-processing"));
			for (int i = 0; i < bulkProcessingThreads; i++) {
				this.createSingleFileHandler(i);
			}
			log.debug("Created in total {} bulk processing threads. Will watch folder [{}] for files with extension [{}]", bulkProcessingThreads,
					bulkOutDirectory, fileExtension);
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
		final String fullFileMask = ".*" + fileExtension;
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, routeId, bulkProcessingThreads,
				bulkFileAcceptanceTimeoutMillis);
		final FileFindingHandler ffh = new FileFindingHandler(bulkOutDirectory, bfp, fileFilter, moveToErrorFileProcessor);
		runnables.add(ffh);
	}
}
