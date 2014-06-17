package com.threeglav.sh.bauk.files.feed;

import java.util.ArrayList;
import java.util.Collection;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.files.FileAttributesHashedNameFilter;
import com.threeglav.sh.bauk.files.FileFindingHandler;
import com.threeglav.sh.bauk.files.FileProcessingErrorHandler;
import com.threeglav.sh.bauk.files.MoveFileErrorHandler;
import com.threeglav.sh.bauk.files.ThroughputTestingFileFindingHandler;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BaukProperty;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedSource;
import com.threeglav.sh.bauk.util.BaukPropertyUtil;
import com.threeglav.sh.bauk.util.BaukUtil;

public final class FeedFilesHandler extends AbstractFeedHandler {

	private final FileProcessingErrorHandler moveToErrorFileProcessor;
	private final int feedFileAcceptanceTimeoutMillis = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_OLDER_THAN_MILLIS_PARAM_NAME,
			BaukEngineConfigurationConstants.FEED_FILE_ACCEPTANCE_TIMEOUT_MILLIS_DEFAULT);

	private static final boolean throughputTestingMode = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.THROUGHPUT_TESTING_MODE_PARAM_NAME, false);

	public FeedFilesHandler(final Feed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		this.validate();
		final String errorDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.ERROR_DIRECTORY_PARAM_NAME,
				factFeed.getErrorDirectory());
		moveToErrorFileProcessor = new MoveFileErrorHandler(errorDirectory);

	}

	private void validate() {
		final Collection<String> fileNameMasks = BaukPropertyUtil.getAllPropertyValuesByName(factFeed.getSource().getProperties(),
				FeedSource.FILE_FEED_SOURCE_FILE_NAME_MASK_PROPERTY_NAME);
		if (fileNameMasks == null || fileNameMasks.isEmpty()) {
			throw new IllegalArgumentException("Could not find any file masks for " + factFeed.getName() + ". Check your configuration!");
		}
	}

	private void createSingleFileHandler(final int processingThreadId, final String fullFileMask) {
		final FeedFileProcessor feedFileProc = new FeedFileProcessor(factFeed, config, fullFileMask);
		final FileAttributesHashedNameFilter fileFilter = new FileAttributesHashedNameFilter(fullFileMask, processingThreadId, feedProcessingThreads,
				feedFileAcceptanceTimeoutMillis);
		Runnable ffh;
		final String configuredSourceDirectory = this.getConfiguredSourceFolder();
		final String sourceDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME,
				configuredSourceDirectory);
		if (throughputTestingMode) {
			ffh = new ThroughputTestingFileFindingHandler(sourceDirectory, feedFileProc, fileFilter, moveToErrorFileProcessor);
		} else {
			ffh = new FileFindingHandler(sourceDirectory, feedFileProc, fileFilter, moveToErrorFileProcessor);
		}
		runnables.add(ffh);
	}

	private String getConfiguredSourceFolder() {
		final ArrayList<BaukProperty> properties = factFeed.getSource().getProperties();
		final String configuredSourceDirectory = BaukPropertyUtil.getRequiredUniqueProperty(properties,
				FeedSource.FILE_FEED_SOURCE_DIRECTORY_PATH_PROPERTY_NAME).getValue();
		return configuredSourceDirectory;
	}

	@Override
	public void init() {
		if (feedProcessingThreads > 0) {
			final String sourceDirectory = ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.SOURCE_DIRECTORY_PARAM_NAME,
					this.getConfiguredSourceFolder());
			final Collection<String> fileNameMasks = BaukPropertyUtil.getAllPropertyValuesByName(factFeed.getSource().getProperties(),
					FeedSource.FILE_FEED_SOURCE_FILE_NAME_MASK_PROPERTY_NAME);
			for (final String fileMask : fileNameMasks) {
				log.debug("Creating {} processing threads for {}", feedProcessingThreads, fileMask);
				for (int i = 0; i < feedProcessingThreads; i++) {
					this.createSingleFileHandler(i, fileMask);
					log.debug("Created processing thread #{} for {}", i, fileMask);
				}
				log.debug("Created in total {} threads for processing {}", feedProcessingThreads, fileMask);
				BaukUtil.logEngineMessage("Created " + feedProcessingThreads + " threads to watch for new feed files in [" + sourceDirectory
						+ "] directory, matching [" + fileMask + "] pattern");
			}
		}
	}
}
