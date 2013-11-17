package com.threeglav.bauk.main;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.util.StringUtil;

public class ConfigurationValidator {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Config config;

	public ConfigurationValidator(final Config config) {
		this.config = config;
	}

	public void validate() throws Exception {
		final boolean sourceOk = getOrCreateDirectory(config.getSourceDirectory(), false);
		if (!sourceOk) {
			throw new IllegalStateException("Was not able to find folder where input feeds will be stored! Aborting!");
		}
		final boolean archiveOk = getOrCreateDirectory(config.getArchiveDirectory(), true);
		if (!archiveOk) {
			log.warn("Was not able to find directory for storing archives. This feature will be disabled!");
		}
		final boolean errorOk = getOrCreateDirectory(config.getErrorDirectory(), true);
		if (!errorOk) {
			throw new IllegalStateException("Was not able to find folder for storing corrupted/invalid data! Aborting!");
		}
		final boolean bulkOutOk = getOrCreateDirectory(config.getBulkOutputDirectory(), true);
		if (!bulkOutOk) {
			throw new IllegalStateException("Was not able to find folder for storing bulk output data! Aborting!");
		}
		for (final FactFeed ff : config.getFactFeeds()) {
			validateFactFeed(ff);
		}
	}

	private void validateFactFeed(final FactFeed ff) {
		if (!StringUtil.isEmpty(ff.getData().getEachLineStartsWithCharacter())) {
			log.warn(
					"Configuration for feed {} requires every data line to start with [{}]. This is mandatory for correct data interpretation!",
					ff.getName(), ff.getData().getEachLineStartsWithCharacter());
		}
		if (ff.getType() == FactFeedType.REPETITIVE && ff.getRepetitionCount() <= 0) {
			throw new IllegalStateException("Feed " + ff.getName() + " is marked as " + FactFeedType.REPETITIVE
					+ " but repetition count is not positive integer value!");
		}
	}

	private boolean getOrCreateDirectory(final String directory, final boolean shouldWrite) throws Exception {
		if (StringUtil.isEmpty(directory)) {
			return false;
		}
		log.debug("Checking if [{}] is valid", directory);
		final File dir = new File(directory);
		if (dir.exists() && dir.canRead() && dir.isDirectory()) {
			if (shouldWrite) {
				return dir.canWrite();
			} else {
				return true;
			}
		} else {
			log.warn("Was not able to find {}. Will try to create it!", directory);
			return dir.mkdirs();
		}
	}

}
