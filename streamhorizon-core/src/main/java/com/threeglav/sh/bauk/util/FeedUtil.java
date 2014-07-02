package com.threeglav.sh.bauk.util;

import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedTarget;

public abstract class FeedUtil {

	public static final String getBulkOutputFileNameOnly(final Feed f, final String inputFeedFileName) {
		if (f == null) {
			throw new IllegalArgumentException("Feed must not be null");
		}
		if (StringUtil.isEmpty(inputFeedFileName)) {
			throw new IllegalArgumentException("Input file name must not be null");
		}
		String bulkOutFileName = null;
		if (f.isFileTarget()) {
			final String targetExtension = getBulkOutputFileExtension(f);
			if (!StringUtil.isEmpty(targetExtension)) {
				bulkOutFileName = f.getName() + "_" + StringUtil.getFileNameWithoutExtension(inputFeedFileName) + "." + targetExtension;
			}
		}
		return bulkOutFileName;
	}

	public static final String getBulkOutputFileExtension(final Feed f) {
		if (f.isFileTarget()) {
			final String targetExtension = BaukPropertyUtil.getUniquePropertyIfExistsOrDefault(f.getTarget().getProperties(),
					FeedTarget.FILE_TARGET_EXTENSION_PROP_NAME, FeedTarget.FILE_TARGET_EXTENSION_DEFAULT_VALUE);
			return targetExtension;
		}
		return null;
	}

	public static final String getBulkOutputFileFullPath(final Feed f, final String inputFeedFileName) {
		if (f == null) {
			throw new IllegalArgumentException("Feed must not be null");
		}
		if (StringUtil.isEmpty(inputFeedFileName)) {
			throw new IllegalArgumentException("Input file name must not be null");
		}
		final String bulkOutFileName = getBulkOutputFileNameOnly(f, inputFeedFileName);
		if (StringUtil.isEmpty(bulkOutFileName)) {
			return null;
		}
		String bulkOutFilePath = null;
		if (f.isFileTarget()) {
			final String targetDirectory = getConfiguredBulkOutputDirectory(f);
			bulkOutFilePath = targetDirectory + "/" + bulkOutFileName;
		}
		return bulkOutFilePath;
	}

	private static final String getBulkOutputDirectoryIfSet(final Feed f) {
		if (f == null) {
			throw new IllegalArgumentException("Feed must not be null");
		}
		if (f.isFileTarget()) {
			final String targetDirectory = BaukPropertyUtil.getRequiredUniqueProperty(f.getTarget().getProperties(),
					FeedTarget.FILE_TARGET_DIRECTORY_PROP_NAME).getValue();
			return targetDirectory;
		}
		return null;
	}

	public static final String getConfiguredBulkOutputDirectory(final Feed f) {
		return ConfigurationProperties.getSystemProperty(FeedTarget.FILE_TARGET_DIRECTORY_PROP_NAME, FeedUtil.getBulkOutputDirectoryIfSet(f));
	}

}
