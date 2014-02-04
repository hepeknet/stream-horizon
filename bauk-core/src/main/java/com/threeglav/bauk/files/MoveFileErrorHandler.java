package com.threeglav.bauk.files;

import java.io.File;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.FileUtil;
import com.threeglav.bauk.util.StringUtil;

public class MoveFileErrorHandler implements FileProcessingErrorHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Path targetFolderPath;
	private final boolean isDebugEnabled;

	public MoveFileErrorHandler(final String targetFolderName) {
		if (StringUtil.isEmpty(targetFolderName)) {
			throw new IllegalArgumentException("Target folder must not be null or empty!");
		}
		final File targetDir = new File(targetFolderName);
		if (!targetDir.isDirectory() || !targetDir.exists()) {
			throw new IllegalArgumentException(targetFolderName + " is not a readable directory!");
		}
		targetFolderPath = targetDir.toPath();
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public void handleError(final Path path, final Exception exc) {
		Throwable cause = exc;
		while (cause != null) {
			log.error("Caught exception while processing file {}. Triggering error handling!", path.toString());
			log.error("Exception ", cause);
			cause = cause.getCause();
		}
		final Path destinationPath = targetFolderPath.resolve(path.getFileName());
		final long start = System.currentTimeMillis();
		FileUtil.moveFile(path, destinationPath);
		final long total = System.currentTimeMillis() - start;
		if (isDebugEnabled) {
			log.debug("Moved {} to {}. In total took {}ms to move this file", path, targetFolderPath, total);
		}
	}

}
