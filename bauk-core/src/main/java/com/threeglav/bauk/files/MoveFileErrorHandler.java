package com.threeglav.bauk.files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.threeglav.bauk.util.FileUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class MoveFileErrorHandler implements FileProcessingErrorHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Histogram moveFilesTimeTakenHistogram;
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
		moveFilesTimeTakenHistogram = MetricsUtil.createHistogram("Time taken (millis) to move files to " + targetDir.getName() + "");
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public void handleError(final File f, final Exception exc) throws IOException {
		final String originalFilePath = f.getAbsolutePath();
		if (exc != null) {
			log.error("Caught exception while processing file {}. Triggering error handling!", originalFilePath);
			log.error("Exception ", exc);
		}
		if (StringUtil.isEmpty(originalFilePath)) {
			throw new IllegalArgumentException("Was not able to find file to be moved");
		}
		final File originalFile = new File(originalFilePath);
		if (originalFile.isFile() && originalFile.exists()) {
			final Path originalPath = originalFile.toPath();
			final Path destinationPath = targetFolderPath.resolve(originalPath.getFileName());
			final long start = System.currentTimeMillis();
			FileUtil.moveFile(originalPath, destinationPath);
			final long total = System.currentTimeMillis() - start;
			if (isDebugEnabled) {
				log.debug("Moved {} to {}. In total took {}ms to move this file", originalFilePath, targetFolderPath, total);
			}
			if (moveFilesTimeTakenHistogram != null) {
				moveFilesTimeTakenHistogram.update(total);
			}
		}
	}

}
