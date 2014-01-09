package com.threeglav.bauk.camel;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class MoveFileProcessor implements Processor {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Histogram moveFilesTimeTakenHistogram;
	private final Path targetFolderPath;

	public MoveFileProcessor(final String targetFolderName) {
		if (StringUtil.isEmpty(targetFolderName)) {
			throw new IllegalArgumentException("Target folder must not be null or empty!");
		}
		final File targetDir = new File(targetFolderName);
		if (!targetDir.isDirectory() || !targetDir.exists()) {
			throw new IllegalArgumentException(targetFolderName + " is not a readable directory!");
		}
		targetFolderPath = targetDir.toPath();
		moveFilesTimeTakenHistogram = MetricsUtil.createHistogram("Time taken (millis) to move files to " + targetDir.getName() + "");
	}

	@Override
	public void process(final Exchange exchange) throws Exception {
		final String originalFilePath = (String) exchange.getIn().getHeader("originalFilePath");
		if (StringUtil.isEmpty(originalFilePath)) {
			throw new IllegalArgumentException("Was not able to find file to be moved");
		}
		final File originalFile = new File(originalFilePath);
		if (originalFile.isFile() && originalFile.exists()) {
			final Path originalPath = originalFile.toPath();
			final long start = System.currentTimeMillis();
			Files.move(originalPath, targetFolderPath.resolve(originalPath.getFileName()), StandardCopyOption.REPLACE_EXISTING);
			final long total = System.currentTimeMillis() - start;
			if (log.isDebugEnabled()) {
				log.debug("Moved {} to {}. In total took {}ms to move this file", originalFilePath, targetFolderPath, total);
			}
			if (moveFilesTimeTakenHistogram != null) {
				moveFilesTimeTakenHistogram.update(total);
			}
		}
	}

}
