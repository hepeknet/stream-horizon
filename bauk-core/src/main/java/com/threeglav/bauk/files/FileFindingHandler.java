package com.threeglav.bauk.files;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.StringUtil;

public class FileFindingHandler implements Runnable {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String pathTofolder;
	private final int pollingDelayMillis = 1000;
	private final FileProcessor fileProcessor;
	private final FileFilter fileFilter;
	private final FileProcessingErrorHandler errorHandler;
	private final File folderToPoll;
	private final boolean isDebugEnabled;

	public FileFindingHandler(final String pathTofolder, final FileProcessor fileProcessor, final FileFilter fileFilter,
			final FileProcessingErrorHandler errorHandler) {
		if (StringUtil.isEmpty(pathTofolder)) {
			throw new IllegalArgumentException("Path to folder must not be null or empty");
		}
		if (fileProcessor == null) {
			throw new IllegalArgumentException("File processor must not be null");
		}
		if (fileFilter == null) {
			throw new IllegalArgumentException("File filter must not be null");
		}
		if (errorHandler == null) {
			throw new IllegalArgumentException("Error handler must not be null");
		}
		this.pathTofolder = pathTofolder;
		this.fileProcessor = fileProcessor;
		this.fileFilter = fileFilter;
		this.errorHandler = errorHandler;
		folderToPoll = new File(pathTofolder);
		if (!folderToPoll.isDirectory() || !folderToPoll.exists()) {
			throw new IllegalStateException("Unable to find existing folder " + pathTofolder);
		}
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public void run() {
		final LinkedList<File> queuedFiles = new LinkedList<>();
		while (true) {
			final boolean shouldStop = BaukUtil.shutdownStarted();
			if (shouldStop) {
				log.debug("Stopping polling for files");
				break;
			}
			if (queuedFiles.isEmpty()) {
				if (isDebugEnabled) {
					log.debug("Queue empty. Will try to find new files in {}", pathTofolder);
				}
				final File[] files = folderToPoll.listFiles(fileFilter);
				if (files != null) {
					for (final File f : files) {
						queuedFiles.add(f);
					}
					if (isDebugEnabled) {
						log.debug("Queued in total {} files", queuedFiles.size());
					}
				}
			}
			if (queuedFiles.isEmpty()) {
				if (isDebugEnabled) {
					log.debug("No files found! Will sleep for {}ms", pollingDelayMillis);
				}
				try {
					Thread.sleep(pollingDelayMillis);
					continue;
				} catch (final InterruptedException e) {
					log.error("Exception while sleeping", e);
				}
			} else {
				final File f = queuedFiles.poll();
				if (f != null) {
					try {
						final BasicFileAttributes bfa = Files.readAttributes(f.toPath(), BasicFileAttributes.class);
						fileProcessor.process(f, bfa);
						if (isDebugEnabled) {
							log.debug("Successfully processed {}", f.getName());
						}
					} catch (final Exception exc) {
						try {
							errorHandler.handleError(f, exc);
						} catch (final IOException ie) {
							log.error(
									"Exiting application! Was not able to handle error in processing file {}! Was file changed while being processed? {}",
									f.getAbsolutePath(), ie.getMessage());
							log.error("Exception ", ie);
							System.exit(-1);
						}
					}
				}
			}
		}
	}

}
