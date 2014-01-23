package com.threeglav.bauk.files;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.EmailSender;
import com.threeglav.bauk.util.FileUtil;
import com.threeglav.bauk.util.StringUtil;

public final class FileFindingHandler implements Runnable {

	private static final int MAX_FILES_PER_FOLDER_POLL = 100;

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String pathTofolder;
	private final int pollingDelayMillis = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.FILE_POLLING_DELAY_MILLIS_PARAM_NAME, SystemConfigurationConstants.FILE_POLLING_DELAY_MILLIS_DEFAULT);
	private final FileProcessor fileProcessor;
	private final DirectoryStream.Filter<Path> fileFilter;
	private final FileProcessingErrorHandler errorHandler;
	private final Path folderToPollPath;
	private final boolean isDebugEnabled;
	private final EmailSender emailSender;

	public FileFindingHandler(final String pathTofolder, final FileProcessor fileProcessor, final DirectoryStream.Filter<Path> fileFilter,
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
		final File folderToPoll = new File(pathTofolder);
		folderToPollPath = folderToPoll.toPath();
		if (!folderToPoll.isDirectory() || !folderToPoll.exists()) {
			throw new IllegalStateException("Unable to find folder " + pathTofolder);
		}
		isDebugEnabled = log.isDebugEnabled();
		EngineRegistry.registerFeedProcessingThread();
		emailSender = new EmailSender();
	}

	@Override
	public void run() {
		final LinkedList<Path> queuedFiles = new LinkedList<>();
		while (true) {
			try {
				EngineRegistry.pauseProcessingIfNeeded();
				final boolean shouldStop = BaukUtil.shutdownStarted();
				if (shouldStop) {
					log.debug("Stopping polling for files because shutdown signal was caught!");
					break;
				}
				if (queuedFiles.isEmpty()) {
					if (isDebugEnabled) {
						log.debug("Queue empty. Will try to find new files in {}", pathTofolder);
					}
					this.findAllMatchingFilesInFolder(queuedFiles);
				}
				if (queuedFiles.isEmpty()) {
					if (isDebugEnabled) {
						log.debug("No files found! Will sleep for {}ms", pollingDelayMillis);
					}
					try {
						Thread.sleep(pollingDelayMillis);
					} catch (final InterruptedException e) {
						log.error("Exception while sleeping", e);
					}
				} else {
					Path path = queuedFiles.poll();
					while (path != null) {
						this.processSingleFile(path);
						EngineRegistry.pauseProcessingIfNeeded();
						path = queuedFiles.poll();
					}
				}
			} catch (final Exception exc) {
				log.error("Exception while processing files", exc);
			}
		}
	}

	private void findAllMatchingFilesInFolder(final LinkedList<Path> queuedFiles) {
		try (DirectoryStream<Path> ds = Files.newDirectoryStream(folderToPollPath, fileFilter)) {
			for (final Path p : ds) {
				queuedFiles.add(p);
				if (queuedFiles.size() >= MAX_FILES_PER_FOLDER_POLL) {
					log.info("Gathered {} files. Stopping now since this is maximum allowed limit", queuedFiles.size());
					break;
				}
			}
			if (isDebugEnabled) {
				log.debug("Queued in total {} files", queuedFiles.size());
			}
		} catch (final Exception e) {
			log.error("Problem while traversing folder looking for matching files", e);
		}
	}

	private void processSingleFile(final Path path) {
		try {
			final BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
			final BaukFile baukFile = FileUtil.createBaukFile(path, bfa);
			fileProcessor.process(baukFile);
			if (isDebugEnabled) {
				log.debug("Successfully processed {}", path);
			}
		} catch (final Exception exc) {
			errorHandler.handleError(path, exc);
			emailSender.sendProcessingErrorEmail(exc.getMessage());
		}
	}

}
