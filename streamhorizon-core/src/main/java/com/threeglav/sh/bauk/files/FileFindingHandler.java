package com.threeglav.sh.bauk.files;

import java.io.File;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.EmailSender;
import com.threeglav.sh.bauk.util.FileUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class FileFindingHandler implements Runnable {

	private static final int MAX_FILES_PER_FOLDER_POLL = 100;

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	protected final String pathTofolder;
	protected final int pollingDelayMillis = ConfigurationProperties
			.getSystemProperty(BaukEngineConfigurationConstants.FILE_POLLING_DELAY_MILLIS_PARAM_NAME,
					BaukEngineConfigurationConstants.FILE_POLLING_DELAY_MILLIS_DEFAULT);
	protected final InputFeedProcessor fileProcessor;
	private final DirectoryStream.Filter<Path> fileFilter;
	protected final FileProcessingErrorHandler errorHandler;
	private final Path folderToPollPath;
	protected final boolean isDebugEnabled;
	protected final EmailSender emailSender;

	public FileFindingHandler(final String pathTofolder, final InputFeedProcessor fileProcessor, final DirectoryStream.Filter<Path> fileFilter,
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
						final boolean checkShouldStop = BaukUtil.shutdownStarted();
						if (checkShouldStop) {
							log.debug("Stopping processing files because shutdown signal was caught!");
							return;
						}
						EngineRegistry.pauseProcessingIfNeeded();
						path = queuedFiles.poll();
					}
				}
			} catch (final Exception exc) {
				log.error("Exception while processing files", exc);
			}
		}
	}

	protected void findAllMatchingFilesInFolder(final LinkedList<Path> queuedFiles) {
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
		} catch (final DirectoryIteratorException die) {
			log.warn("Problem while iterating through files. Probably other thread already processed one of the existing files. Details: {}",
					die.getMessage());
		} catch (final Exception e) {
			log.error("Problem while traversing folder looking for matching files", e);
		}
	}

	private void processSingleFile(final Path path) {
		try {
			final boolean fileExists = path.toFile().exists();
			if (!fileExists) {
				log.warn("File {} does not exists. Nothing to do here. Already picked up by another process", path);
				return;
			}
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
