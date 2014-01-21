package com.threeglav.bauk.files;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.events.EngineEvents;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.FileUtil;
import com.threeglav.bauk.util.StringUtil;

public final class FileFindingHandler implements Runnable, Observer {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String pathTofolder;
	private final int pollingDelayMillis = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.FILE_POLLING_DELAY_MILLIS_PARAM_NAME, SystemConfigurationConstants.FILE_POLLING_DELAY_MILLIS_DEFAULT);
	private final FileProcessor fileProcessor;
	private final DirectoryStream.Filter<Path> fileFilter;
	private final FileProcessingErrorHandler errorHandler;
	private final Path folderToPollPath;
	private final boolean isDebugEnabled;
	private boolean isProcessingPaused = false;

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
		EngineEvents.registerForProcessingEvents(this);
	}

	@Override
	public void run() {
		final LinkedList<Path> queuedFiles = new LinkedList<>();

		while (true) {
			try {
				final boolean shouldStop = BaukUtil.shutdownStarted();
				this.waitUntilProcessingAllowed();
				if (shouldStop) {
					log.debug("Stopping polling for files");
					break;
				}
				if (queuedFiles.isEmpty()) {
					if (isDebugEnabled) {
						log.debug("Queue empty. Will try to find new files in {}", pathTofolder);
					}
					this.findAllFilesInFolder(queuedFiles);
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
						if (isProcessingPaused) {
							// skip to start and wait before starting to process next file
							break;
						}
						path = queuedFiles.poll();
					}
				}
			} catch (final Exception exc) {
				log.error("Exception while processing files", exc);
			}
		}
	}

	private void findAllFilesInFolder(final LinkedList<Path> queuedFiles) {
		try (DirectoryStream<Path> ds = Files.newDirectoryStream(folderToPollPath, fileFilter)) {
			for (final Path p : ds) {
				queuedFiles.add(p);
			}
			if (isDebugEnabled) {
				log.debug("Queued in total {} files", queuedFiles.size());
			}
		} catch (final Exception e) {
			log.error("Problem while traversing folder", e);
		}
	}

	private void processSingleFile(final Path path) {
		try {
			final BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
			final BaukFile baukFile = FileUtil.createBaukFile(path, bfa);
			EngineRegistry.reportJobInProgress();
			fileProcessor.process(baukFile);
			if (isDebugEnabled) {
				log.debug("Successfully processed {}", path);
			}
		} catch (final Exception exc) {
			errorHandler.handleError(path, exc);
		} finally {
			EngineRegistry.reportFinishedJob();
		}
	}

	private void waitUntilProcessingAllowed() {
		while (isProcessingPaused) {
			try {
				log.info("Processing paused.. will wait before accepting new tasks");
				Thread.sleep(1000);
			} catch (final InterruptedException e) {
				//
			}
		}
		log.debug("Allowed to continue processing...");
	}

	@Override
	public void update(final Observable o, final Object arg) {
		log.info("Received event {}", arg);
		if (EngineEvents.PROCESSING_EVENT.PAUSE.equals(arg)) {
			log.info("Pausing processing...");
			isProcessingPaused = true;
		} else if (EngineEvents.PROCESSING_EVENT.CONTINUE.equals(arg)) {
			log.info("Continuing processing...");
			isProcessingPaused = false;
		}
	}

}
