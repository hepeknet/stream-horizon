package com.threeglav.sh.bauk.files;

import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;

import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.FileUtil;

/**
 * Caches first find file and then uses it repeatedly - not touching disk. Good for testing throughput
 * 
 * @author Threeglav
 * 
 */
public class ThroughputTestingFileFindingHandler extends FileFindingHandler {

	private BaukFile cachedFile;

	public ThroughputTestingFileFindingHandler(final String pathTofolder, final InputFeedProcessor fileProcessor, final Filter<Path> fileFilter,
			final FileProcessingErrorHandler errorHandler) {
		super(pathTofolder, fileProcessor, fileFilter, errorHandler);
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
				if (cachedFile == null && queuedFiles.isEmpty()) {
					if (isDebugEnabled) {
						log.debug("Queue empty. Will try to find new files in {}", pathTofolder);
					}
					this.findAllMatchingFilesInFolder(queuedFiles);
				}
				if (cachedFile != null) {
					this.sendCachedFile();
					continue;
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
					final Path path = queuedFiles.poll();
					if (path != null) {
						this.cacheFile(path);
						final boolean checkShouldStop = BaukUtil.shutdownStarted();
						if (checkShouldStop) {
							log.debug("Stopping processing files because shutdown signal was caught!");
							return;
						}
						EngineRegistry.pauseProcessingIfNeeded();
					}
				}
			} catch (final Exception exc) {
				log.error("Exception while processing files", exc);
			}
		}
	}

	private void sendCachedFile() {
		try {
			fileProcessor.process(cachedFile);
		} catch (final Exception exc) {
			errorHandler.handleError(cachedFile.getPath(), exc);
			emailSender.sendProcessingErrorEmail(exc.getMessage());
		}
	}

	private void cacheFile(final Path path) {
		try {
			final BasicFileAttributes bfa = Files.readAttributes(path, BasicFileAttributes.class);
			cachedFile = new CachedBaukFile(FileUtil.createBaukFile(path, bfa));
			log.warn("Cached {} for throughput testing! Will not consider other files in the folder!", cachedFile.getFullFilePath());
		} catch (final Exception exc) {
			errorHandler.handleError(path, exc);
			emailSender.sendProcessingErrorEmail(exc.getMessage());
		}
	}

}
