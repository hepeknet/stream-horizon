package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;

public class NamedPipeBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final String DEFAULT_PIPE_DIRECTORY = "/var/streamhorizon";

	private BufferedWriter writer;
	private String threadName;

	public NamedPipeBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);

	}

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			final String str = this.concatenateAllValues(resolvedData);
			writer.write(str);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
			this.cleanup();
			throw new IllegalStateException(exc);
		}
	}

	private void cleanup() {
		IOUtils.closeQuietly(writer);
		writer = null;
		threadName = null;
	}

	@Override
	public void startWriting(final Map<String, String> globalAttributes) {
		if (threadName == null) {
			threadName = globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_FEED_PROCESSOR_ID);
			final String fileName = DEFAULT_PIPE_DIRECTORY + "/bulkFile" + threadName + ".pipe";
			final File f = new File(fileName);
			if (!f.exists()) {
				throw new IllegalArgumentException("Unable to find named pipe [" + fileName + "]. This has to be created before starting engine!");
			}
			try {
				writer = new BufferedWriter(new FileWriter(f));
				log.debug("Successfully created writer to named pipe {}", fileName);
			} catch (final IOException e) {
				log.error("Error while creating writer to named pipe " + fileName + ".", e);
				throw new IllegalStateException(e);
			}
		}
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
		try {
			writer.flush();
		} catch (final IOException e) {
			log.error("Exception while flushing writer to pipe", e);
			this.cleanup();
			throw new IllegalStateException(e);
		}
	}

}
