package com.threeglav.bauk.feed;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileWriter extends ConfigAware {

	private BufferedWriter writer;

	public BulkFileWriter(final FactFeed factFeed, final Config config) {
		super(factFeed, config);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			this.log.debug("Creating writer to [{}]", outputFilePath);
			this.writer = new BufferedWriter(new FileWriter(outputFilePath), TextFileReaderComponent.DEFAULT_BUFFER_SIZE);
			this.log.debug("Successfully created writer to [{}]", outputFilePath);
		} catch (final Exception exc) {
			this.log.error("Exception while creating writer", exc);
			throw new RuntimeException(exc);
		}
	}

	public void startWriting(final String outputFilePath) {
		if (StringUtil.isEmpty(outputFilePath)) {
			throw new IllegalArgumentException("input file must not be null or empty");
		}
		if (this.writer != null) {
			throw new IllegalArgumentException("Writer is not null!");
		}
		this.createFileWriter(outputFilePath);
	}

	public void write(final String line) {
		try {
			this.writer.write(line);
		} catch (final Exception exc) {
			this.log.error("Exception while writing data", exc);
		}
	}

	public void closeResources() {
		IOUtils.closeQuietly(this.writer);
		this.writer = null;
	}

}
