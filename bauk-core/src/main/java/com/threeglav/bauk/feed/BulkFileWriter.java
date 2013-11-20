package com.threeglav.bauk.feed;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileWriter extends ConfigAware {

	private BufferedWriter writer;
	private final int bufferSize;

	public BulkFileWriter(final FactFeed factFeed, final Config config) {
		super(factFeed, config);
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * Constants.ONE_MEGABYTE;
		log.debug("Write buffer size is {}", bufferSize);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			log.debug("Creating writer to [{}]", outputFilePath);
			writer = new BufferedWriter(new FileWriter(outputFilePath), bufferSize);
			log.debug("Successfully created writer to [{}]", outputFilePath);
		} catch (final Exception exc) {
			log.error("Exception while creating writer", exc);
			throw new RuntimeException(exc);
		}
	}

	public void startWriting(final String outputFilePath) {
		if (StringUtil.isEmpty(outputFilePath)) {
			throw new IllegalArgumentException("input file must not be null or empty");
		}
		if (writer != null) {
			throw new IllegalArgumentException("Writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		this.createFileWriter(outputFilePath);
	}

	public void write(final String line) {
		try {
			writer.write(line);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	public void closeResources() {
		IOUtils.closeQuietly(writer);
		writer = null;
	}

}
