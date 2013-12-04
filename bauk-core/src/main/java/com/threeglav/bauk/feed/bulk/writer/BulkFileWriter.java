package com.threeglav.bauk.feed.bulk.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class BulkFileWriter extends ConfigAware implements BulkOutputWriter {

	private BufferedWriter writer;
	private final int bufferSize;

	public BulkFileWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
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

	@Override
	public void startWriting(final String outputFilePath) {
		if (StringUtil.isEmpty(outputFilePath)) {
			throw new IllegalArgumentException("input file must not be null or empty");
		}
		if (writer != null) {
			throw new IllegalArgumentException("Writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		this.createFileWriter(outputFilePath);
	}

	@Override
	public void write(final String line) {
		try {
			writer.write(line);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources() {
		IOUtils.closeQuietly(writer);
		writer = null;
	}

}
