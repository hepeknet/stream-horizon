package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.StringUtil;

public class FileBulkOutputWriter extends AbstractBulkOutputWriter {

	private BufferedWriter writer;

	public FileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			finalBulkOutputFilePath = outputFilePath;
			temporaryBulkOutputFilePath = finalBulkOutputFilePath + TEMPORARY_FILE_EXTENSION;
			if (isDebugEnabled) {
				log.debug("Creating writer to [{}]", temporaryBulkOutputFilePath);
			}
			writer = new BufferedWriter(new FileWriter(temporaryBulkOutputFilePath), bufferSize);
			if (isDebugEnabled) {
				log.debug("Successfully created writer to [{}]", temporaryBulkOutputFilePath);
			}
		} catch (final Exception exc) {
			log.error("Exception while creating writer", exc);
			throw new RuntimeException(exc);
		}
	}

	@Override
	public void initialize(final String outputFilePath) {
		if (StringUtil.isEmpty(outputFilePath)) {
			throw new IllegalArgumentException("input file must not be null or empty");
		}
		if (writer != null) {
			throw new IllegalArgumentException("Writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		this.createFileWriter(outputFilePath);
	}

	@Override
	public void doOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			final String str = this.concatenateAllValues(resolvedData);
			writer.write(str);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes, final boolean success) {
		IOUtils.closeQuietly(writer);
		writer = null;
		if (success) {
			this.renameTemporaryBulkOutputFile();
			this.renameOutputFile(finalBulkOutputFilePath, globalAttributes);
		} else {
			this.deleteTemporaryBulkOutputFile();
		}
		finalBulkOutputFilePath = null;
		temporaryBulkOutputFilePath = null;
	}

}
