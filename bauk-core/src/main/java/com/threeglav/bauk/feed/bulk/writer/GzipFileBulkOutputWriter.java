package com.threeglav.bauk.feed.bulk.writer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class GzipFileBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private GZIPOutputStream gzipOutStream;

	public GzipFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			finalBulkOutputFilePath = outputFilePath;
			temporaryBulkOutputFilePath = finalBulkOutputFilePath + TEMPORARY_FILE_EXTENSION;
			if (isDebugEnabled) {
				log.debug("Creating zip writer to [{}]", temporaryBulkOutputFilePath);
			}
			final FileOutputStream fos = new FileOutputStream(temporaryBulkOutputFilePath);
			gzipOutStream = new CustomGzip(fos, bufferSize);
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
		if (gzipOutStream != null) {
			throw new IllegalArgumentException("Gzip writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		String finalOutputFilePath = outputFilePath;
		if (!outputFilePath.toLowerCase().endsWith(".gz")) {
			if (isDebugEnabled) {
				log.debug("Will add .gz extension to bulk output file {}!", outputFilePath);
			}
			finalOutputFilePath += ".gz";
		}
		this.createFileWriter(finalOutputFilePath);
	}

	@Override
	public void doOutput(final Object[] resolvedData) {
		try {
			final String dataStr = this.concatenateAllValues(resolvedData);
			final byte[] dataBytes = dataStr.getBytes(UTF_8_CHARSET);
			gzipOutStream.write(dataBytes);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes, final boolean success) {
		IOUtils.closeQuietly(gzipOutStream);
		gzipOutStream = null;
		if (success) {
			this.renameTemporaryBulkOutputFile();
			this.renameOutputFile(finalBulkOutputFilePath, globalAttributes);
		} else {
			this.deleteTemporaryBulkOutputFile();
		}
		finalBulkOutputFilePath = null;
		temporaryBulkOutputFilePath = null;
	}

	static class CustomGzip extends GZIPOutputStream {

		public CustomGzip(final OutputStream out, final int size) throws IOException {
			super(out, size);
			def.setLevel(Deflater.BEST_SPEED);
		}

	}

}
