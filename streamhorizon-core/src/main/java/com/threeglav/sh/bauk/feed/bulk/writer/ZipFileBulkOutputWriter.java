package com.threeglav.sh.bauk.feed.bulk.writer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.StringUtil;

public class ZipFileBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private ZipOutputStream zipOutStream;

	public ZipFileBulkOutputWriter(final Feed factFeed, final BaukConfiguration config) {
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
			zipOutStream = new ZipOutputStream(new BufferedOutputStream(fos, bufferSize));
			final ZipEntry ze = new ZipEntry("inputFeedFile");
			zipOutStream.putNextEntry(ze);
			if (isDebugEnabled) {
				log.debug("Successfully created writer to [{}]", outputFilePath);
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
		if (zipOutStream != null) {
			throw new IllegalArgumentException("Zip writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		String finalOutputFilePath = outputFilePath;
		if (!outputFilePath.toLowerCase().endsWith(".zip")) {
			if (isDebugEnabled) {
				log.debug("Will add .zip extension to bulk output file {}!", outputFilePath);
			}
			finalOutputFilePath += ".zip";
		}
		this.createFileWriter(finalOutputFilePath);
	}

	@Override
	public void doWriteOutput(final Object[] resolvedData, final Map<String, String> globalAttributes) {
		try {
			final String dataStr = this.concatenateAllValues(resolvedData);
			final byte[] dataBytes = dataStr.getBytes(UTF_8_CHARSET);
			zipOutStream.write(dataBytes);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResourcesAfterWriting(final Map<String, String> globalAttributes, final boolean success) {
		IOUtils.closeQuietly(zipOutStream);
		zipOutStream = null;
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
