package com.threeglav.bauk.feed.bulk.writer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class ZipFileBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private ZipOutputStream zipOutStream;
	private String currentBulkOutputFilePath;
	private final int bufferSize;

	public ZipFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
		log.debug("Write buffer size is {}", bufferSize);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			currentBulkOutputFilePath = outputFilePath;
			if (isDebugEnabled) {
				log.debug("Creating zip writer to [{}]", outputFilePath);
			}
			final FileOutputStream fos = new FileOutputStream(outputFilePath);
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
	public void doOutput(final Object[] resolvedData) {
		try {
			final StringBuilder sb = this.concatenateAllValues(resolvedData);
			sb.append("\n");
			final String dataStr = sb.toString();
			final byte[] dataBytes = dataStr.getBytes(UTF_8_CHARSET);
			zipOutStream.write(dataBytes);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
		IOUtils.closeQuietly(zipOutStream);
		zipOutStream = null;
		this.renameOutputFile(currentBulkOutputFilePath, globalAttributes);
		currentBulkOutputFilePath = null;
	}

}
