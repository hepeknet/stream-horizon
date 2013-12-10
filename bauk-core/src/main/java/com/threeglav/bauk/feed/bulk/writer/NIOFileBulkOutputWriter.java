package com.threeglav.bauk.feed.bulk.writer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class NIOFileBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	private static final byte[] NEW_LINE_BYTES = "\n".getBytes();
	private FileChannel rwChannel;
	private final int bufferSize;
	private ByteBuffer wrBuf;

	public NIOFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
		log.debug("Write buffer size is {}", bufferSize);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			rwChannel = new RandomAccessFile(outputFilePath, "rw").getChannel();
			wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);
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
		if (rwChannel != null) {
			throw new IllegalArgumentException("Writer is not null! Unable to start writing unless previous writer has been closed!");
		}
		this.createFileWriter(outputFilePath);
	}

	@Override
	public void doOutput(final String line) {
		try {
			final byte[] bytes = line.getBytes();
			final int bytesLen = bytes.length + NEW_LINE_BYTES.length;
			if (wrBuf.remaining() < bytesLen) {
				wrBuf.flip();
				rwChannel.write(wrBuf);
				wrBuf.clear();
			}
			wrBuf.put(bytes);
			wrBuf.put(NEW_LINE_BYTES);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
		try {
			if (wrBuf.hasRemaining()) {
				wrBuf.flip();
				rwChannel.write(wrBuf);
				rwChannel.force(true);
			}
		} catch (final IOException ie) {
			log.error("Exception while writing data to file", ie);
		}
		wrBuf.clear();
		IOUtils.closeQuietly(rwChannel);
		rwChannel = null;
	}

}
