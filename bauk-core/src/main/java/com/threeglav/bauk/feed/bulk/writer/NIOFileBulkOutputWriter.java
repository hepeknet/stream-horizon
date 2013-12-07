package com.threeglav.bauk.feed.bulk.writer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class NIOFileBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	private FileChannel rwChannel;
	private final int bufferSize;
	private ByteBuffer wrBuf;
	private int position;

	public NIOFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
		log.debug("Write buffer size is {}", bufferSize);
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			rwChannel = new RandomAccessFile(outputFilePath, "rw").getChannel();
			position = 0;
			wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferSize);
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
			if (!wrBuf.hasRemaining()) {
				wrBuf = rwChannel.map(FileChannel.MapMode.READ_WRITE, position, bufferSize);
			}
			final byte[] bytes = line.getBytes();
			position += bytes.length;
			wrBuf.put(bytes);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources() {
		IOUtils.closeQuietly(rwChannel);
		rwChannel = null;
	}

}
