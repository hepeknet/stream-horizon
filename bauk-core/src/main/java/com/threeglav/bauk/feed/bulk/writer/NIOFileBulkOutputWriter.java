package com.threeglav.bauk.feed.bulk.writer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class NIOFileBulkOutputWriter extends ConfigAware implements BulkOutputWriter {

	private static final byte[] NEW_LINE_BYTES = "\n".getBytes();
	private final byte[] bulkOutputFileDelimiterBytes;
	private FileChannel rwChannel;
	private ByteBuffer wrBuf;

	public NIOFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bulkOutputFileDelimiterBytes = factFeed.getBulkLoadDefinition().getBulkLoadFileDelimiter().getBytes();
	}

	private void createFileWriter(final String outputFilePath) {
		try {
			rwChannel = new RandomAccessFile(outputFilePath, "rw").getChannel();
			wrBuf = ByteBuffer.allocate(Integer.MAX_VALUE);
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
	public void doOutput(final String[] resolvedData) {
		try {
			for (int i = 0; i < resolvedData.length; i++) {
				if (i != 0) {
					wrBuf.put(bulkOutputFileDelimiterBytes);
				}
				final String data = resolvedData[i];
				final byte[] bytes = data.getBytes();
				wrBuf.put(bytes);
			}
			wrBuf.put(NEW_LINE_BYTES);
		} catch (final Exception exc) {
			log.error("Exception while writing data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
		try {
			wrBuf.flip();
			while (wrBuf.hasRemaining()) {
				rwChannel.write(wrBuf);
			}
			rwChannel.force(true);
		} catch (final IOException ie) {
			log.error("Exception while writing data to file", ie);
		}
		wrBuf.clear();
		IOUtils.closeQuietly(rwChannel);
		rwChannel = null;
	}

}
