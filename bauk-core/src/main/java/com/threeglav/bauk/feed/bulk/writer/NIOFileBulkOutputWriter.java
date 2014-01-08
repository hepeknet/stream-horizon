package com.threeglav.bauk.feed.bulk.writer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.StringUtil;

public class NIOFileBulkOutputWriter extends AbstractBulkOutputWriter {

	private static final byte[] NEW_LINE_BYTES = "\n".getBytes();
	private final byte[] bulkOutputFileDelimiterBytes;
	private FileChannel rwChannel;
	private ByteBuffer wrBuf;

	public NIOFileBulkOutputWriter(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
		bulkOutputFileDelimiterBytes = bulkOutputFileDelimiter.getBytes();
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
		log.debug("Initialized writer");
		this.createFileWriter(outputFilePath);
	}

	@Override
	public void doOutput(final String[] resolvedData) {
		if (resolvedData == null) {
			log.debug("Data is null");
			return;
		}
		try {
			log.debug("Writing. Size {}", resolvedData.length);
			for (int i = 0; i < resolvedData.length; i++) {
				if (i != 0) {
					wrBuf.put(bulkOutputFileDelimiterBytes);
				}
				final String data = resolvedData[i];
				final byte[] bytes = data.getBytes();
				log.debug("Wrote {} bytes to file", bytes.length);
				wrBuf.put(bytes);
			}
			wrBuf.put(NEW_LINE_BYTES);
		} catch (final Exception exc) {
			throw new RuntimeException("Exception while writing bulk output data", exc);
		}
	}

	@Override
	public void closeResources(final Map<String, String> globalAttributes) {
		try {
			log.debug("Closing file");
			wrBuf.flip();
			while (wrBuf.hasRemaining()) {
				log.debug("Remaining...");
				rwChannel.write(wrBuf);
			}
			rwChannel.force(true);
			log.debug("Forced....");
		} catch (final IOException ie) {
			log.error("Exception while writing data to file", ie);
		} finally {
			IOUtils.closeQuietly(rwChannel);
			wrBuf.clear();
			rwChannel = null;
			log.debug("Closed all");
		}
	}

}
