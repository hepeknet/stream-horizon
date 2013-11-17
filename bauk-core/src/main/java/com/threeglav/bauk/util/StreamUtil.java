package com.threeglav.bauk.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamUtil {

	private static final Logger LOG = LoggerFactory.getLogger(StreamUtil.class);

	public static InputStream unzipInputStream(final InputStream inputStream) throws IOException {
		if (inputStream == null) {
			throw new IllegalArgumentException("InputStream must not be null");
		}
		final ZipInputStream zis = new ZipInputStream(inputStream);
		final ZipEntry ze = zis.getNextEntry();
		final String name = ze.getName();
		final long compressedSize = ze.getCompressedSize();
		final long size = ze.getSize();
		if (LOG.isDebugEnabled()) {
			LOG.debug("Found zip entry {} of compressed size {} and original size {}", new Object[] { name, compressedSize, size });
		}
		return zis;
	}

	public static InputStream ungzipInputStream(final InputStream inputStream) throws IOException {
		if (inputStream == null) {
			throw new IllegalArgumentException("InputStream must not be null");
		}
		return new GZIPInputStream(inputStream);
	}

}
