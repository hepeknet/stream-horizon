package com.threeglav.bauk.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

public abstract class CompressionUtil {

	public static byte[] gzipToByteArray(final String str) throws IOException {
		if (str == null || str.length() == 0) {
			return null;
		}
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(str.getBytes());
		gzip.close();
		return out.toByteArray();
	}

	public static InputStream gzipToInputStream(final String str) throws IOException {
		final byte[] bytes = gzipToByteArray(str);
		if (bytes != null) {
			return new ByteArrayInputStream(bytes);
		}
		return null;
	}

}
