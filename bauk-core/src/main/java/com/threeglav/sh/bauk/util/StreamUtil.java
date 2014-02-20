package com.threeglav.sh.bauk.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public abstract class StreamUtil {

	public static InputStream ungzipInputStream(final InputStream inputStream) throws IOException {
		if (inputStream == null) {
			throw new IllegalArgumentException("InputStream must not be null");
		}
		return new GZIPInputStream(inputStream);
	}

}
