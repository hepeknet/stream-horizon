package com.threeglav.bauk.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileUtil {

	private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

	public static String getFileAsText(final String filePath) {
		if (StringUtil.isEmpty(filePath)) {
			throw new IllegalArgumentException("File path must not be null or empty string");
		}
		final File f = new File(filePath);
		if (!f.exists() && f.canRead() && f.isFile()) {
			throw new IllegalArgumentException("Unable to find readable file at path [" + filePath + "]");
		}
		final StringBuilder sb = new StringBuilder();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(f));
			String line = reader.readLine();
			while (line != null) {
				sb.append(line);
				line = reader.readLine();
			}
		} catch (final IOException ie) {
			LOG.error("IOException while reading file {}", filePath);
			LOG.error("Details: ", ie);
		} finally {
			IOUtils.closeQuietly(reader);
		}
		LOG.debug("Content for {} is {}", filePath, sb);
		return sb.toString();
	}

}
