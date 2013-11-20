package com.threeglav.bauk.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.Constants;

public abstract class StringUtil {

	private static final Logger LOG = LoggerFactory.getLogger(StringUtil.class);

	public static boolean isEmpty(final String str) {
		return str == null || str.trim().isEmpty();
	}

	public static String replaceAllNonASCII(final String original) {
		if (original == null) {
			return original;
		}
		return original.replaceAll("[^A-Za-z0-9 ]", "");
	}

	public static String getFileNameWithoutExtension(final String fileName) {
		if (StringUtil.isEmpty(fileName)) {
			return fileName;
		}
		final int lastDotIndex = fileName.lastIndexOf('.');
		if (lastDotIndex != -1) {
			return fileName.substring(0, lastDotIndex);
		}
		return fileName;
	}

	public static String replaceAllAttributes(final String statement, final Map<String, String> attributes, final String attributePrefix) {
		if (isEmpty(statement)) {
			LOG.info("Statement is null or empty! Unable to replace any attributes");
			return statement;
		}
		if (attributes == null || attributes.isEmpty()) {
			LOG.debug("No attributes passed. Unable to replace any attributes in statement {}", statement);
			return statement;
		}
		LOG.debug("Replacing all attributes {} in [{}]", attributes, statement);
		String prefix = Constants.STATEMENT_PLACEHOLDER_DELIMITER_START;
		if (!isEmpty(attributePrefix)) {
			prefix = prefix + attributePrefix;
		}
		String replaced = statement;
		for (final String key : attributes.keySet()) {
			final String placeHolder = prefix + key + Constants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			final String value = attributes.get(key);
			LOG.trace("Replacing {} with {}", placeHolder, value);
			replaced = replaced.replace(placeHolder, value);
		}
		return replaced;
	}

	public static String fixFilePath(final String path) {
		if (isEmpty(path)) {
			return path;
		}
		return path.replace("\\", "/");
	}

	public static String getSimpleClassName(final String fullClassName) {
		if (StringUtil.isEmpty(fullClassName)) {
			return fullClassName;
		}
		final int lastIndexOfDot = fullClassName.lastIndexOf('.');
		if (lastIndexOfDot != -1) {
			return fullClassName.substring(lastIndexOfDot + 1);
		}
		return fullClassName;
	}

}
