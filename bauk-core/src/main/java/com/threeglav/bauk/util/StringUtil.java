package com.threeglav.bauk.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BaukConstants;

public abstract class StringUtil {

	private static final Logger LOG = LoggerFactory.getLogger(StringUtil.class);

	private static final String NULL_VALUE = "NULL";

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

	public static String replaceAllAttributes(final String statement, final Map<String, String> attributes, final String attributePrefix,
			final String dbStringLiteral) {
		if (StringUtil.isEmpty(dbStringLiteral)) {
			throw new IllegalArgumentException("String literal must not be null or empty!");
		}
		if (isEmpty(statement)) {
			LOG.info("Statement is null or empty! Unable to replace any attributes");
			return statement;
		}
		if (attributes == null || attributes.isEmpty()) {
			LOG.debug("No attributes passed. Unable to replace any attributes in statement {}", statement);
			return statement;
		}
		LOG.debug("Replacing all attributes {} in [{}]", attributes, statement);
		String prefix = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START;
		if (!isEmpty(attributePrefix)) {
			prefix = prefix + attributePrefix;
		}
		String replaced = statement;
		for (final String key : attributes.keySet()) {
			final String placeHolder = prefix + key + BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			final String value = attributes.get(key);
			replaced = replaceSingleAttribute(replaced, placeHolder, value, dbStringLiteral);
		}
		return replaced;
	}

	public static String replaceSingleAttribute(final String statement, final String attributeName, final String attributeValue,
			final String dbStringLiteral) {
		String replaced = statement;
		if (attributeValue == null) {
			// also try to replace '${abc}' with NULL
			final String stringEnclosedPlaceHolder = dbStringLiteral + attributeName + dbStringLiteral;
			LOG.debug("Trying to replace {} with {}", stringEnclosedPlaceHolder, NULL_VALUE);
			replaced = replaced.replace(stringEnclosedPlaceHolder, NULL_VALUE);
			LOG.debug("Trying to replace {} with {}", attributeName, NULL_VALUE);
			replaced = replaced.replace(attributeName, NULL_VALUE);
		} else {
			LOG.trace("Replacing {} with {}", attributeName, attributeValue);
			replaced = replaced.replace(attributeName, attributeValue);
		}
		return replaced;
	}

	public static String getNaturalKeyCacheKey(final String[] values) {
		if (values == null) {
			throw new IllegalArgumentException("Unable to build cache key from null!");
		}
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			sb.append(values[i]);
		}
		return sb.toString();
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
