package com.threeglav.sh.bauk.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BaukConstants;

public abstract class StringUtil {

	public static final int DEFAULT_STRING_BUILDER_CAPACITY = 500;

	private static final Logger LOG = LoggerFactory.getLogger(StringUtil.class);

	private static final String NULL_VALUE = "NULL";

	private static final boolean isDebugEnabled = LOG.isDebugEnabled();

	public static final boolean isEmpty(final String str) {
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

	public static String replaceAllAttributes(final String statement, final Map<String, String> attributes, final String dbStringLiteral,
			final String dbStringEscapeLiteral) {
		if (StringUtil.isEmpty(dbStringLiteral)) {
			throw new IllegalArgumentException("String literal must not be null or empty!");
		}
		if (StringUtil.isEmpty(dbStringEscapeLiteral)) {
			throw new IllegalArgumentException("String escape literal must not be null or empty!");
		}
		if (isEmpty(statement)) {
			LOG.info("Statement is null or empty! Unable to replace any attributes");
			return statement;
		}
		if (attributes == null || attributes.isEmpty()) {
			LOG.debug("No attributes passed. Unable to replace any attributes in statement {}", statement);
			return statement;
		}
		if (isDebugEnabled) {
			LOG.debug("Replacing all attributes {} in [{}]", attributes, statement);
		}
		final String prefix = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START;
		String replaced = statement;
		for (final String key : attributes.keySet()) {
			final String placeHolder = prefix + key + BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			final String value = attributes.get(key);
			replaced = replaceSingleAttribute(replaced, placeHolder, value, dbStringLiteral, dbStringEscapeLiteral);
		}
		return replaced;
	}

	public static String replaceSingleAttribute(final String statement, final String attributeName, final String attributeValue,
			final String dbStringLiteral, final String dbStringEscapeLiteral) {
		String replaced = statement;
		if (attributeValue == null) {
			// also try to replace '${abc}' with NULL
			final String stringEnclosedPlaceHolder = dbStringLiteral + attributeName + dbStringLiteral;
			if (isDebugEnabled) {
				LOG.debug("Trying to replace {} with {}", stringEnclosedPlaceHolder, NULL_VALUE);
			}
			replaced = StringUtils.replace(replaced, stringEnclosedPlaceHolder, NULL_VALUE);
			if (isDebugEnabled) {
				LOG.debug("Trying to replace {} with {}", attributeName, NULL_VALUE);
			}
			replaced = StringUtils.replace(replaced, attributeName, NULL_VALUE);
		} else {
			if (isDebugEnabled) {
				LOG.debug("Replacing {} with {}", attributeName, attributeValue);
			}
			// escape all quotes
			final String cleanedUpValue = StringUtils.replace(attributeValue, dbStringLiteral, dbStringEscapeLiteral);
			replaced = StringUtils.replace(replaced, attributeName, cleanedUpValue);
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

	public static Set<String> collectAllAttributesFromString(final String str) {
		if (StringUtil.isEmpty(str)) {
			return null;
		}
		final Set<String> attributes = new HashSet<>();
		int fromIndex = 0;
		while (true) {
			final int indexOfPlaceholderStart = str.indexOf(BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START, fromIndex);
			if (indexOfPlaceholderStart != -1) {
				final int indexOfPlaceHolderEnd = str.indexOf(BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END, indexOfPlaceholderStart);
				if (indexOfPlaceHolderEnd != -1) {
					final String attributeName = str.substring(
							indexOfPlaceholderStart + BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START.length(), indexOfPlaceHolderEnd);
					attributes.add(attributeName);
					fromIndex = indexOfPlaceHolderEnd;
				} else {
					break;
				}
			} else {
				break;
			}
		}
		return attributes;
	}

}
