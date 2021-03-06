package com.threeglav.sh.bauk.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFeedParser implements FeedParser {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	protected final String delimiter;
	private final int expectedTokens;
	private String nullString;
	private final int delimiterLenght;
	private static final char DEFAULT_NON_INIT_CHAR = '\0';
	private final char singleCharacterDelimiter;
	private final boolean isSingleCharacterDelimiter;
	private final int expectedTokensMinusOne;

	public AbstractFeedParser(final String delimiter, final int expectedTokens) {
		if (delimiter == null || delimiter.trim().isEmpty()) {
			throw new IllegalArgumentException("Value delimiter must not be null or empty. Check your configuration!");
		}
		this.delimiter = delimiter;
		delimiterLenght = delimiter.length();
		log.debug("Created parser for delimiter {}", delimiter);
		if (delimiterLenght == 1) {
			singleCharacterDelimiter = delimiter.charAt(0);
			isSingleCharacterDelimiter = true;
			log.info("Single character delimiter {}. This will slightly speed up execution", singleCharacterDelimiter);
		} else {
			isSingleCharacterDelimiter = false;
			singleCharacterDelimiter = DEFAULT_NON_INIT_CHAR;
		}
		if (expectedTokens <= 0) {
			throw new IllegalArgumentException("Expected tokens must be positive integer!");
		}
		this.expectedTokens = expectedTokens;
		expectedTokensMinusOne = this.expectedTokens - 1;
	}

	public void setNullString(final String nullStr) {
		nullString = nullStr;
	}

	protected final boolean isNullStringValue(final String value) {
		return value.equals(nullString);
	}

	protected final boolean isParsedValueNull(final String val) {
		return val == null || val.isEmpty();
	}

	protected final String[] splitLine(final String line) {
		return this.splitLine(line, 0);
	}

	private String[] splitLine(final String line, final int skipCharacters) {
		try {
			if (isSingleCharacterDelimiter) {
				return this.splitCharacterDelimiter(line, skipCharacters);
			} else {
				return this.splitMultiCharacterDelimiter(line, skipCharacters);
			}
		} catch (final Exception exc) {
			log.error("Exception while parsing line [{}], skipCharacters {}, delimiter {}, expectedTokens {}", new Object[] { line, skipCharacters,
					delimiter, expectedTokens });
			Throwable cause = exc;
			while (cause != null) {
				log.error("Details", cause);
				cause = cause.getCause();
			}
			return null;
		}
	}

	private String[] splitMultiCharacterDelimiter(final String line, final int skipCharacters) {
		final String[] lines = new String[expectedTokens];
		int indexOfDelimiter = line.indexOf(delimiter, skipCharacters);
		int fromIndex = skipCharacters;
		int count = 0;
		while (indexOfDelimiter != -1 && count < expectedTokensMinusOne) {
			final String val = line.substring(fromIndex, indexOfDelimiter);
			if (this.isParsedValueNull(val)) {
				lines[count++] = null;
			} else {
				lines[count++] = val;
			}
			fromIndex = indexOfDelimiter + delimiterLenght;
			indexOfDelimiter = line.indexOf(delimiter, fromIndex);
		}
		final String val = line.substring(fromIndex, line.length());
		if (this.isParsedValueNull(val)) {
			lines[count++] = null;
		} else {
			lines[count++] = val;
		}
		// if we already allocated array of appropriate size and filled it
		if (count == expectedTokens) {
			return lines;
		}
		// otherwise we want to send only used part of array (in case we did not know how many tokens to expect)
		final String[] finalLines = new String[count];
		System.arraycopy(lines, 0, finalLines, 0, count);
		return finalLines;
	}

	private String[] splitCharacterDelimiter(final String line, final int skipCharacters) {
		final String[] lines = new String[expectedTokens];
		int indexOfDelimiter = line.indexOf(singleCharacterDelimiter, skipCharacters);
		int fromIndex = skipCharacters;
		int count = 0;
		while (indexOfDelimiter != -1 && count < expectedTokensMinusOne) {
			final String val = line.substring(fromIndex, indexOfDelimiter);
			if (this.isParsedValueNull(val)) {
				lines[count++] = null;
			} else {
				lines[count++] = val;
			}
			fromIndex = indexOfDelimiter + delimiterLenght;
			indexOfDelimiter = line.indexOf(singleCharacterDelimiter, fromIndex);
		}
		final String val = line.substring(fromIndex, line.length());
		if (this.isParsedValueNull(val)) {
			lines[count++] = null;
		} else {
			lines[count++] = val;
		}
		// if we already allocated array of appropriate size and filled it
		if (count == expectedTokens) {
			return lines;
		}
		// otherwise we want to send only used part of array (in case we did not know how many tokens to expect)
		final String[] finalLines = new String[count];
		System.arraycopy(lines, 0, finalLines, 0, count);
		return finalLines;
	}

}
