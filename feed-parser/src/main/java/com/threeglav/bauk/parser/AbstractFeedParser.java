package com.threeglav.bauk.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFeedParser implements FeedParser {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final int DEFAULT_EXPECTED_MEMBERS = 50;

	private static final String EMPTY_STRING = "";
	protected final String delimiter;
	private int expectedTokens = DEFAULT_EXPECTED_MEMBERS;
	private String nullString;
	protected final int delimiterLenght;
	private static final char DEFAULT_NON_INIT_CHAR = '\0';
	private char singleCharacterDelimiter = DEFAULT_NON_INIT_CHAR;

	public AbstractFeedParser(final String delimiter) {
		if (delimiter == null || delimiter.trim().isEmpty()) {
			throw new IllegalArgumentException("Delimiter must not be null or empty");
		}
		this.delimiter = delimiter;
		delimiterLenght = delimiter.length();
		log.debug("Created parser for delimiter {}", delimiter);
		if (delimiterLenght == 1) {
			singleCharacterDelimiter = delimiter.charAt(0);
			log.info("Single character delimiter {}. This will slightly speed up execution", singleCharacterDelimiter);
		}
	}

	public void setExpectedTokens(final int expectedTokens) {
		this.expectedTokens = expectedTokens;
	}

	public void setNullString(final String nullStr) {
		nullString = nullStr;
	}

	protected boolean isNullStringValue(final String value) {
		return value.equals(nullString);
	}

	protected boolean isNullOrEmpty(final String val) {
		return val == null || EMPTY_STRING.equals(val);
	}

	protected String[] splitLine(final String line) {
		return splitLine(line, 0);
	}

	protected String[] splitLine(final String line, final int skipCharacters) {
		try {
			if (singleCharacterDelimiter != DEFAULT_NON_INIT_CHAR) {
				return splitCharacterDelimiter(line, skipCharacters);
			} else {
				return splitStringDelimiter(line, skipCharacters);
			}
		} catch (final Exception exc) {
			log.error("Exception while parsing line {}, skipCharacters {}, delimiter {}", line, skipCharacters, delimiter);
			log.error("Details", exc);
			return null;
		}
	}

	private String[] splitStringDelimiter(final String line, final int skipCharacters) {
		final String[] lines = new String[expectedTokens];
		int indexOfDelimiter = line.indexOf(delimiter, skipCharacters);
		int fromIndex = skipCharacters;
		int count = 0;
		while (indexOfDelimiter != -1 && count < (expectedTokens - 1)) {
			lines[count++] = line.substring(fromIndex, indexOfDelimiter);
			fromIndex = indexOfDelimiter + delimiterLenght;
			indexOfDelimiter = line.indexOf(delimiter, fromIndex);
		}
		lines[count++] = line.substring(fromIndex, line.length());
		final String[] finalLines = new String[count];
		System.arraycopy(lines, 0, finalLines, 0, count);
		return finalLines;
	}

	private String[] splitCharacterDelimiter(final String line, final int skipCharacters) {
		final String[] lines = new String[expectedTokens];
		int indexOfDelimiter = line.indexOf(singleCharacterDelimiter, skipCharacters);
		int fromIndex = skipCharacters;
		int count = 0;
		while (indexOfDelimiter != -1) {
			lines[count++] = line.substring(fromIndex, indexOfDelimiter);
			fromIndex = indexOfDelimiter + 1;
			indexOfDelimiter = line.indexOf(singleCharacterDelimiter, fromIndex);
		}
		lines[count++] = line.substring(fromIndex, line.length());
		final String[] finalLines = new String[count];
		System.arraycopy(lines, 0, finalLines, 0, count);
		return finalLines;
	}

}
