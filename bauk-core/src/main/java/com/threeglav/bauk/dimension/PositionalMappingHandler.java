package com.threeglav.bauk.dimension;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BulkLoadOutputValueHandler;

public class PositionalMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final int expectedFeedAttributes;
	private final int position;

	public PositionalMappingHandler(final int positionInParsedFeedLine, final int expectedFeedAttrsNumber) {
		if (positionInParsedFeedLine < 0) {
			throw new IllegalArgumentException("Position must not be negative number");
		}
		position = positionInParsedFeedLine;
		if (expectedFeedAttrsNumber < 0 || expectedFeedAttrsNumber < position) {
			throw new IllegalArgumentException("Expected feed attributes must not be negative or less than " + position);
		}
		expectedFeedAttributes = expectedFeedAttrsNumber;
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> headerAttributes, final Map<String, String> globalAttributes) {
		if (parsedLine.length < expectedFeedAttributes) {
			throw new IllegalArgumentException("Provided feed line has less than expected " + expectedFeedAttributes
					+ " values. Check your configuration or input data!");
		}
		try {
			return parsedLine[position];
		} catch (final Exception exc) {
			log.error("Exception while trying to get value on position {} from {}", position, Arrays.toString(parsedLine));
			throw exc;
		}
	}

}
