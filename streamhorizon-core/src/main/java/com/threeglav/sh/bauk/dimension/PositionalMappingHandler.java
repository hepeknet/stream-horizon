package com.threeglav.sh.bauk.dimension;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;

public final class PositionalMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final int positionInInputFeed;

	public PositionalMappingHandler(final int positionInParsedFeedLine, final int expectedFeedAttrsNumber) {
		if (positionInParsedFeedLine < 0) {
			throw new IllegalArgumentException("Position must not be negative number");
		}
		positionInInputFeed = positionInParsedFeedLine;
		if (expectedFeedAttrsNumber < 0 || expectedFeedAttrsNumber < positionInInputFeed) {
			throw new IllegalArgumentException("Expected feed attributes must not be negative or less than " + positionInInputFeed);
		}
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes) {
		try {
			return parsedLine[positionInInputFeed];
		} catch (final Exception exc) {
			log.error("Exception while trying to get value on position {} from {}. Check your configuration and input feed data!",
					positionInInputFeed, Arrays.toString(parsedLine));
			throw exc;
		}
	}

	@Override
	public void closeCurrentFeed() {

	}

	@Override
	public String getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return this.getBulkLoadValue(parsedLine, globalValues);
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return false;
	}

}
