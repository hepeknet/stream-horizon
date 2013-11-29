package com.threeglav.bauk.header;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.parser.FullFeedParser;

public class DefaultHeaderParser implements HeaderParser {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	@Override
	public Map<String, String> parseHeader(final String headerLine, final String[] declaredAttributeNames, final String startsWithString,
			final String delimiter) {
		if (headerLine == null) {
			return new HashMap<String, String>();
		}
		final Map<String, String> headerValues = new HashMap<String, String>();
		final FullFeedParser ffp = new FullFeedParser(delimiter);
		final String[] parsed = ffp.parse(headerLine);
		if (parsed == null || parsed.length == 0) {
			throw new IllegalArgumentException("Header is null or has zero length");
		}
		final String expectedCharacter = startsWithString;
		if (expectedCharacter == null) {
			throw new IllegalStateException("Expected character for header not set");
		}
		log.debug("Expected character is [{}]", expectedCharacter);
		if (!expectedCharacter.equals(parsed[0])) {
			throw new IllegalStateException("First header character [" + parsed[0] + "] does not match expected character [" + expectedCharacter
					+ "]");
		}
		final int declaredAttributesSize = declaredAttributeNames.length;
		final int parsedLength = parsed.length - 1;
		if (parsedLength != declaredAttributesSize) {
			throw new IllegalStateException("Number of defined header attributes " + declaredAttributesSize
					+ " is different than number of parsed items " + parsedLength + " from " + Arrays.toString(parsed));
		}
		for (int i = 1; i < parsed.length; i++) {
			headerValues.put(declaredAttributeNames[i - 1], parsed[i]);
		}
		return headerValues;
	}

}
