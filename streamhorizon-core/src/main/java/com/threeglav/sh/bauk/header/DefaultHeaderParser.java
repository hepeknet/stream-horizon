package com.threeglav.sh.bauk.header;

import gnu.trove.map.hash.THashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.header.HeaderParser;
import com.threeglav.sh.bauk.parser.FullFeedParser;

public final class DefaultHeaderParser implements HeaderParser {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private String startsWithString;
	private FullFeedParser fullFeedParser;
	private final boolean isDebugEnabled;

	public DefaultHeaderParser() {
		isDebugEnabled = log.isDebugEnabled();
	}

	@Override
	public Map<String, String> parseHeader(final String headerLine, final String[] declaredAttributeNames, final Map<String, String> globalAttributes) {
		if (headerLine == null) {
			return new HashMap<String, String>();
		}
		final Map<String, String> headerValues = new THashMap<String, String>();
		final String[] parsed = fullFeedParser.parse(headerLine);
		if (parsed == null || parsed.length == 0) {
			throw new IllegalArgumentException("Header is null or has zero length");
		}
		if (!startsWithString.equals(parsed[0])) {
			throw new IllegalStateException("First header character [" + parsed[0] + "] does not match expected character [" + startsWithString + "]");
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

	@Override
	public void init(final String configuredHeaderStartsWithString, final String configuredDelimiter, final Map<String, String> engineConfigProperties) {
		startsWithString = configuredHeaderStartsWithString;
		fullFeedParser = new FullFeedParser(configuredDelimiter);
		if (startsWithString == null) {
			throw new IllegalStateException("Expected character for header not set");
		}
		if (isDebugEnabled) {
			log.debug("Expected character is header line should start with is [{}]", startsWithString);
		}
	}

}
