package com.threeglav.bauk.dimension;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.util.StringUtil;

public final class GlobalAttributeMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String attributeName;
	private Map<String, String> latestUsedGlobalMap;
	private String latestGlobalValue;

	public GlobalAttributeMappingHandler(final String attributeName) {
		if (StringUtil.isEmpty(attributeName)) {
			throw new IllegalArgumentException("Attribute name must not be null or empty string");
		}
		this.attributeName = attributeName;
		log.debug("Will look for attribute {}", attributeName);
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues, final boolean isLastLine) {
		if (globalValues != latestUsedGlobalMap) {
			latestUsedGlobalMap = globalValues;
			latestGlobalValue = globalValues.get(attributeName);
		}
		return latestGlobalValue;
	}

	@Override
	public void closeCurrentFeed() {

	}

	@Override
	public void calculatePerFeedValues(final Map<String, String> globalValues) {

	}

}
