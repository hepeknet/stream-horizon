package com.threeglav.sh.bauk.dimension;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.util.StringUtil;

public final class GlobalAttributeMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String attributeName;

	public GlobalAttributeMappingHandler(final String attributeName) {
		if (StringUtil.isEmpty(attributeName)) {
			throw new IllegalArgumentException("Attribute name must not be null or empty string");
		}
		this.attributeName = attributeName;
		log.debug("Will look for attribute {}", attributeName);
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return globalValues.get(attributeName);
	}

	@Override
	public void closeCurrentFeed() {

	}

	@Override
	public void calculatePerFeedValues(final Map<String, String> globalValues) {

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
