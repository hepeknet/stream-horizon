package com.threeglav.bauk.dimension;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.util.StringUtil;

public final class HeaderGlobalMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String attributeName;
	private String attributeNameWithoutPrefix;
	private final boolean isHeaderMapping;
	private final boolean isGlobalMapping;
	private Map<String, String> latestUsedHeaderMap;
	private String latestHeaderValue;
	private Map<String, String> latestUsedGlobalMap;
	private String latestGlobalValue;

	public HeaderGlobalMappingHandler(final String attributeName) {
		if (StringUtil.isEmpty(attributeName)) {
			throw new IllegalArgumentException("Attribute name must not be null or empty string");
		}
		this.attributeName = attributeName;
		if (this.attributeName.startsWith(BaukConstants.HEADER_ATTRIBUTE_PREFIX)) {
			attributeNameWithoutPrefix = this.attributeName.replace(BaukConstants.HEADER_ATTRIBUTE_PREFIX, "");
			isHeaderMapping = true;
			isGlobalMapping = false;
		} else if (this.attributeName.startsWith(BaukConstants.GLOBAL_ATTRIBUTE_PREFIX)) {
			attributeNameWithoutPrefix = this.attributeName.replace(BaukConstants.GLOBAL_ATTRIBUTE_PREFIX, "");
			isGlobalMapping = true;
			isHeaderMapping = false;
		} else {
			throw new IllegalArgumentException("Unsupported attribute name " + attributeName);
		}
		log.debug("Will look for attribute {}", attributeName);
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> headerValues, final Map<String, String> globalValues) {
		if (isHeaderMapping && headerValues != null) {
			// no need to lookup more than once per feed, so we just compare the reference instead of doing map lookup
			if (headerValues != latestUsedHeaderMap) {
				latestUsedHeaderMap = headerValues;
				latestHeaderValue = headerValues.get(attributeNameWithoutPrefix);
			}
			return latestHeaderValue;
		} else if (isGlobalMapping && globalValues != null) {
			if (globalValues != latestUsedGlobalMap) {
				latestUsedGlobalMap = globalValues;
				latestGlobalValue = globalValues.get(attributeNameWithoutPrefix);
			}
			return latestGlobalValue;
		}
		return null;
	}

	@Override
	public void closeCurrentFeed() {

	}

}
