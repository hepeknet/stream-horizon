package com.threeglav.bauk.dimension;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.util.StringUtil;

public class ConstantMappingHandler implements BulkLoadOutputValueHandler {

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private final String attributeName;
	private String attributeNameWithoutPrefix;
	private boolean isHeaderMapping;
	private boolean isGlobalMapping;
	private Map<String, String> latestUsedHeaderMap;
	private String latestHeaderValue;
	private Map<String, String> latestUsedGlobalMap;
	private String latestGlobalValue;

	public ConstantMappingHandler(final String attributeName) {
		if (StringUtil.isEmpty(attributeName)) {
			throw new IllegalArgumentException("Attribute name must not be null or empty string");
		}
		this.attributeName = attributeName;
		if (this.attributeName.startsWith(Constants.HEADER_ATTRIBUTE_PREFIX)) {
			this.attributeNameWithoutPrefix = this.attributeName.replace(Constants.HEADER_ATTRIBUTE_PREFIX, "");
			this.isHeaderMapping = true;
		} else if (this.attributeName.startsWith(Constants.GLOBAL_ATTRIBUTE_PREFIX)) {
			this.attributeNameWithoutPrefix = this.attributeName.replace(Constants.GLOBAL_ATTRIBUTE_PREFIX, "");
			this.isGlobalMapping = true;
		}
		this.log.debug("Will look for attribute {}", attributeName);
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> headerValues, final Map<String, String> globalValues) {
		if (this.isHeaderMapping && headerValues != null) {
			// no need to lookup more than once per feed, so we just compare reference
			if (headerValues != this.latestUsedHeaderMap) {
				this.latestUsedHeaderMap = headerValues;
				this.latestHeaderValue = headerValues.get(this.attributeNameWithoutPrefix);
			}
			return this.latestHeaderValue;
		} else if (this.isGlobalMapping && globalValues != null) {
			if (globalValues != this.latestUsedGlobalMap) {
				this.latestUsedGlobalMap = globalValues;
				this.latestGlobalValue = globalValues.get(this.attributeNameWithoutPrefix);
			}
			return this.latestGlobalValue;
		}
		return null;
	}

}
