package com.threeglav.bauk.feed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.dimension.ConstantOutputValueHandler;
import com.threeglav.bauk.dimension.DimensionHandler;
import com.threeglav.bauk.dimension.HeaderGlobalMappingHandler;
import com.threeglav.bauk.dimension.PositionalMappingHandler;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadFormatDefinition;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.StringUtil;

public class BulkOutputValuesResolver extends ConfigAware {

	private static final String DIMENSION_PREFIX = "dimension.";
	private static final String FEED_PREFIX = "feed.";

	private final int bulkOutputFileNumberOfValues;
	private final BulkLoadOutputValueHandler[] outputValueHandlers;
	private final CacheInstanceManager cacheInstanceManager;
	private final DbHandler dbHandler;
	private final String outputDelimiter;

	public BulkOutputValuesResolver(final FactFeed factFeed, final Config config, final CacheInstanceManager cacheInstanceManager,
			final DbHandler dbHandler, final String routeIdentifier) {
		super(factFeed, config);
		if (config.getDimensions() == null || config.getDimensions().isEmpty()) {
			throw new IllegalArgumentException("Did not find any dimensions defined! Check your configuration file");
		}
		if (cacheInstanceManager == null) {
			throw new IllegalArgumentException("Cache handler must not be null");
		}
		this.cacheInstanceManager = cacheInstanceManager;
		if (dbHandler == null) {
			throw new IllegalArgumentException("DbHandler must not be null");
		}
		this.dbHandler = dbHandler;
		this.validate();
		bulkOutputFileNumberOfValues = factFeed.getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes().size();
		outputValueHandlers = new BulkLoadOutputValueHandler[bulkOutputFileNumberOfValues];
		log.info("Bulk output file will have {} values delimited by {}", bulkOutputFileNumberOfValues, factFeed.getDelimiterString());
		this.createOutputValueHandlers(routeIdentifier);
		outputDelimiter = factFeed.getDelimiterString();
	}

	private void validate() {
		final BulkLoadDefinition bulkDefinition = this.getFactFeed().getBulkLoadDefinition();
		if (bulkDefinition == null) {
			throw new IllegalArgumentException("Bulk definition not found in configuration");
		}
		final BulkLoadFormatDefinition bulkLoadFileDefinition = bulkDefinition.getBulkLoadFormatDefinition();
		if (bulkLoadFileDefinition == null) {
			throw new IllegalArgumentException("Bulk load file definition not found in configuration file. Please check your configuration!");
		}
		if (bulkLoadFileDefinition.getAttributes() == null || bulkLoadFileDefinition.getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any defined bulk output attributes");
		}
	}

	private void createOutputValueHandlers(final String routeIdentifier) {
		final ArrayList<Attribute> bulkOutputAttributes = this.getFactFeed().getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes();
		final String[] bulkOutputAttributeNames = AttributeParsingUtil.getAttributeNames(bulkOutputAttributes);
		if (log.isDebugEnabled()) {
			log.debug("Bulk output attributes are {}", Arrays.toString(bulkOutputAttributeNames));
		}
		final Data feedData = this.getFactFeed().getData();
		final String firstStringInEveryLine = feedData.getEachLineStartsWithCharacter();
		int feedDataLineOffset = 0;
		if (!StringUtil.isEmpty(firstStringInEveryLine)) {
			feedDataLineOffset = 1;
		}
		// one handler per dimension only
		final Map<String, DimensionHandler> cachedDimensionHandlers = new HashMap<String, DimensionHandler>();
		final Map<String, Integer> feedAttributeNamesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(feedData.getAttributes());
		for (int i = 0; i < bulkOutputAttributeNames.length; i++) {
			final String bulkOutputAttributeName = bulkOutputAttributeNames[i];
			if (bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
				final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
				log.debug("Searching for configured dimension by name [{}]", requiredDimensionName);
				final DimensionHandler cachedHandler = cachedDimensionHandlers.get(requiredDimensionName);
				if (cachedHandler != null) {
					outputValueHandlers[i] = cachedHandler;
				} else {
					final Dimension dim = this.getConfig().getDimensionMap().get(requiredDimensionName);
					if (dim == null) {
						throw new IllegalArgumentException("Was not able to find dimension definition for dimension with name ["
								+ requiredDimensionName + "]");
					}
					final DimensionHandler dimHandler = new DimensionHandler(dim, this.getFactFeed(), cacheInstanceManager.getCacheInstance(dim
							.getName()), dbHandler, feedDataLineOffset, routeIdentifier);
					cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
					outputValueHandlers[i] = dimHandler;
				}
				log.debug("Value at position {} in bulk output load will be mapped using dimension {}", i, requiredDimensionName);
			} else if (bulkOutputAttributeName.startsWith(FEED_PREFIX)) {
				final String requiredFeedAttributeName = bulkOutputAttributeName.replace(FEED_PREFIX, "");
				log.debug("Searching for feed attribute {}", requiredFeedAttributeName);
				final Integer foundPosition = feedAttributeNamesAndPositions.get(requiredFeedAttributeName);
				if (foundPosition == null) {
					throw new IllegalArgumentException("Was not able to find feed attribute " + requiredFeedAttributeName
							+ " in feed definition and this attribute was defined in bulk output file definition. Check config file!");
				}
				outputValueHandlers[i] = new PositionalMappingHandler(foundPosition + feedDataLineOffset, feedAttributeNamesAndPositions.size());
				log.debug(
						"Value at position {} in bulk output load will be copied directly from value in feed at position {}. Every data line in feed must have {} values",
						i, foundPosition, feedAttributeNamesAndPositions.size());
			} else if (bulkOutputAttributeName.startsWith(Constants.HEADER_ATTRIBUTE_PREFIX)
					|| bulkOutputAttributeName.startsWith(Constants.GLOBAL_ATTRIBUTE_PREFIX)) {
				final HeaderGlobalMappingHandler cmh = new HeaderGlobalMappingHandler(bulkOutputAttributeName);
				outputValueHandlers[i] = cmh;
				log.debug("Value at position {} in bulk output load will be mapped value derived from {}", i, bulkOutputAttributeName);
			} else if (StringUtil.isEmpty(bulkOutputAttributeName)) {
				final Attribute attr = bulkOutputAttributes.get(i);
				final String value = attr.getConstantValue();
				outputValueHandlers[i] = new ConstantOutputValueHandler(value);
				log.debug("Value at position {} in bulk output load will be constant value {}", value);
			} else {
				throw new IllegalArgumentException("Unknown type of bulk output attribute " + bulkOutputAttributeName
						+ ". Must be either dimension. feed. header. global.");
			}
		}
	}

	public String resolveValues(final String[] inputValues, final Map<String, String> headerData, final Map<String, String> globalData) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
			if (i != 0) {
				sb.append(outputDelimiter);
			}
			final String val = outputValueHandlers[i].getBulkLoadValue(inputValues, headerData, globalData);
			sb.append(val);
		}
		return sb.toString();
	}

}
