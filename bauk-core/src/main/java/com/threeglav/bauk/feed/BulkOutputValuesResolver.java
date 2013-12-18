package com.threeglav.bauk.feed;

import gnu.trove.map.hash.THashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.dimension.CachedPerFeedDimensionHandler;
import com.threeglav.bauk.dimension.ConstantOutputValueHandler;
import com.threeglav.bauk.dimension.DimensionHandler;
import com.threeglav.bauk.dimension.GlobalAttributeMappingHandler;
import com.threeglav.bauk.dimension.PositionalMappingHandler;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadFormatDefinition;
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

	// one handler per dimension only
	private static final Map<String, DimensionHandler> cachedDimensionHandlers = new THashMap<String, DimensionHandler>();

	public BulkOutputValuesResolver(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier,
			final CacheInstanceManager cacheInstanceManager) {
		super(factFeed, config);
		if (config.getDimensions() == null || config.getDimensions().isEmpty()) {
			throw new IllegalArgumentException("Did not find any dimensions defined! Check your configuration file");
		}
		if (cacheInstanceManager == null) {
			throw new IllegalArgumentException("Cache instance manager must not be null");
		}
		this.validate();
		this.cacheInstanceManager = cacheInstanceManager;
		bulkOutputFileNumberOfValues = factFeed.getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes().size();
		outputValueHandlers = new BulkLoadOutputValueHandler[bulkOutputFileNumberOfValues];
		log.info("Bulk output file will have {} values delimited by {}", bulkOutputFileNumberOfValues, factFeed.getDelimiterString());
		this.createOutputValueHandlers(routeIdentifier);
	}

	private void validate() {
		final BulkLoadDefinition bulkDefinition = this.getFactFeed().getBulkLoadDefinition();
		if (bulkDefinition == null) {
			throw new IllegalArgumentException("Bulk definition not found in configuration for feed " + this.getFactFeed().getName());
		}
		final BulkLoadFormatDefinition bulkLoadFormatDefinition = bulkDefinition.getBulkLoadFormatDefinition();
		if (bulkLoadFormatDefinition == null) {
			throw new IllegalArgumentException("Bulk load format definition not found in configuration file. Please check your configuration!");
		}
		if (bulkLoadFormatDefinition.getAttributes() == null || bulkLoadFormatDefinition.getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any defined bulk output attributes");
		}
		if (this.getFactFeed().getData() == null) {
			throw new IllegalArgumentException("Did not find any data definition for feed " + this.getFactFeed().getName());
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
		final Map<String, Integer> feedAttributeNamesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(feedData.getAttributes());
		for (int i = 0; i < bulkOutputAttributeNames.length; i++) {
			final String bulkOutputAttributeName = bulkOutputAttributeNames[i];
			if (StringUtil.isEmpty(bulkOutputAttributeName)) {
				final Attribute attr = bulkOutputAttributes.get(i);
				final String value = attr.getConstantValue();
				outputValueHandlers[i] = new ConstantOutputValueHandler(value);
				log.debug("Value at position {} in bulk output load will be constant value {}", i, value);
			} else if (bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
				final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
				log.debug("Searching for configured dimension by name [{}]", requiredDimensionName);
				final DimensionHandler cachedHandler = cachedDimensionHandlers.get(requiredDimensionName);
				if (cachedHandler != null) {
					outputValueHandlers[i] = cachedHandler;
				} else {
					final Dimension dim = this.getConfig().getDimensionMap().get(requiredDimensionName);
					if (dim == null) {
						throw new IllegalArgumentException("Was not able to find dimension definition for dimension with name ["
								+ requiredDimensionName + "]. This dimension is used to create bulk output! Please check your configuration!");
					}
					final boolean cachePerFeedDimension = !StringUtil.isEmpty(dim.getCacheKeyPerFeedInto());
					DimensionHandler dimHandler = null;
					if (cachePerFeedDimension) {
						dimHandler = new CachedPerFeedDimensionHandler(dim, this.getFactFeed(), cacheInstanceManager.getCacheInstance(dim.getName()),
								feedDataLineOffset, routeIdentifier, this.getConfig());
					} else {
						dimHandler = new DimensionHandler(dim, this.getFactFeed(), cacheInstanceManager.getCacheInstance(dim.getName()),
								feedDataLineOffset, routeIdentifier, this.getConfig());
					}
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
			} else {
				final GlobalAttributeMappingHandler cmh = new GlobalAttributeMappingHandler(bulkOutputAttributeName);
				outputValueHandlers[i] = cmh;
				log.debug("Value at position {} in bulk output load will be mapped value derived from {}", i, bulkOutputAttributeName);
			}
		}
	}

	public void startFeed(final Map<String, String> globalData) {
		if (isDebugEnabled) {
			log.debug("Starting feed with attributes {}", globalData);
		}
		for (int i = 0; i < outputValueHandlers.length; i++) {
			outputValueHandlers[i].calculatePerFeedValues(globalData);
		}
		if (isDebugEnabled) {
			log.debug("Started feed. In total have {} dimension handlers. Global attributes {}", outputValueHandlers.length, globalData);
		}
	}

	public String[] resolveValues(final String[] inputValues, final Map<String, String> globalData, final boolean isLastLine) {
		final String[] output = new String[bulkOutputFileNumberOfValues];
		for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
			output[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData, isLastLine);
		}
		return output;
	}

	public void closeCurrentFeed() {
		if (outputValueHandlers != null) {
			for (int i = 0; i < outputValueHandlers.length; i++) {
				outputValueHandlers[i].closeCurrentFeed();
			}
		}
	}

}
