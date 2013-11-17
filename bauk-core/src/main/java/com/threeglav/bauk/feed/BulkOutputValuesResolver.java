package com.threeglav.bauk.feed;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.dimension.ConstantMappingHandler;
import com.threeglav.bauk.dimension.DimensionHandler;
import com.threeglav.bauk.dimension.PositionalMappingHandler;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.header.HeaderParsingUtil;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
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
		this.bulkOutputFileNumberOfValues = factFeed.getBulkDefinition().getBulkLoadFileDefinition().getAttributes().size();
		this.outputValueHandlers = new BulkLoadOutputValueHandler[this.bulkOutputFileNumberOfValues];
		this.log.info("Bulk output file will have {} values", this.bulkOutputFileNumberOfValues);
		this.createOutputValueHandlers(routeIdentifier);
		this.outputDelimiter = factFeed.getDelimiterString();
	}

	private void validate() {
		if (this.getFactFeed().getBulkDefinition() == null) {
			throw new IllegalArgumentException("Bulk definition not found in configuration");
		}
		if (this.getFactFeed().getBulkDefinition().getBulkLoadFileDefinition() == null) {
			throw new IllegalArgumentException("Bulk load file definition not found in configuration file. Please check your configuration!");
		}
		if (this.getFactFeed().getBulkDefinition().getBulkLoadFileDefinition().getAttributes() == null
				|| this.getFactFeed().getBulkDefinition().getBulkLoadFileDefinition().getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any defined bulk output attributes");
		}
	}

	private void createOutputValueHandlers(final String routeIdentifier) {
		final String[] bulkOutputAttributeNames = HeaderParsingUtil.getAttributeNames(this.getFactFeed().getBulkDefinition()
				.getBulkLoadFileDefinition().getAttributes());
		if (this.log.isDebugEnabled()) {
			this.log.debug("Bulk output attributes are {}", Arrays.toString(bulkOutputAttributeNames));
		}
		final String firstStringInEveryLine = this.getFactFeed().getData().getEachLineStartsWithCharacter();
		int feedDataLineOffset = 0;
		if (!StringUtil.isEmpty(firstStringInEveryLine)) {
			feedDataLineOffset = 1;
		}
		// one handler per dimension only
		final Map<String, DimensionHandler> cachedDimensionHandlers = new HashMap<String, DimensionHandler>();
		for (int i = 0; i < bulkOutputAttributeNames.length; i++) {
			final String bulkOutputAttributeName = bulkOutputAttributeNames[i];
			if (bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
				final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
				this.log.debug("Searching for configured dimension by name [{}]", requiredDimensionName);
				final DimensionHandler cachedHandler = cachedDimensionHandlers.get(requiredDimensionName);
				if (cachedHandler != null) {
					this.outputValueHandlers[i] = cachedHandler;
				} else {
					Dimension dim = null;
					for (final Dimension d : this.getConfig().getDimensions()) {
						if (requiredDimensionName.equalsIgnoreCase(d.getName())) {
							dim = d;
							break;
						}
					}
					if (dim == null) {
						throw new IllegalArgumentException("Was not able to find dimension definition for dimension with name ["
								+ requiredDimensionName + "]");
					}
					final DimensionHandler dimHandler = new DimensionHandler(dim, this.getFactFeed(), this.cacheInstanceManager.getCacheInstance(dim
							.getName()), this.dbHandler, feedDataLineOffset, routeIdentifier);
					cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
					this.outputValueHandlers[i] = dimHandler;
				}
				this.log.debug("Value at position {} in bulk output load will be mapped using dimension {}", i, requiredDimensionName);
			} else if (bulkOutputAttributeName.startsWith(FEED_PREFIX)) {
				final String requiredFeedAttributeName = bulkOutputAttributeName.replace(FEED_PREFIX, "");
				this.log.debug("Searching for feed attribute {}", requiredFeedAttributeName);
				final String[] feedAttributeNames = HeaderParsingUtil.getAttributeNames(this.getFactFeed().getData().getAttributes());
				int foundPosition = -1;
				for (int k = 0; k < feedAttributeNames.length; k++) {
					final String feedAttrName = feedAttributeNames[k];
					if (requiredFeedAttributeName.equalsIgnoreCase(feedAttrName)) {
						this.log.debug("Found feed attribute {} at position {}", feedAttrName, k);
						foundPosition = k;
					}
				}
				if (foundPosition < 0) {
					throw new IllegalArgumentException("Was not able to find feed attribute " + requiredFeedAttributeName
							+ " in feed definition and this attribute was defined in bulk output file definition. Check config file!");
				}
				this.outputValueHandlers[i] = new PositionalMappingHandler(foundPosition + feedDataLineOffset, feedAttributeNames.length);
				this.log.debug(
						"Value at position {} in bulk output load will be copied directly from value in feed, position {}. Every data line in feed must have {} values",
						i, foundPosition, feedAttributeNames.length);
			} else if (bulkOutputAttributeName.startsWith(Constants.HEADER_ATTRIBUTE_PREFIX)
					|| bulkOutputAttributeName.startsWith(Constants.GLOBAL_ATTRIBUTE_PREFIX)) {
				final ConstantMappingHandler cmh = new ConstantMappingHandler(bulkOutputAttributeName);
				this.outputValueHandlers[i] = cmh;
				this.log.debug("Value at position {} in bulk output load will be constant value derived from {}", i, bulkOutputAttributeName);
			} else {
				throw new IllegalArgumentException("Unknown type of bulk output attribute " + bulkOutputAttributeName
						+ ". Must be either dimension. feed. header. global.");
			}
		}
	}

	public String resolveValues(final String[] inputValues, final Map<String, String> headerData, final Map<String, String> globalData) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.bulkOutputFileNumberOfValues; i++) {
			if (i != 0) {
				sb.append(this.outputDelimiter);
			}
			final String val = this.outputValueHandlers[i].getBulkLoadValue(inputValues, headerData, globalData);
			sb.append(val);
		}
		return sb.toString();
	}

}
