package com.threeglav.bauk.feed;

import gnu.trove.map.hash.THashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.dimension.CachePreviouslyUsedValuesPerThreadDimensionHandler;
import com.threeglav.bauk.dimension.ConstantOutputValueHandler;
import com.threeglav.bauk.dimension.DimensionHandler;
import com.threeglav.bauk.dimension.GlobalAttributeMappingHandler;
import com.threeglav.bauk.dimension.PositionalMappingHandler;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.model.BaukAttribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinition;
import com.threeglav.bauk.model.BulkLoadFormatDefinition;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.BaukThreadFactory;
import com.threeglav.bauk.util.StringUtil;

/**
 * This class is single threaded
 * 
 * @author Borisa
 * 
 */
public class BulkOutputValuesResolver extends ConfigAware {

	private static final String DIMENSION_PREFIX = "dimension.";
	private static final String FEED_PREFIX = "feed.";

	private final int bulkOutputFileNumberOfValues;
	private final BulkLoadOutputValueHandler[] outputValueHandlers;
	private final CacheInstanceManager cacheInstanceManager;

	// optimization - only invoke handlers interested in per-feed calculations
	private final boolean hasCloseFeedValueHandlers;
	private final int[] closeValueHandlerPositions;

	// optimization - only invoke handlers interested in per-feed calculations
	private final boolean hasCalculatePerFeedValueHandlers;
	private final int[] perFeedValueHandlerPositions;

	// one handler per dimension only
	static final Map<String, DimensionHandler> cachedDimensionHandlers = new THashMap<String, DimensionHandler>();

	static final Set<String> alreadyStartedCreatingDimensionNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

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
		if (factFeed.getBulkLoadDefinition().getBulkLoadFormatDefinition() != null) {
			bulkOutputFileNumberOfValues = factFeed.getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes().size();
		} else {
			bulkOutputFileNumberOfValues = 0;
		}
		outputValueHandlers = new BulkLoadOutputValueHandler[bulkOutputFileNumberOfValues];
		log.info("Bulk output file will have {} values delimited by {}", bulkOutputFileNumberOfValues, factFeed.getDelimiterString());
		this.createOutputValueHandlers(routeIdentifier);
		final List<Integer> perFeedValueHandlers = new ArrayList<>();
		final List<Integer> closeFeedValueHandlers = new ArrayList<>();
		for (int i = 0; i < outputValueHandlers.length; i++) {
			if (outputValueHandlers[i].hasCalculatePerFeedValues()) {
				perFeedValueHandlers.add(i);
			}
			if (outputValueHandlers[i].closeShouldBeInvoked()) {
				closeFeedValueHandlers.add(i);
			}
		}
		hasCalculatePerFeedValueHandlers = !perFeedValueHandlers.isEmpty();
		if (hasCalculatePerFeedValueHandlers) {
			perFeedValueHandlerPositions = new int[perFeedValueHandlers.size()];
			int counter = 0;
			for (final Integer pos : perFeedValueHandlers) {
				perFeedValueHandlerPositions[counter++] = pos;
			}
		} else {
			perFeedValueHandlerPositions = null;
		}

		hasCloseFeedValueHandlers = !closeFeedValueHandlers.isEmpty();
		if (hasCloseFeedValueHandlers) {
			closeValueHandlerPositions = new int[closeFeedValueHandlers.size()];
			int counter = 0;
			for (final Integer pos : closeFeedValueHandlers) {
				closeValueHandlerPositions[counter++] = pos;
			}
		} else {
			closeValueHandlerPositions = null;
		}
	}

	private void validate() {
		final BulkLoadDefinition bulkDefinition = this.getFactFeed().getBulkLoadDefinition();
		if (bulkDefinition == null) {
			throw new IllegalArgumentException("Bulk definition not found in configuration for feed " + this.getFactFeed().getName());
		}
		final BulkLoadFormatDefinition bulkLoadFormatDefinition = bulkDefinition.getBulkLoadFormatDefinition();
		if (bulkLoadFormatDefinition != null) {
			if (bulkLoadFormatDefinition.getAttributes() == null || bulkLoadFormatDefinition.getAttributes().isEmpty()) {
				throw new IllegalArgumentException("Was not able to find any defined bulk output attributes");
			}
		}
		if (this.getFactFeed().getData() == null) {
			throw new IllegalArgumentException("Did not find any data definition for feed " + this.getFactFeed().getName());
		}
	}

	private void createOutputValueHandlers(final String routeIdentifier) {
		if (bulkOutputFileNumberOfValues == 0) {
			log.info("Could not find any bulk output file attributes for feed {}. Only values from context will be accessible!", this.getFactFeed()
					.getName());
			return;
		}
		final ArrayList<BaukAttribute> bulkOutputAttributes = this.getFactFeed().getBulkLoadDefinition().getBulkLoadFormatDefinition()
				.getAttributes();
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
		this.createDimensionHandlersInParallel(feedDataLineOffset, bulkOutputAttributeNames);
		final Map<String, Integer> feedAttributeNamesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(feedData.getAttributes());
		final int feedDataLineOffsetFinal = feedDataLineOffset;
		for (int i = 0; i < bulkOutputAttributeNames.length; i++) {
			final int bulkOutputHandlerPosition = i;
			this.createOrAssignBulkOutputHandler(routeIdentifier, bulkOutputAttributes, bulkOutputAttributeNames, feedDataLineOffsetFinal,
					feedAttributeNamesAndPositions, bulkOutputHandlerPosition);
		}
	}

	private void createDimensionHandlersInParallel(final int feedDataLineOffset, final String[] bulkOutputAttributeNames) {
		final BaukThreadFactory btf = new BaukThreadFactory("bauk-bulk-handlers", "handler-creator");
		final ExecutorService exec = Executors.newFixedThreadPool(bulkOutputAttributeNames.length, btf);
		final List<Future<Boolean>> futures = new LinkedList<>();
		for (final String bulkOutputAttributeName : bulkOutputAttributeNames) {
			if (!StringUtil.isEmpty(bulkOutputAttributeName) && bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
				final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
				final boolean canStartProcessing = alreadyStartedCreatingDimensionNames.add(requiredDimensionName);
				if (canStartProcessing) {
					log.debug("Starting creation of dimension handler for {}", requiredDimensionName);
					final Future<Boolean> fut = exec.submit(new Callable<Boolean>() {
						@Override
						public Boolean call() throws Exception {
							BulkOutputValuesResolver.this.createAndCacheDimensionHandler(feedDataLineOffset, requiredDimensionName);
							return true;
						}
					});
					futures.add(fut);
				} else {
					log.debug("Someone already started creating dimension handler for {}", requiredDimensionName);
				}
			}
		}
		try {
			for (final Future<Boolean> fut : futures) {
				final Boolean created = fut.get();
				if (!created) {
					throw new IllegalStateException("Unable to create all bulk output handlers!");
				}
			}
			exec.shutdown();
			log.debug("Successfully finished creation of all dimension handlers");
		} catch (final Exception exc) {
			log.error("Exception while waiting for dimension handlers to finish", exc);
			throw new RuntimeException(exc);
		}
	}

	private void createOrAssignBulkOutputHandler(final String routeIdentifier, final ArrayList<BaukAttribute> bulkOutputAttributes,
			final String[] bulkOutputAttributeNames, final int feedDataLineOffset, final Map<String, Integer> feedAttributeNamesAndPositions,
			final int bulkHandlerPosition) {
		final String bulkOutputAttributeName = bulkOutputAttributeNames[bulkHandlerPosition];
		if (StringUtil.isEmpty(bulkOutputAttributeName)) {
			final BaukAttribute attr = bulkOutputAttributes.get(bulkHandlerPosition);
			final String value = attr.getConstantValue();
			outputValueHandlers[bulkHandlerPosition] = new ConstantOutputValueHandler(value);
			log.debug("Value at position {} in bulk output load will be constant value {}", bulkHandlerPosition, value);
		} else if (bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
			final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
			log.debug("Searching for configured dimension by name [{}]", requiredDimensionName);
			final DimensionHandler cachedDimensionHandler = cachedDimensionHandlers.get(requiredDimensionName);
			if (cachedDimensionHandler != null) {
				final boolean cachePerThreadEnabled = cachedDimensionHandler.getDimension().getCachePerThreadEnabled();
				if (cachePerThreadEnabled) {
					final CachePreviouslyUsedValuesPerThreadDimensionHandler proxyDimHandler = new CachePreviouslyUsedValuesPerThreadDimensionHandler(
							cachedDimensionHandler);
					log.debug("For dimension {} caching per thread is enabled!", requiredDimensionName);
					outputValueHandlers[bulkHandlerPosition] = proxyDimHandler;
				} else {
					outputValueHandlers[bulkHandlerPosition] = cachedDimensionHandler;
					log.debug("For dimension {} caching per thread is disabled!");
				}

			} else {
				throw new IllegalStateException("Was not able to find previously cached dimension handler for " + requiredDimensionName);
			}
			log.debug("Value at position {} in bulk output load will be mapped using dimension {}", bulkHandlerPosition, requiredDimensionName);
		} else if (bulkOutputAttributeName.startsWith(FEED_PREFIX)) {
			final String requiredFeedAttributeName = bulkOutputAttributeName.replace(FEED_PREFIX, "");
			log.debug("Searching for feed attribute {}", requiredFeedAttributeName);
			final Integer foundPosition = feedAttributeNamesAndPositions.get(requiredFeedAttributeName);
			if (foundPosition == null) {
				throw new IllegalArgumentException("Was not able to find feed attribute " + requiredFeedAttributeName
						+ " in feed definition and this attribute was defined in bulk output file definition. Check config file!");
			}
			outputValueHandlers[bulkHandlerPosition] = new PositionalMappingHandler(foundPosition + feedDataLineOffset,
					feedAttributeNamesAndPositions.size());
			log.debug(
					"Value at position {} in bulk output load will be copied directly from value in feed at position {}. Every data line in feed must have {} values",
					bulkHandlerPosition, foundPosition, feedAttributeNamesAndPositions.size());
		} else {
			final GlobalAttributeMappingHandler cmh = new GlobalAttributeMappingHandler(bulkOutputAttributeName);
			outputValueHandlers[bulkHandlerPosition] = cmh;
			log.debug("Value at position {} in bulk output load will be mapped value derived from {}", bulkHandlerPosition, bulkOutputAttributeName);
		}
	}

	private void createAndCacheDimensionHandler(final int feedDataLineOffset, final String requiredDimensionName) {
		if (cachedDimensionHandlers.containsKey(requiredDimensionName)) {
			throw new IllegalStateException("Handler for dimension " + requiredDimensionName + " has already been created and cached!");
		}
		final Dimension dim = this.getConfig().getDimensionMap().get(requiredDimensionName);
		if (dim == null) {
			throw new IllegalArgumentException("Was not able to find dimension definition for dimension with name [" + requiredDimensionName
					+ "]. This dimension is used to create bulk output! Please check your configuration!");
		}
		final DimensionHandler dimHandler = new DimensionHandler(dim, this.getFactFeed(), cacheInstanceManager.getCacheInstance(dim.getName()),
				feedDataLineOffset, this.getConfig());
		cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
	}

	public void startFeed(final Map<String, String> globalData) {
		if (hasCalculatePerFeedValueHandlers) {
			if (isDebugEnabled) {
				log.debug("Starting feed with attributes {}", globalData);
			}
			for (int i = 0; i < perFeedValueHandlerPositions.length; i++) {
				final int pos = perFeedValueHandlerPositions[i];
				outputValueHandlers[pos].calculatePerFeedValues(globalData);
			}
			if (isDebugEnabled) {
				log.debug("Started feed. In total have {} dimension handlers. Global attributes {}", outputValueHandlers.length, globalData);
			}
		}
	}

	public final Object[] resolveValues(final String[] inputValues, final Map<String, String> globalData) {
		final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
		for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
			reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData);
		}
		return reusedForPerformanceOutputValues;
	}

	public final Object[] resolveLastLineValues(final String[] inputValues, final Map<String, String> globalData) {
		final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
		for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
			reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getLastLineBulkLoadValue(inputValues, globalData);
		}
		return reusedForPerformanceOutputValues;
	}

	public void closeCurrentFeed() {
		if (hasCloseFeedValueHandlers) {
			for (int i = 0; i < closeValueHandlerPositions.length; i++) {
				final int pos = closeValueHandlerPositions[i];
				outputValueHandlers[pos].closeCurrentFeed();
			}
		}
	}

}
