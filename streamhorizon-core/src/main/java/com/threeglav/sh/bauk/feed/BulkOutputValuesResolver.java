package com.threeglav.sh.bauk.feed;

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
import java.util.concurrent.atomic.AtomicInteger;

import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.dimension.CachePreviouslyUsedValuesPerThreadDimensionHandler;
import com.threeglav.sh.bauk.dimension.ConstantOutputValueHandler;
import com.threeglav.sh.bauk.dimension.GlobalAttributeMappingHandler;
import com.threeglav.sh.bauk.dimension.InsertOnlyDimensionHandler;
import com.threeglav.sh.bauk.dimension.PositionalMappingHandler;
import com.threeglav.sh.bauk.dimension.T1DimensionHandler;
import com.threeglav.sh.bauk.dimension.T2DimensionHandler;
import com.threeglav.sh.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.BulkLoadDefinition;
import com.threeglav.sh.bauk.model.BulkLoadFormatDefinition;
import com.threeglav.sh.bauk.model.Data;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.DimensionType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.BaukThreadFactory;
import com.threeglav.sh.bauk.util.StringUtil;

/**
 * This class is single threaded
 * 
 * @author Borisa
 * 
 */
public class BulkOutputValuesResolver extends ConfigAware {

	private static final AtomicInteger RESOLVER_COUNTER = new AtomicInteger(0);

	private static final String DIMENSION_PREFIX = "dimension.";
	private static final String FEED_PREFIX = "feed.";

	protected final int bulkOutputFileNumberOfValues;
	protected final BulkLoadOutputValueHandler[] outputValueHandlers;
	private final CacheInstanceManager cacheInstanceManager;

	// optimization - only invoke handlers interested in per-feed calculations
	private final boolean hasCloseFeedValueHandlers;
	private final int[] closeValueHandlerPositions;

	private final int uniqueResolverIdentifier;

	// we want to reduce a chance that all threads try to resolve same values from database at the same time
	// therefore some threads will resolve values in reverse order, thus reducing a change to double check database
	// for the same value at the same time
	private final boolean reverseResolution;

	// one handler per dimension only
	static final Map<String, InsertOnlyDimensionHandler> cachedDimensionHandlers = new THashMap<String, InsertOnlyDimensionHandler>();

	static final Set<String> alreadyStartedCreatingDimensionNames = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public BulkOutputValuesResolver(final Feed factFeed, final BaukConfiguration config, final CacheInstanceManager cacheInstanceManager) {
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
		this.createOutputValueHandlers();
		final List<Integer> closeFeedValueHandlers = new ArrayList<>();
		for (int i = 0; i < outputValueHandlers.length; i++) {
			if (outputValueHandlers[i].closeShouldBeInvoked()) {
				closeFeedValueHandlers.add(i);
			}
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
		uniqueResolverIdentifier = RESOLVER_COUNTER.incrementAndGet();
		reverseResolution = (uniqueResolverIdentifier % 2 == 0);
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

	private void createOutputValueHandlers() {
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
			this.createOrAssignBulkOutputHandler(bulkOutputAttributes, bulkOutputAttributeNames, feedDataLineOffsetFinal,
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

	private boolean shouldCachePerThreadValues(final InsertOnlyDimensionHandler cachedDimensionHandler) {
		final Dimension dimension = cachedDimensionHandler.getDimension();
		boolean cachePerThreadEnabled = dimension.getCachePerThreadEnabled();
		final DimensionType type = dimension.getType();
		if (type == DimensionType.T1 || type == DimensionType.T2) {
			if (cachePerThreadEnabled) {
				log.warn("Dimensions {} is of type {} which does not allow caching values per thread. Will disable this feature for this dimension!",
						dimension.getName(), dimension.getType());
			}
			cachePerThreadEnabled = false;
		}
		return cachePerThreadEnabled;
	}

	private BulkLoadOutputValueHandler getDimensionHandler(final String bulkOutputAttributeName, final int bulkHandlerPosition) {
		final String requiredDimensionName = bulkOutputAttributeName.replace(DIMENSION_PREFIX, "");
		log.debug("Searching for configured dimension by name [{}]", requiredDimensionName);
		final InsertOnlyDimensionHandler cachedDimensionHandler = cachedDimensionHandlers.get(requiredDimensionName);
		BulkLoadOutputValueHandler dimensionHandler;
		if (cachedDimensionHandler != null) {
			final boolean cachePerThreadEnabled = this.shouldCachePerThreadValues(cachedDimensionHandler);
			if (cachePerThreadEnabled) {
				final CachePreviouslyUsedValuesPerThreadDimensionHandler proxyDimHandler = new CachePreviouslyUsedValuesPerThreadDimensionHandler(
						cachedDimensionHandler);
				log.debug("For dimension {} caching per thread is enabled!", requiredDimensionName);
				dimensionHandler = proxyDimHandler;
			} else {
				log.debug("For dimension {} caching per thread is disabled!");
				dimensionHandler = cachedDimensionHandler;
			}
		} else {
			throw new IllegalStateException("Was not able to find previously cached dimension handler for " + requiredDimensionName);
		}
		log.debug("Value at position {} in bulk output load will be mapped using dimension {}", bulkHandlerPosition, requiredDimensionName);
		return dimensionHandler;
	}

	private void createOrAssignBulkOutputHandler(final ArrayList<BaukAttribute> bulkOutputAttributes, final String[] bulkOutputAttributeNames,
			final int feedDataLineOffset, final Map<String, Integer> feedAttributeNamesAndPositions, final int bulkHandlerPosition) {
		final String bulkOutputAttributeName = bulkOutputAttributeNames[bulkHandlerPosition];
		if (StringUtil.isEmpty(bulkOutputAttributeName)) {
			final BaukAttribute attr = bulkOutputAttributes.get(bulkHandlerPosition);
			final String value = attr.getConstantValue();
			outputValueHandlers[bulkHandlerPosition] = new ConstantOutputValueHandler(value);
			log.debug("Value at position {} in bulk output load will be constant value {}", bulkHandlerPosition, value);
		} else if (bulkOutputAttributeName.startsWith(DIMENSION_PREFIX)) {
			outputValueHandlers[bulkHandlerPosition] = this.getDimensionHandler(bulkOutputAttributeName, bulkHandlerPosition);
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
		if (dim.getType() == DimensionType.INSERT_ONLY) {
			log.debug("Dimension {} is type INSERT_ONLY", requiredDimensionName);
			final InsertOnlyDimensionHandler dimHandler = new InsertOnlyDimensionHandler(dim, this.getFactFeed(),
					cacheInstanceManager.getCacheInstance(dim.getName()), feedDataLineOffset, this.getConfig());
			cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
		} else if (dim.getType() == DimensionType.T1) {
			log.debug("Dimension {} is type T1", requiredDimensionName);
			final T1DimensionHandler dimHandler = new T1DimensionHandler(dim, this.getFactFeed(),
					cacheInstanceManager.getCacheInstance(dim.getName()), feedDataLineOffset, this.getConfig());
			cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
		} else if (dim.getType() == DimensionType.T2) {
			log.debug("Dimension {} is type T2", requiredDimensionName);
			final T2DimensionHandler dimHandler = new T2DimensionHandler(dim, this.getFactFeed(),
					cacheInstanceManager.getCacheInstance(dim.getName()), feedDataLineOffset, this.getConfig());
			cachedDimensionHandlers.put(requiredDimensionName, dimHandler);
		} else {
			throw new IllegalStateException("Was not able to find type for dimension " + requiredDimensionName);
		}
	}

	/**
	 * Resolves values from dimensions. Returned object MUST NOT be modified in any way by users.
	 * 
	 * @param inputValues
	 * @param globalData
	 * @return
	 */
	public Object[] resolveValues(final String[] inputValues, final Map<String, String> globalData) {
		if (reverseResolution) {
			final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
			for (int i = bulkOutputFileNumberOfValues - 1; i >= 0; i--) {
				reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData);
			}
			return reusedForPerformanceOutputValues;
		} else {
			final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
			for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
				reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData);
			}
			return reusedForPerformanceOutputValues;
		}
	}

	public final Object[] resolveLastLineValues(final String[] inputValues, final Map<String, String> globalData) {
		if (reverseResolution) {
			final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
			for (int i = bulkOutputFileNumberOfValues - 1; i >= 0; i--) {
				reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getLastLineBulkLoadValue(inputValues, globalData);
			}
			return reusedForPerformanceOutputValues;
		} else {
			final Object[] reusedForPerformanceOutputValues = new Object[bulkOutputFileNumberOfValues];
			for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
				reusedForPerformanceOutputValues[i] = outputValueHandlers[i].getLastLineBulkLoadValue(inputValues, globalData);
			}
			return reusedForPerformanceOutputValues;
		}
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
