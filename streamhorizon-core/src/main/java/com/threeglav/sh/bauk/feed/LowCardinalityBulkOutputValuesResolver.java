package com.threeglav.sh.bauk.feed;

import gnu.trove.map.hash.THashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BulkLoadOutputValueHandler;
import com.threeglav.sh.bauk.dimension.DimensionHandler;
import com.threeglav.sh.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.ArrayUtil;

public class LowCardinalityBulkOutputValuesResolver extends BulkOutputValuesResolver {

	private static final int MAX_COMBINED_LOOKUP_MAP_SIZE = 100000;

	private final StringBuilder reusedForPerformance = new StringBuilder(150);

	private final THashMap<String, Object[]> combinedLookupValues = new THashMap<String, Object[]>(MAX_COMBINED_LOOKUP_MAP_SIZE);

	private int[][] dimensionFeedLocationMapping;
	private int[] flattenedDimensionFeedLocationMapping;
	private final boolean shouldTryCombinedLookups;
	private boolean[] isPositionUsedInCombinedLookup;

	public LowCardinalityBulkOutputValuesResolver(final Feed factFeed, final BaukConfiguration config,
			final CacheInstanceManager cacheInstanceManager) {
		super(factFeed, config, cacheInstanceManager);
		this.prepareAllGoodDimensions();
		shouldTryCombinedLookups = dimensionFeedLocationMapping != null && dimensionFeedLocationMapping.length > 1;
		if (shouldTryCombinedLookups) {
			flattenedDimensionFeedLocationMapping = ArrayUtil.flattenArray(dimensionFeedLocationMapping);
		}
	}

	private boolean isLowCardinalityFeedOnlyDimension(final DimensionHandler dimHandler) {
		final boolean isLowCardinality = dimHandler.getDimension().getUseInCombinedLookup();
		boolean hasAllDataInFeed = true;
		for (final int m : dimHandler.getMappedColumnPositionsInFeed()) {
			if (m == DimensionHandler.NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION) {
				hasAllDataInFeed = false;
				break;
			}
		}
		if (isLowCardinality && !hasAllDataInFeed) {
			log.warn(
					"Dimension {} is marked as low cardinality but not all of its data is coming from input feed. This dimension can not be used for combined lookup optimization",
					dimHandler.getDimension().getName());
		}
		return isLowCardinality && hasAllDataInFeed;
	}

	private List<DimensionHandler> collectAllDimensionHandlers() {
		final List<DimensionHandler> handlers = new LinkedList<>();
		isPositionUsedInCombinedLookup = new boolean[outputValueHandlers.length];
		int counter = 0;
		for (final BulkLoadOutputValueHandler handler : outputValueHandlers) {
			isPositionUsedInCombinedLookup[counter] = false;
			if (handler instanceof DimensionHandler) {
				final DimensionHandler dh = (DimensionHandler) handler;
				if (this.isLowCardinalityFeedOnlyDimension(dh)) {
					isPositionUsedInCombinedLookup[counter] = true;
					handlers.add(dh);
				}
			}
			counter++;
		}
		return handlers;
	}

	private void prepareAllGoodDimensions() {
		final Collection<DimensionHandler> dimHandlers = this.collectAllDimensionHandlers();
		final List<DimensionHandler> allGoodDimensions = new LinkedList<>();
		for (final DimensionHandler h : dimHandlers) {
			final boolean isGood = this.isLowCardinalityFeedOnlyDimension(h);
			if (isGood) {
				log.info("Dimension {} is low cardinality and will be considered for combined lookup optimization", h.getDimension().getName());
				allGoodDimensions.add(h);
			}
		}
		final int count = allGoodDimensions.size();
		if (count > 1) {
			log.info("Found {} dimensions that can be used for combined lookup optimization. Will optimize!", count);
			dimensionFeedLocationMapping = new int[count][];
			for (int i = 0; i < count; i++) {
				final DimensionHandler dh = allGoodDimensions.get(i);
				dimensionFeedLocationMapping[i] = dh.getMappedColumnPositionsInFeed();
			}
		} else {
			log.info("Found only one dimension that can be used for combined lookup optimization. Will not optimize!");
		}
	}

	@Override
	public final Object[] resolveValues(final String[] inputValues, final Map<String, String> globalData) {
		if (!shouldTryCombinedLookups) {
			return super.resolveValues(inputValues, globalData);
		} else {
			final String combinedLookupKey = this.createCombinedLookupValue(inputValues);
			final Object[] cached = combinedLookupValues.get(combinedLookupKey);
			if (cached != null) {
				if (isDebugEnabled) {
					log.debug("Found combined lookup cached value for {}", combinedLookupKey);
				}
				for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
					if (!isPositionUsedInCombinedLookup[i]) {
						cached[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData);
					}
				}
				return cached;
			} else {
				if (isDebugEnabled) {
					log.debug("Did not find combined lookup cached value for {}. Will create it and cache it", combinedLookupKey);
				}
				final Object[] dataToCache = new Object[bulkOutputFileNumberOfValues];
				for (int i = 0; i < bulkOutputFileNumberOfValues; i++) {
					dataToCache[i] = outputValueHandlers[i].getBulkLoadValue(inputValues, globalData);
				}
				combinedLookupValues.put(combinedLookupKey, dataToCache);
				if (isDebugEnabled) {
					log.debug("Cached {}={}. Total size of map is {}", combinedLookupKey, Arrays.toString(dataToCache), combinedLookupValues.size());
				}
				return dataToCache;
			}
		}
	}

	private String createCombinedLookupValue(final String[] parsedValues) {
		reusedForPerformance.setLength(0);
		for (int i = 0; i < flattenedDimensionFeedLocationMapping.length; i++) {
			reusedForPerformance.append(BaukConstants.NATURAL_KEY_DELIMITER);
			reusedForPerformance.append(parsedValues[i]);
		}
		return reusedForPerformance.toString();
	}

	@Override
	public void closeCurrentFeed() {
		super.closeCurrentFeed();
		if (combinedLookupValues.size() > MAX_COMBINED_LOOKUP_MAP_SIZE) {
			combinedLookupValues.clear();
		}
	}

}
