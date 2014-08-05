package com.threeglav.sh.bauk.dimension;

import java.util.Arrays;
import java.util.Map;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public abstract class AbstractDimensionHandler extends ConfigAware implements DimensionHandler {

	protected final Dimension dimension;
	protected String[] mappedColumnNames;
	protected int[] mappedColumnsPositionsInFeed;
	protected final DimensionCache dimensionCache;
	protected final int mappedColumnsPositionOffset;
	private final boolean hasNaturalKeysNotPresentInFeed;
	private final boolean hasOnlyOneNaturalKeyDefinedForLookup;
	protected int[] naturalKeyPositionsInFeed;
	protected String[] naturalKeyNames;
	protected final boolean noNaturalKeyColumnsDefined;

	public AbstractDimensionHandler(final Feed factFeed, final BaukConfiguration config, final Dimension dimension,
			final int naturalKeyPositionOffset, final CacheInstance cacheInstance) {
		super(factFeed, config);
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		if (cacheInstance == null) {
			throw new IllegalArgumentException("Cache instance must not be null");
		}
		mappedColumnsPositionOffset = naturalKeyPositionOffset;
		hasNaturalKeysNotPresentInFeed = this.calculatePositionOfNaturalKeyValues();
		hasOnlyOneNaturalKeyDefinedForLookup = naturalKeyPositionsInFeed.length == 1;
		final int numberOfNaturalKeys = this.dimension.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys == 0) {
			noNaturalKeyColumnsDefined = true;
			log.warn("Did not find any defined natural keys for {}. Will disable any caching of data for this dimension!", dimension.getName());
		} else {
			noNaturalKeyColumnsDefined = false;
			log.debug("Caching for dimension {} is enabled", dimension.getName());
		}
		dimensionCache = this.initializeDimensionCache(cacheInstance, dimension);
		this.calculatePositionOfMappedColumnValues();
	}

	private void calculatePositionOfMappedColumnValues() {
		final int numberOfMappedColumns = dimension.getMappedColumns().size();
		mappedColumnNames = new String[numberOfMappedColumns];
		mappedColumnsPositionsInFeed = new int[numberOfMappedColumns];
		log.debug("Calculating mapped columns position values. Will use offset {}", mappedColumnsPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed()
				.getSourceFormatDefinition().getData().getAttributes());
		for (final MappedColumn mc : dimension.getMappedColumns()) {
			final String mappedColumnName = mc.getName();
			int mappedColumnPositionValue;
			mappedColumnNames[i] = mappedColumnName;
			log.debug("Trying to find position in feed for mapped column {} for dimension {}", mappedColumnName, dimension.getName());
			final Integer attrPosition = dataAttributesAndPositions.get(mappedColumnName);
			if (attrPosition == null) {
				log.debug("Could not find mapping for {}.{} in feed. Will expect this value to be mapped from global attributes!",
						dimension.getName(), mappedColumnName);
				mappedColumnPositionValue = NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION;
			} else {
				mappedColumnPositionValue = attrPosition + mappedColumnsPositionOffset;
			}
			mappedColumnsPositionsInFeed[i++] = mappedColumnPositionValue;
			if (log.isDebugEnabled()) {
				log.debug("Mapped column [{}] for dimension {} will be read from feed position {}",
						new Object[] { mappedColumnName, dimension.getName(), mappedColumnPositionValue });
			}
		}
	}

	protected DimensionCache initializeDimensionCache(final CacheInstance cacheInstance, final Dimension dimension) {
		log.debug("Initializing cache for dimension {}", dimension.getName());
		return new DimensionCacheTroveImpl(cacheInstance, dimension);
	}

	String buildNaturalKeyForCacheLookup(final String[] parsedLine, final Map<String, String> globalAttributes, final StringBuilder sb) {
		if (hasNaturalKeysNotPresentInFeed) {
			return this.buildNaturalKeyForCacheLookupNotAllNaturalKeysInFeed(parsedLine, globalAttributes, sb);
		} else if (hasOnlyOneNaturalKeyDefinedForLookup) {
			return this.buildNaturalKeyForCacheLookupOnlyOneNaturalKeyUseForLookup(parsedLine);
		} else {
			return this.buildNaturalKeyForCacheLookupAllNaturalKeysInFeed(parsedLine, sb);
		}
	}

	String buildNaturalKeyForCacheLookup(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		return this.buildNaturalKeyForCacheLookup(parsedLine, globalAttributes, sb);
	}

	private final String buildNaturalKeyForCacheLookupOnlyOneNaturalKeyUseForLookup(final String[] parsedLine) {
		final int key = naturalKeyPositionsInFeed[0];
		try {
			return parsedLine[key];
		} catch (final ArrayIndexOutOfBoundsException aioobe) {
			log.error(
					"Tried to get data from input feed, position {} but looks like there is not enough data values in this row. Did you configure footer correctly? Do all rows in input feed have same length?",
					key);
			throw aioobe;
		}
	}

	private final String buildNaturalKeyForCacheLookupAllNaturalKeysInFeed(final String[] parsedLine, final StringBuilder sb) {
		for (int i = 0; i < naturalKeyPositionsInFeed.length; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			final int key = naturalKeyPositionsInFeed[i];
			try {
				sb.append(parsedLine[key]);
			} catch (final ArrayIndexOutOfBoundsException aioobe) {
				log.error(
						"Tried to get data from input feed, position {} but looks like there are only {} values available in this row. Did you configure footer correctly? Do all rows in input feed have same length? Problematic row is [{}]",
						key, parsedLine.length, Arrays.toString(parsedLine));
				throw aioobe;
			}
		}
		return sb.toString();
	}

	private final String buildNaturalKeyForCacheLookupNotAllNaturalKeysInFeed(final String[] parsedLine, final Map<String, String> globalAttributes,
			final StringBuilder sb) {
		for (int i = 0; i < naturalKeyPositionsInFeed.length; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			final int key = naturalKeyPositionsInFeed[i];
			if (key == NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION) {
				final String attributeName = naturalKeyNames[i];
				if (attributeName != null) {
					final String globalAttributeValue = globalAttributes.get(attributeName);
					if (isDebugEnabled) {
						log.debug(
								"Natural key {}.{} is not mapped to any of declared feed attributes. Will use value [{}] found in global attributes",
								dimension.getName(), attributeName, globalAttributeValue);
					}
					sb.append(globalAttributeValue);
				}
			} else {
				sb.append(parsedLine[key]);
			}
		}
		return sb.toString();
	}

	private boolean calculatePositionOfNaturalKeyValues() {
		boolean foundNaturalKeyNotPresentInFeed = false;
		if (noNaturalKeyColumnsDefined) {
			return foundNaturalKeyNotPresentInFeed;
		}
		final int numberOfNaturalKeys = dimension.getNumberOfNaturalKeys();
		naturalKeyNames = new String[numberOfNaturalKeys];
		naturalKeyPositionsInFeed = new int[numberOfNaturalKeys];
		log.debug("Calculating natural keys position values. Will use offset {}", mappedColumnsPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed()
				.getSourceFormatDefinition().getData().getAttributes());
		for (final MappedColumn nk : dimension.getMappedColumns()) {
			if (!nk.isNaturalKey()) {
				continue;
			}
			final String mappedColumnName = nk.getName();
			naturalKeyNames[i] = mappedColumnName;
			log.debug("Trying to find position in feed for natural key {} for dimension {}", mappedColumnName, dimension.getName());
			final Integer attrPosition = dataAttributesAndPositions.get(mappedColumnName);
			int naturalKeyPositionValue;
			if (attrPosition == null) {
				naturalKeyPositionValue = NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION;
				foundNaturalKeyNotPresentInFeed = true;
				log.debug("Was not able to find mapping for {}.{} in feed. Will expect this value to be found in global attributes",
						dimension.getName(), mappedColumnName);
			} else {
				naturalKeyPositionValue = attrPosition + mappedColumnsPositionOffset;
			}
			naturalKeyPositionsInFeed[i++] = naturalKeyPositionValue;
			if (log.isDebugEnabled()) {
				log.debug("Mapped column [{}] for dimension {} will be read from feed position {}",
						new Object[] { mappedColumnName, dimension.getName(), naturalKeyPositionValue });
			}
		}
		return foundNaturalKeyNotPresentInFeed;
	}

	@Override
	public Dimension getDimension() {
		return dimension;
	}

	@Override
	public final int[] getMappedColumnPositionsInFeed() {
		return mappedColumnsPositionsInFeed;
	}

	String[] getMappedColumnNames() {
		return mappedColumnNames;
	}

	/*
	 * used for testing
	 */

	String[] getNaturalKeyNames() {
		return naturalKeyNames;
	}

	int[] getNaturalKeyPositionsInFeed() {
		return naturalKeyPositionsInFeed;
	}

}
