package com.threeglav.sh.bauk.dimension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

public class T1DimensionHandler extends InsertOnlyDimensionHandler {

	private String[] nonNaturalKeyNames;
	private int[] nonNaturalKeyPositionsInFeed;
	private final StatefulAttributeReplacer updateStatementReplacer;
	private final Counter dbAccessUpdateCounter;
	private Map<String, String> naturalKeyToNonNaturalKeyMapping;

	public T1DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(dimension, factFeed, cacheInstance, naturalKeyPositionOffset, config);
		if (dimension.getSqlStatements() != null && !StringUtil.isEmpty(dimension.getSqlStatements().getUpdateSingleRecord())) {
			updateStatementReplacer = new StatefulAttributeReplacer(dimension.getSqlStatements().getUpdateSingleRecord(), dbStringLiteral,
					dbStringEscapeLiteral);
		} else {
			throw new IllegalStateException("T1 dimension " + dimension.getName() + " must have updateSingleRecord statement defined");
		}
		this.checkNoNaturalKeysExist();
		this.findAllNonNaturalKeys();
		dbAccessUpdateCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database updates executed");
	}

	private void checkNoNaturalKeysExist() {
		final int nonNaturalKeys = dimension.getNumberOfNonNaturalKeys();
		if (nonNaturalKeys <= 0) {
			throw new IllegalStateException("Dimension [" + dimension.getName()
					+ "] can not be marked as T1 dimension because it does not have any non-natural keys defined!");
		}
	}

	private void findAllNonNaturalKeys() {
		final int numberOfNonNaturalKeys = dimension.getNumberOfNonNaturalKeys();
		nonNaturalKeyNames = new String[numberOfNonNaturalKeys];
		nonNaturalKeyPositionsInFeed = new int[numberOfNonNaturalKeys];
		log.debug("Calculating non-natural keys position values. Will use offset {}", mappedColumnsPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed().getData()
				.getAttributes());
		for (final MappedColumn nk : dimension.getMappedColumns()) {
			if (nk.isNaturalKey()) {
				continue;
			}
			final String mappedColumnName = nk.getName();
			nonNaturalKeyNames[i] = mappedColumnName;
			log.debug("Trying to find position in feed for non natural key {} for dimension {}", mappedColumnName, dimension.getName());
			final Integer attrPosition = dataAttributesAndPositions.get(mappedColumnName);
			int naturalKeyPositionValue;
			if (attrPosition == null) {
				log.error("Was not able to find mapping for {}.{} in feed. T1 dimensions must have all their non natural keys available in feed!",
						dimension.getName(), mappedColumnName);
				throw new IllegalStateException("T1 dimension " + dimension.getName()
						+ " must have all its non-natural keys available in feed. Was not able to find " + mappedColumnName + " defined in feed!");
			}
			naturalKeyPositionValue = attrPosition + mappedColumnsPositionOffset;
			nonNaturalKeyPositionsInFeed[i++] = naturalKeyPositionValue;
			if (log.isDebugEnabled()) {
				log.debug("Non-natural key column [{}] for dimension {} will be read from feed position {}", new Object[] { mappedColumnName,
						dimension.getName(), naturalKeyPositionValue });
			}
		}
	}

	@Override
	String buildNaturalKeyForCacheLookup(final String[] parsedLine, final Map<String, String> globalAttributes, final StringBuilder sb) {
		this.concatenateAllNaturalKeyValues(parsedLine, sb);
		sb.append(BaukConstants.NATURAL_NON_NATURAL_DELIMITER);
		this.concatenateAllNonNaturalKeyValues(parsedLine, sb);
		return sb.toString();
	}

	private void concatenateAllNaturalKeyValues(final String[] parsedLine, final StringBuilder sb) {
		for (int i = 0; i < naturalKeyPositionsInFeed.length; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			final int key = naturalKeyPositionsInFeed[i];
			try {
				sb.append(parsedLine[key]);
			} catch (final ArrayIndexOutOfBoundsException aioobe) {
				log.error(
						"Tried to get data from input feed, position {} but looks like there is not enough data values in this row. Did you configure footer correctly? Do all rows in input feed have same length?",
						key);
				throw aioobe;
			}
		}
	}

	private void concatenateAllNonNaturalKeyValues(final String[] parsedLine, final StringBuilder sb) {
		for (int i = 0; i < nonNaturalKeyPositionsInFeed.length; i++) {
			if (i != 0) {
				sb.append(BaukConstants.NON_NATURAL_KEY_DELIMITER);
			}
			final int key = nonNaturalKeyPositionsInFeed[i];
			try {
				sb.append(parsedLine[key]);
			} catch (final ArrayIndexOutOfBoundsException aioobe) {
				log.error(
						"Tried to get data from input feed, position {} but looks like there is not enough data values in this row. Did you configure footer correctly? Do all rows in input feed have same length?",
						key);
				throw aioobe;
			}
		}
	}

	@Override
	String buildNaturalKeyForCacheLookup(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		return this.buildNaturalKeyForCacheLookup(parsedLine, globalAttributes, sb);
	}

	private boolean nonNaturalKeysEqualInFeedAndInCache(final String[] parsedLine) {
		final StringBuilder nonNaturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNonNaturalKeyValues(parsedLine, nonNaturalKey);
		final String nonNaturalKeyFromFeed = nonNaturalKey.toString();
		final StringBuilder naturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNaturalKeyValues(parsedLine, naturalKey);
		final String naturalKeyFromFeed = naturalKey.toString();
		final String nonNaturalKeyFromCache = naturalKeyToNonNaturalKeyMapping.get(naturalKeyFromFeed);
		if (isDebugEnabled) {
			log.debug("Comparing non natural keys from feed [{}] with non natural keys found in lookup key [{}]", nonNaturalKeyFromFeed,
					nonNaturalKeyFromCache);
		}
		return nonNaturalKeyFromFeed.equals(nonNaturalKeyFromCache);
	}

	private void updateKeysInCache(final String[] parsedLine) {
		final StringBuilder nonNaturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNonNaturalKeyValues(parsedLine, nonNaturalKey);
		final String nonNaturalKeyFromFeed = nonNaturalKey.toString();
		final StringBuilder naturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNaturalKeyValues(parsedLine, naturalKey);
		final String naturalKeyFromFeed = naturalKey.toString();
		final String nonNaturalKeyFromCache = naturalKeyToNonNaturalKeyMapping.get(naturalKeyFromFeed);
		final String oldLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromCache;
		final Integer oldSurrogateKey = dimensionCache.getSurrogateKeyFromCache(oldLookupKey);
		if (oldSurrogateKey == null) {
			throw new IllegalStateException("Should have found surrogate key for old lookup key " + oldLookupKey);
		}
		dimensionCache.removeFromCache(oldLookupKey);
		final String newLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromFeed;
		dimensionCache.putInCache(newLookupKey, oldSurrogateKey);
	}

	@Override
	Integer getBulkLoadValueByPrecalculatedLookupKey(final String[] parsedLine, final Map<String, String> globalAttributes, final String lookupKey) {
		Integer surrogateKey = null;
		if (lookupKey != null) {
			surrogateKey = dimensionCache.getSurrogateKeyFromCache(lookupKey);
		}
		if (surrogateKey == null) {
			if (isDebugEnabled) {
				log.debug("Did not find surrogate key for [{}] in cache, dimension {}. Going to database", lookupKey, dimension.getName());
			}
			final boolean shouldDoUpdate = !this.nonNaturalKeysEqualInFeedAndInCache(parsedLine);
			if (shouldDoUpdate) {
				final String preparedUpdateStatement = this.prepareStatement(parsedLine, globalAttributes, updateStatementReplacer);
				this.tryUpdateStatement(preparedUpdateStatement);
				this.updateKeysInCache(parsedLine);
				surrogateKey = dimensionCache.getSurrogateKeyFromCache(lookupKey);
				if (surrogateKey == null) {
					throw new IllegalStateException("Performed update operation but still was not able to find surrogate key for lookup ["
							+ lookupKey + "]");
				}
				if (isDebugEnabled) {
					log.debug("Surrogate key after update is {}. Lookup key is {}", surrogateKey, lookupKey);
				}
			} else {
				surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes, lookupKey);
			}
			if (!noNaturalKeyColumnsDefined && lookupKey != null) {
				dimensionCache.putInCache(lookupKey, surrogateKey);
			}
		}
		if (isTraceEnabled) {
			log.trace("Found surrogate key {} for natural key {} for dimension {}", surrogateKey, lookupKey, dimension.getName());
		}
		return surrogateKey;
	}

	private void tryUpdateStatement(final String preparedStatement) {
		this.getDbHandler().executeInsertOrUpdateStatement(preparedStatement, dimension.getName());
		if (dbAccessUpdateCounter != null) {
			dbAccessUpdateCounter.inc();
		}
	}

	@Override
	protected void processReturnedPreCacheValues(final List<DimensionKeysPair> retrievedValues) {
		naturalKeyToNonNaturalKeyMapping = new ConcurrentHashMap<>();
		for (final DimensionKeysPair dkp : retrievedValues) {
			naturalKeyToNonNaturalKeyMapping.put(dkp.naturalKeyOnly, dkp.nonNaturalKeyOnly);
		}
	}

}
