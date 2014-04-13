package com.threeglav.sh.bauk.dimension;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.DimensionType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

public class T1DimensionHandler extends InsertOnlyDimensionHandler {

	private static final int MAX_NUMBER_OF_LOCKS = 20;
	private final Object[] LOCKS = new Object[MAX_NUMBER_OF_LOCKS];

	private String[] nonNaturalKeyNames;
	private int[] nonNaturalKeyPositionsInFeed;
	private StatefulAttributeReplacer updateStatementReplacer;
	private final Counter dbAccessUpdateCounter;
	protected Map<String, String> naturalKeyToNonNaturalKeyMapping;

	public T1DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(dimension, factFeed, cacheInstance, naturalKeyPositionOffset, config);
		if (dimension.getSqlStatements() != null && !StringUtil.isEmpty(dimension.getSqlStatements().getUpdateSingleRecord())) {
			updateStatementReplacer = new StatefulAttributeReplacer(dimension.getSqlStatements().getUpdateSingleRecord(), dbStringLiteral,
					dbStringEscapeLiteral);
		} else if (dimension.getType() == DimensionType.T1) {
			throw new IllegalStateException(dimension.getType() + " dimension " + dimension.getName()
					+ " must have updateSingleRecord statement defined");
		}
		this.checkNoNaturalKeysExist();
		this.findAllNonNaturalKeys();
		dbAccessUpdateCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database updates executed");
		this.initializeLocks();
	}

	private void initializeLocks() {
		for (int i = 0; i < MAX_NUMBER_OF_LOCKS; i++) {
			LOCKS[i] = new Object();
		}
	}

	private Object getLockForLookupKey(final String lookupKey) {
		final int lockPosition = lookupKey.hashCode() % MAX_NUMBER_OF_LOCKS;
		return LOCKS[Math.abs(lockPosition)];
	}

	@Override
	protected DimensionCache initializeDimensionCache(final CacheInstance cacheInstance, final Dimension dimension) {
		return new DimensionCacheMapImpl(cacheInstance, dimension);
	}

	protected void checkNoNaturalKeysExist() {
		final int nonNaturalKeys = dimension.getNumberOfNonNaturalKeys();
		if (nonNaturalKeys <= 0) {
			throw new IllegalStateException("Dimension [" + dimension.getName() + "] can not be marked as " + dimension.getType()
					+ " dimension because it does not have any non-natural keys defined!");
		}
	}

	protected void findAllNonNaturalKeys() {
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
				throw new IllegalStateException(dimension.getType() + " dimension " + dimension.getName()
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

	protected void concatenateAllNaturalKeyValues(final String[] parsedLine, final StringBuilder sb) {
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

	protected void concatenateAllNonNaturalKeyValues(final String[] parsedLine, final StringBuilder sb) {
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

	private static enum NaturalKeyComparisonResult {
		EQUAL, NOT_EQUAL, KEY_NOT_FOUND;
	}

	private NaturalKeyComparisonResult nonNaturalKeysEqualInFeedAndInCache(final String[] parsedLine) {
		final StringBuilder nonNaturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNonNaturalKeyValues(parsedLine, nonNaturalKey);
		final String nonNaturalKeyFromFeed = nonNaturalKey.toString();
		final StringBuilder naturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNaturalKeyValues(parsedLine, naturalKey);
		final String naturalKeyFromFeed = naturalKey.toString();
		final String nonNaturalKeyFromCache = naturalKeyToNonNaturalKeyMapping.get(naturalKeyFromFeed);
		if (nonNaturalKeyFromCache == null) {
			if (isDebugEnabled) {
				log.debug("Could not find non natural key in cache for natural key {}", naturalKeyFromFeed);
			}
			return NaturalKeyComparisonResult.KEY_NOT_FOUND;
		}
		if (isDebugEnabled) {
			log.debug("Comparing non natural keys from feed [{}] with non natural keys found in lookup key [{}]", nonNaturalKeyFromFeed,
					nonNaturalKeyFromCache);
		}
		final boolean keysAreSame = nonNaturalKeyFromFeed.equals(nonNaturalKeyFromCache);
		if (keysAreSame) {
			return NaturalKeyComparisonResult.EQUAL;
		} else {
			return NaturalKeyComparisonResult.NOT_EQUAL;
		}
	}

	private Integer updateKeysInCache(final String[] parsedLine) {
		final StringBuilder nonNaturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNonNaturalKeyValues(parsedLine, nonNaturalKey);
		final String nonNaturalKeyFromFeed = nonNaturalKey.toString();
		final StringBuilder naturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNaturalKeyValues(parsedLine, naturalKey);
		final String naturalKeyFromFeed = naturalKey.toString();
		final String nonNaturalKeyFromCache = naturalKeyToNonNaturalKeyMapping.get(naturalKeyFromFeed);
		final String oldLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromCache;
		naturalKeyToNonNaturalKeyMapping.put(naturalKeyFromFeed, nonNaturalKeyFromFeed);
		final Integer oldSurrogateKey = dimensionCache.getSurrogateKeyFromCache(oldLookupKey);
		dimensionCache.removeFromCache(oldLookupKey);
		if (oldSurrogateKey != null) {
			final String newLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromFeed;
			dimensionCache.putInCache(newLookupKey, oldSurrogateKey);
		}
		return oldSurrogateKey;
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
			final NaturalKeyComparisonResult keyComparison = this.nonNaturalKeysEqualInFeedAndInCache(parsedLine);
			final boolean shouldDoUpdate = (keyComparison == NaturalKeyComparisonResult.NOT_EQUAL);
			if (shouldDoUpdate) {
				final Object lock = this.getLockForLookupKey(lookupKey);
				synchronized (lock) {
					surrogateKey = this.doPerformRecordUpdate(parsedLine, globalAttributes, lookupKey);
				}
			} else {
				surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes, lookupKey);
				if (keyComparison == NaturalKeyComparisonResult.KEY_NOT_FOUND) {
					this.updateKeysInCache(parsedLine);
				}
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

	protected Integer doPerformRecordUpdate(final String[] parsedLine, final Map<String, String> globalAttributes, final String lookupKey) {
		final String preparedUpdateStatement = this.prepareStatement(parsedLine, globalAttributes, updateStatementReplacer);
		final int updatedRows = this.getDbHandler().executeInsertOrUpdateStatement(preparedUpdateStatement, dimension.getName());
		if (dbAccessUpdateCounter != null) {
			dbAccessUpdateCounter.inc();
		}
		final Integer surrogateKey = this.updateKeysInCache(parsedLine);
		if (surrogateKey == null) {
			if (updatedRows == 1) {
				if (isDebugEnabled) {
					log.debug(
							"Updated 1 row in database but could not find key in cache. Will perform database lookup. Lookup key = [{}], line = [{}]",
							lookupKey, Arrays.toString(parsedLine));
				}
				final Integer surrogateKeyInDb = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes, lookupKey);
				if (surrogateKeyInDb != null) {
					this.updateKeysInCache(parsedLine);
					return surrogateKeyInDb;
				} else {
					log.error(
							"Was not able to find surrogate key in database, even after update statement {} successfully updated 1 row in database. Parsed line is {}",
							preparedUpdateStatement, Arrays.toString(parsedLine));
					throw new IllegalStateException(
							"Performed update operation and lookup in database but still was not able to find surrogate key for lookup [" + lookupKey
									+ "]");
				}
			} else {
				log.error(
						"Could not find surrogate key after updating cache. Update statement [{}] updated in total {} rows in database. Parsed line was {}",
						preparedUpdateStatement, updatedRows, Arrays.toString(parsedLine));
				throw new IllegalStateException("Performed update operation but still was not able to find surrogate key for lookup [" + lookupKey
						+ "]");
			}
		}
		if (isDebugEnabled) {
			log.debug("Surrogate key after update is {}. Lookup key is {}", surrogateKey, lookupKey);
		}
		return surrogateKey;
	}

	@Override
	protected void processReturnedPreCacheValues(final List<DimensionKeysPair> retrievedValues) {
		naturalKeyToNonNaturalKeyMapping = new ConcurrentHashMap<>();
		for (final DimensionKeysPair dkp : retrievedValues) {
			naturalKeyToNonNaturalKeyMapping.put(dkp.naturalKeyOnly, dkp.nonNaturalKeyOnly);
		}
	}

}
