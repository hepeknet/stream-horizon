package com.threeglav.sh.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dimension.db.InsertOnlyDimensionKeysRowMapper;
import com.threeglav.sh.bauk.dimension.db.T1DimensionKeysRowMapper;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.DimensionType;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

/**
 * One instance per dimension. Must be thread-safe.
 * 
 * @author Borisa
 * 
 */
public class InsertOnlyDimensionHandler extends ConfigAware implements DimensionHandler {

	private static final String DIMENSION_SK_SUFFIX = ".sk";

	private static final int NUMBER_OF_PRE_CACHED_ROWS_WARNING = 500000;

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final int PRE_CACHE_EXECUTION_WARNING = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
			BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);

	private final boolean dimensionPrecachingDisabled;

	private final String dimensionLastLineSKAttributeName;
	protected final Dimension dimension;
	private String[] mappedColumnNames;
	private int[] mappedColumnsPositionsInFeed;
	private String[] naturalKeyNames;
	protected int[] naturalKeyPositionsInFeed;
	private final Counter dbAccessSelectCounter;
	private final Counter dbAccessInsertCounter;
	private final Counter dbAccessPreCachedValuesCounter;
	protected final int mappedColumnsPositionOffset;
	protected final String dbStringLiteral;
	protected final String dbStringEscapeLiteral;
	protected final boolean noNaturalKeyColumnsDefined;
	protected final DimensionCache dimensionCache;
	private final boolean hasNaturalKeysNotPresentInFeed;
	private final boolean exposeLastLineValueInContext;
	private final StatefulAttributeReplacer insertStatementReplacer;
	private final StatefulAttributeReplacer selectStatementReplacer;
	private final boolean hasOnlyOneNaturalKeyDefinedForLookup;

	public InsertOnlyDimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(factFeed, config);
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		if (cacheInstance == null) {
			throw new IllegalArgumentException("Cache instance must not be null");
		}
		dimensionCache = this.initializeDimensionCache(cacheInstance, dimension);
		if (naturalKeyPositionOffset < 0) {
			throw new IllegalArgumentException("Natural key position offset must not be negative number");
		}
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		dbStringEscapeLiteral = this.getConfig().getDatabaseStringEscapeLiteral();
		exposeLastLineValueInContext = dimension.getExposeLastLineValueInContext();
		this.validate();
		mappedColumnsPositionOffset = naturalKeyPositionOffset;
		this.calculatePositionOfMappedColumnValues();
		hasNaturalKeysNotPresentInFeed = this.calculatePositionOfNaturalKeyValues();
		hasOnlyOneNaturalKeyDefinedForLookup = naturalKeyPositionsInFeed.length == 1;
		dbAccessSelectCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database selects executed");
		dbAccessInsertCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database inserts executed");
		dbAccessPreCachedValuesCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total pre-cached values retrieved");
		final int numberOfNaturalKeys = this.dimension.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys == 0) {
			noNaturalKeyColumnsDefined = true;
			log.warn("Did not find any defined natural keys for {}. Will disable any caching of data for this dimension!", dimension.getName());
		} else {
			noNaturalKeyColumnsDefined = false;
			log.debug("Caching for dimension {} is enabled", dimension.getName());
		}
		dimensionLastLineSKAttributeName = dimension.getName() + DIMENSION_SK_SUFFIX;
		log.debug("Last surrogate key value for {} will be available in attributes under name {}", dimension.getName(),
				dimensionLastLineSKAttributeName);
		dimensionPrecachingDisabled = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.DISABLE_DIMENSION_PRE_CACHING_PARAM_NAME, false);
		if (!dimensionPrecachingDisabled) {
			this.preCacheAllKeys();
		} else {
			log.info("Precaching for all dimensions is disabled!");
		}
		if (dimension.getSqlStatements() != null && !StringUtil.isEmpty(dimension.getSqlStatements().getInsertSingle())) {
			insertStatementReplacer = new StatefulAttributeReplacer(dimension.getSqlStatements().getInsertSingle(), dbStringLiteral,
					dbStringEscapeLiteral);
		} else {
			insertStatementReplacer = null;
		}
		if (dimension.getSqlStatements() != null && !StringUtil.isEmpty(dimension.getSqlStatements().getSelectRecordIdentifier())) {
			selectStatementReplacer = new StatefulAttributeReplacer(dimension.getSqlStatements().getSelectRecordIdentifier(), dbStringLiteral,
					dbStringEscapeLiteral);
		} else {
			selectStatementReplacer = null;
		}
	}

	protected DimensionCache initializeDimensionCache(final CacheInstance cacheInstance, final Dimension dimension) {
		return new DimensionCacheTroveImpl(cacheInstance, dimension);
	}

	@Override
	public Dimension getDimension() {
		return dimension;
	}

	private void validate() {
		if (dimension.getMappedColumns() == null || dimension.getMappedColumns().isEmpty()) {
			log.warn("Did not find any mapped columns for {}!", dimension.getName());
		}
		if (dimension.getSqlStatements() == null) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " did not define any sql statements! Check your configuration file!");
		}
		if (this.getFactFeed().getData() == null) {
			throw new IllegalArgumentException("Was not able to find definition of data for feed " + this.getFactFeed().getName());
		}
		if (this.getFactFeed().getData().getAttributes() == null || this.getFactFeed().getData().getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any attributes defined in feed " + this.getFactFeed().getName());
		}
		if (dimension.getLocalCacheMaxSize() != null && dimension.getLocalCacheMaxSize() < 0) {
			throw new IllegalArgumentException("Local cache size for dimension must not be negative integer!");
		}
		final int numberOfNaturalKeys = dimension.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys > this.getFactFeed().getData().getAttributes().size()) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " has more defined natural keys than there are attributes in feed " + this.getFactFeed().getName());
		}
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
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed().getData()
				.getAttributes());
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

	private void calculatePositionOfMappedColumnValues() {
		final int numberOfMappedColumns = dimension.getMappedColumns().size();
		mappedColumnNames = new String[numberOfMappedColumns];
		mappedColumnsPositionsInFeed = new int[numberOfMappedColumns];
		log.debug("Calculating mapped columns position values. Will use offset {}", mappedColumnsPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed().getData()
				.getAttributes());
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

	private void preCacheAllKeys() {
		if (noNaturalKeyColumnsDefined) {
			log.info("Configured to skip caching. Will not precache any key values for {}!", dimension.getName());
			return;
		}
		final String preCacheStatement = dimension.getSqlStatements().getPreCacheRecords();
		if (!StringUtil.isEmpty(preCacheStatement)) {
			log.debug("For dimension {} statement for pre-caching is {}", dimension.getName(), preCacheStatement);
			final long start = System.currentTimeMillis();
			int numberOfRows = 0;
			int numberOfExpectedColumnsReturnedByPreCache = naturalKeyNames.length + 1;
			RowMapper<DimensionKeysPair> rowMapper = new InsertOnlyDimensionKeysRowMapper(dimension.getName(), preCacheStatement,
					numberOfExpectedColumnsReturnedByPreCache);
			if (dimension.getType() == DimensionType.T1 || dimension.getType() == DimensionType.T2) {
				numberOfExpectedColumnsReturnedByPreCache = mappedColumnNames.length + 1;
				rowMapper = new T1DimensionKeysRowMapper(dimension.getName(), preCacheStatement, numberOfExpectedColumnsReturnedByPreCache,
						dimension.getNumberOfNaturalKeys());
			}
			List<DimensionKeysPair> retrievedValues = this.getDbHandler().queryForDimensionKeys(dimension.getName(), preCacheStatement,
					numberOfExpectedColumnsReturnedByPreCache, rowMapper);
			int cachedValuesCount = 0;
			if (retrievedValues != null) {
				if (isDebugEnabled) {
					log.debug("Passing {} for processing", retrievedValues);
				}
				this.processReturnedPreCacheValues(retrievedValues);
				numberOfRows = retrievedValues.size();
				if (numberOfRows > NUMBER_OF_PRE_CACHED_ROWS_WARNING) {
					log.warn("For dimension {} will pre-cache {} rows. This might take a while!", dimension.getName(), numberOfRows);
				}
				cachedValuesCount = dimensionCache.putAllInCache(retrievedValues);
				retrievedValues.clear();
				retrievedValues = null;
				if (dbAccessPreCachedValuesCounter != null) {
					dbAccessPreCachedValuesCounter.inc(cachedValuesCount);
				}
			}
			log.debug("Pre-cached {} keys for {}", cachedValuesCount, dimension.getName());
			if (cachedValuesCount > BaukConstants.LOW_CARDINALITY_DIMENSION_PRE_CACHE_KEYS_THRESHOLD && dimension.getUseInCombinedLookup()) {
				log.warn(
						"Dimension [{}] is marked as low cardinality but precaching statement returned in total {} values! Maximum recommended values for low cardinality dimensions is {}",
						dimension.getName(), cachedValuesCount, BaukConstants.LOW_CARDINALITY_DIMENSION_PRE_CACHE_KEYS_THRESHOLD);
			}
			final long total = System.currentTimeMillis() - start;
			BaukUtil.logEngineMessageSync("For dimension " + dimension.getName() + " pre-cached " + cachedValuesCount + " values. Time taken "
					+ total + "ms");
			if (total > PRE_CACHE_EXECUTION_WARNING) {
				log.warn("Precaching of values for dimension {} took in total {}ms. Number of pre-cached values is {}", dimension.getName(), total,
						cachedValuesCount);
			}
		} else {
			log.info("Could not find pre-cache sql statement for {}!", dimension.getName());
		}
	}

	protected void processReturnedPreCacheValues(final List<DimensionKeysPair> retrievedValues) {
		// used by subclasses
	}

	@Override
	public final Integer getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes) {
		String naturalCacheKey = null;
		if (!noNaturalKeyColumnsDefined) {
			naturalCacheKey = this.buildNaturalKeyForCacheLookup(parsedLine, globalAttributes);
		}
		return this.getBulkLoadValueByPrecalculatedLookupKey(parsedLine, globalAttributes, naturalCacheKey);
	}

	/*
	 * Used for delegates - no need to calculate lookup key again in case when previous line cache failed.
	 */
	Integer getBulkLoadValueByPrecalculatedLookupKey(final String[] parsedLine, final Map<String, String> globalAttributes,
			final String naturalCacheKey) {
		Integer surrogateKey = null;
		if (naturalCacheKey != null) {
			surrogateKey = dimensionCache.getSurrogateKeyFromCache(naturalCacheKey);
		}
		if (surrogateKey == null) {
			if (isTraceEnabled) {
				log.trace("Did not find surrogate key for [{}] in cache, dimension {}. Going to database", naturalCacheKey, dimension.getName());
			}
			surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes, naturalCacheKey);
			if (!noNaturalKeyColumnsDefined && naturalCacheKey != null) {
				dimensionCache.putInCache(naturalCacheKey, surrogateKey);
			}
		}
		if (isTraceEnabled) {
			log.trace("Found surrogate key {} for natural key {} for dimension {}", surrogateKey, naturalCacheKey, dimension.getName());
		}
		return surrogateKey;
	}

	@Override
	public Integer getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final Integer surrogateKey = this.getBulkLoadValue(parsedLine, globalAttributes);
		if (exposeLastLineValueInContext) {
			if (globalAttributes != null) {
				globalAttributes.put(dimensionLastLineSKAttributeName, String.valueOf(surrogateKey));
				if (isTraceEnabled) {
					log.trace("Saved last line value {}={}", dimensionLastLineSKAttributeName, surrogateKey);
				}
			}
		}
		return surrogateKey;
	}

	protected final Integer getSurrogateKeyFromDatabase(final String[] parsedLine, final Map<String, String> globalAttributes,
			final String naturalCacheKey) {
		if (isDebugEnabled) {
			log.debug("Will try to execute insert and then select from database for natural cache key {}", naturalCacheKey);
		}
		final Integer surrogateKey = this.doExecuteInsertStatement(parsedLine, globalAttributes, naturalCacheKey);
		if (surrogateKey != null) {
			return surrogateKey;
		}
		final String preparedSelectStatement = this.prepareStatement(parsedLine, globalAttributes, selectStatementReplacer);
		final Integer result = this.trySelectStatement(preparedSelectStatement);
		if (result == null) {
			log.warn("After failing to insert record could not find key by select. Select ctatement is {}", preparedSelectStatement);
		}
		return result;
	}

	protected final Integer doExecuteInsertStatement(final String[] parsedLine, final Map<String, String> globalAttributes,
			final String naturalCacheKey) {
		final String preparedInsertStatement = this.prepareStatement(parsedLine, globalAttributes, insertStatementReplacer);
		try {
			return this.tryInsertStatement(preparedInsertStatement);
		} catch (final DuplicateKeyException dexc) {
			if (naturalCacheKey != null) {
				// Maybe some other thread inserted this record just a moment ago - so, let's try to find it in cache.
				// This is faster than going to database
				final Integer doubleCheckInCache = dimensionCache.getSurrogateKeyFromCache(naturalCacheKey);
				if (doubleCheckInCache != null) {
					return doubleCheckInCache;
				}
			}
			log.warn("Failed inserting record into database - duplicate key! Will try to select value from database! {}. Insert statement was {}",
					dexc.getMessage(), preparedInsertStatement);
		} catch (final Exception exc) {
			log.error(
					"Failed inserting record into database. This should not happen! Check database connection! Will try to select value from database!",
					exc);
			log.error("Insert statement was {}", preparedInsertStatement);
		}
		return null;
	}

	private Integer trySelectStatement(final String preparedStatement) {
		final Long result = this.getDbHandler().executeQueryStatementAndReturnKey(preparedStatement, dimension.getName());
		if (dbAccessSelectCounter != null) {
			dbAccessSelectCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using select surrogate key statement " + preparedStatement);
		}
		if (isTraceEnabled) {
			log.trace("Retrieved surrogate key {} for {}", result, preparedStatement);
		}
		return result.intValue();
	}

	private Integer tryInsertStatement(final String preparedStatement) {
		final Long result = this.getDbHandler().executeInsertStatementAndReturnKey(preparedStatement, dimension.getName());
		if (dbAccessInsertCounter != null) {
			dbAccessInsertCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using insert statement " + preparedStatement);
		}
		if (isDebugEnabled) {
			log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		}
		return result.intValue();
	}

	protected final String prepareStatement(final String[] parsedLine, final Map<String, String> globalAttributes,
			final StatefulAttributeReplacer replacer) {
		Map<String, String> mappedColumnValues = this.getAllMappedColumnValues(parsedLine);
		if (mappedColumnValues != null) {
			mappedColumnValues.putAll(globalAttributes);
		} else {
			mappedColumnValues = globalAttributes;
		}
		final String stat = replacer.replaceAttributes(mappedColumnValues);
		if (isDebugEnabled) {
			log.debug("Final statement (after replacing all values) is {}", stat);
		}
		return stat;
	}

	private Map<String, String> getAllMappedColumnValues(final String[] parsedLine) {
		if (mappedColumnNames == null || parsedLine == null) {
			return null;
		}
		final Map<String, String> vals = new THashMap<>(50);
		for (int i = 0; i < mappedColumnNames.length; i++) {
			final int mappedColumnValuePositionInFeed = mappedColumnsPositionsInFeed[i];
			if (mappedColumnValuePositionInFeed == NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION) {
				// will eventually be replaced by attribute values from context
				continue;
			}
			if (parsedLine.length < mappedColumnValuePositionInFeed) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + mappedColumnValuePositionInFeed);
			}
			final String value = parsedLine[mappedColumnValuePositionInFeed];
			vals.put(mappedColumnNames[i], value);
		}
		return vals;
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

	@Override
	public final int[] getMappedColumnPositionsInFeed() {
		return mappedColumnsPositionsInFeed;
	}

	/*
	 * used for testing
	 */

	String[] getMappedColumnNames() {
		return mappedColumnNames;
	}

	String[] getNaturalKeyNames() {
		return naturalKeyNames;
	}

	int[] getNaturalKeyPositionsInFeed() {
		return naturalKeyPositionsInFeed;
	}

	@Override
	public void closeCurrentFeed() {
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return false;
	}

}
