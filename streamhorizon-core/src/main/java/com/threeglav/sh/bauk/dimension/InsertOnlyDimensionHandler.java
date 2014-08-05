package com.threeglav.sh.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dimension.db.InsertOnlyDimensionKeysRowMapper;
import com.threeglav.sh.bauk.dimension.db.T1DimensionKeysRowMapper;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.DimensionType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

/**
 * One instance per dimension. Must be thread-safe.
 * 
 * 
 */
public class InsertOnlyDimensionHandler extends AbstractDimensionHandler {

	private static final String DIMENSION_SK_SUFFIX = ".sk";

	private static final int NUMBER_OF_PRE_CACHED_ROWS_WARNING = 500000;

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final int PRE_CACHE_EXECUTION_WARNING = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
			BaukEngineConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);

	private final boolean dimensionPrecachingDisabled;

	private final String dimensionLastLineSKAttributeName;

	private final Counter dbAccessSelectCounter;
	private final Counter dbAccessInsertCounter;
	private final Counter dbAccessPreCachedValuesCounter;

	protected final String dbStringLiteral;
	protected final String dbStringEscapeLiteral;

	private final boolean exposeLastLineValueInContext;
	private final StatefulAttributeReplacer insertStatementReplacer;
	private final StatefulAttributeReplacer selectStatementReplacer;

	public InsertOnlyDimensionHandler(final Dimension dimension, final Feed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(factFeed, config, dimension, naturalKeyPositionOffset, cacheInstance);
		if (naturalKeyPositionOffset < 0) {
			throw new IllegalArgumentException("Natural key position offset must not be negative number");
		}
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		dbStringEscapeLiteral = this.getConfig().getDatabaseStringEscapeLiteral();
		exposeLastLineValueInContext = dimension.getExposeLastLineValueInContext();
		this.validate();

		dbAccessSelectCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database selects executed");
		dbAccessInsertCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database inserts executed");
		dbAccessPreCachedValuesCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total pre-cached values retrieved");
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

	private void validate() {
		if (dimension.getType() != DimensionType.CUSTOM) {
			if (dimension.getSqlStatements() == null) {
				throw new IllegalStateException("SqlStatements configuration is required for dimension " + dimension.getName());
			}
		}
		if (dimension.getMappedColumns() == null || dimension.getMappedColumns().isEmpty()) {
			log.warn("Did not find any mapped columns for {}!", dimension.getName());
		}
		if (dimension.getSqlStatements() == null) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " did not define any sql statements! Check your configuration file!");
		}
		if (this.getFactFeed().getSourceFormatDefinition().getData() == null) {
			throw new IllegalArgumentException("Was not able to find definition of data for feed " + this.getFactFeed().getName());
		}
		if (this.getFactFeed().getSourceFormatDefinition().getData().getAttributes() == null
				|| this.getFactFeed().getSourceFormatDefinition().getData().getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any attributes defined in feed " + this.getFactFeed().getName());
		}
		if (dimension.getLocalCacheMaxSize() != null && dimension.getLocalCacheMaxSize() < 0) {
			throw new IllegalArgumentException("Local cache size for dimension must not be negative integer!");
		}
		final int numberOfNaturalKeys = dimension.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys > this.getFactFeed().getSourceFormatDefinition().getData().getAttributes().size()) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " has more defined natural keys than there are attributes in feed " + this.getFactFeed().getName());
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
				log.trace("Did not find surrogate key for [{}] in cache, dimension {}. Will search for value in the database", naturalCacheKey,
						dimension.getName());
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
					"Failed inserting record into database. This should not happen! Check database connection! After this will also try to select SK value from database!",
					exc);
			log.error("Failed insert statement was {}", preparedInsertStatement);
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

	@Override
	public void closeCurrentFeed() {
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return false;
	}

}
