package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.MappedColumn;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class DimensionHandler extends ConfigAware implements BulkLoadOutputValueHandler {

	private static final String DIMENSION_SK_SUFFIX = ".sk";

	private static final int NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION = -1;

	private static final int MAX_ELEMENTS_LOCAL_MAP = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME, SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT);

	private final Map<String, String> localCache = new THashMap<>();

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final String dimensionLastLineSKAttributeName;
	protected final Dimension dimension;
	private String[] mappedColumnNames;
	private int[] mappedColumnsPositionsInFeed;
	private String[] naturalKeyNames;
	private int[] naturalKeyPositionsInFeed;
	private final CacheInstance cacheInstance;
	private final Counter dbAccessCounter;
	private final int mappedColumnsPositionOffset;
	private final Counter localCacheClearCounter;
	private final String dbStringLiteral;
	private final boolean skipCaching;
	private final boolean isTracingEnabled;
	private final boolean isDebugEnabled;

	public DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final String routeIdentifier, final BaukConfiguration config) {
		super(factFeed, config);
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		if (cacheInstance == null) {
			throw new IllegalArgumentException("Cache handler must not be null");
		}
		this.cacheInstance = cacheInstance;
		if (naturalKeyPositionOffset < 0) {
			throw new IllegalArgumentException("Natural key position offset must not be negative number");
		}
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		this.validate();
		mappedColumnsPositionOffset = naturalKeyPositionOffset;
		this.calculatePositionOfMappedColumnValues();
		this.calculatePositionOfNaturalKeyValues();
		dbAccessCounter = MetricsUtil
				.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName() + "] - total database access times");
		localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - local cache clear times");
		final int numberOfNaturalKeys = this.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys == 0) {
			skipCaching = true;
			log.warn("Did not find any defined natural keys for {}. Will disable any caching of data for this dimension!", dimension.getName());
		} else {
			skipCaching = false;
			log.debug("Caching for {} is enabled", dimension.getName());
		}
		dimensionLastLineSKAttributeName = dimension.getName() + DIMENSION_SK_SUFFIX;
		log.debug("Last surrogate key value for {} will be available in attributes under name {}", dimension.getName(),
				dimensionLastLineSKAttributeName);
		this.preCacheAllKeys();
		// help JIT remove dead code
		isTracingEnabled = log.isTraceEnabled();
		isDebugEnabled = log.isDebugEnabled();
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
		final int numberOfNaturalKeys = this.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys > this.getFactFeed().getData().getAttributes().size()) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " has more defined natural keys than there are attributes in feed " + this.getFactFeed().getName());
		}
		if (!StringUtil.isEmpty(dimension.getCacheKeyPerFeedInto())) {
			log.info(
					"Key for dimension {} will be looked up only once and after successful retrieval it will be cached per feed and available as variable {}",
					dimension.getName(), dimension.getCacheKeyPerFeedInto());
		}
	}

	private int getNumberOfNaturalKeys() {
		int num = 0;
		if (dimension.getMappedColumns() != null && !dimension.getMappedColumns().isEmpty()) {
			for (final MappedColumn mp : dimension.getMappedColumns()) {
				if (mp.isNaturalKey()) {
					num++;
				}
			}
		}
		return num;
	}

	private void calculatePositionOfNaturalKeyValues() {
		if (skipCaching) {
			return;
		}
		final int numberOfNaturalKeys = this.getNumberOfNaturalKeys();
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
	}

	private void calculatePositionOfMappedColumnValues() {
		if (skipCaching) {
			return;
		}
		final int numberOfMappedColumns = dimension.getMappedColumns().size();
		mappedColumnNames = new String[numberOfMappedColumns];
		mappedColumnsPositionsInFeed = new int[numberOfMappedColumns];
		log.debug("Calculating mapped columns position values. Will use offset {}", mappedColumnsPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil.getAttributeNamesAndPositions(this.getFactFeed().getData()
				.getAttributes());
		for (final MappedColumn nk : dimension.getMappedColumns()) {
			final String mappedColumnName = nk.getName();
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
		if (skipCaching) {
			log.info("Configured to skip caching. Will not precache any key values for {}!", dimension.getName());
			return;
		}
		final String preCacheStatement = dimension.getSqlStatements().getPreCacheKeys();
		if (!StringUtil.isEmpty(preCacheStatement)) {
			log.debug("For dimension {} statement for pre-caching is {}", dimension.getName(), preCacheStatement);
			int numberOfRows = 0;
			final List<String[]> retrievedValues = this.getDbHandler().queryForDimensionKeys(preCacheStatement, naturalKeyNames.length);
			if (retrievedValues != null) {
				numberOfRows = retrievedValues.size();
				final Iterator<String[]> iter = retrievedValues.iterator();
				while (iter.hasNext()) {
					final String[] row = iter.next();
					iter.remove();
					final String surrogateKeyValue = row[0];
					final String[] naturalKeyValues = new String[row.length - 1];
					System.arraycopy(row, 1, naturalKeyValues, 0, row.length - 1);
					final String naturalKeyValue = StringUtil.getNaturalKeyCacheKey(naturalKeyValues);
					cacheInstance.put(naturalKeyValue, surrogateKeyValue);
					this.putInLocalCache(naturalKeyValue, surrogateKeyValue);
				}
			}
			log.debug("Pre-cached {} keys for {}", numberOfRows, dimension.getName());
		} else {
			log.info("Could not find pre-cache sql statement for {}!", dimension.getName());
		}
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes, final boolean isLastLine) {
		String surrogateKey = null;
		String naturalCacheKey = null;
		if (!skipCaching) {
			naturalCacheKey = this.buildNaturalKeyForCacheLookup(parsedLine, globalAttributes);
			if (naturalCacheKey != null) {
				surrogateKey = this.getSurrogateKeyFromCache(naturalCacheKey);
			}
		}
		if (surrogateKey == null) {
			if (isTracingEnabled) {
				log.trace("Did not find surrogate key for [{}] in cache, dimension {}. Going to database", naturalCacheKey, dimension.getName());
			}
			surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes);
			if (!skipCaching && naturalCacheKey != null) {
				cacheInstance.put(naturalCacheKey, surrogateKey);
				this.putInLocalCache(naturalCacheKey, surrogateKey);
			}
		} else if (isTracingEnabled) {
			log.trace("Found surrogate key {} for {} in cache", surrogateKey, naturalCacheKey);
		}
		if (isTracingEnabled) {
			log.trace("Resolved surrogate key is {}", surrogateKey);
		}
		if (isLastLine && globalAttributes != null) {
			globalAttributes.put(dimensionLastLineSKAttributeName, surrogateKey);
			if (isTracingEnabled) {
				log.trace("Saved last line value {}={}", dimensionLastLineSKAttributeName, surrogateKey);
			}
		}
		return surrogateKey;
	}

	private String getSurrogateKeyFromDatabase(final String[] parsedLine, final Map<String, String> globalAttributes) {
		try {
			return this.tryInsertStatement(parsedLine, globalAttributes);
		} catch (final Exception exc) {
			log.error("Failed inserting record into database. Will try to select value from database!", exc);
		}
		return this.trySelectStatement(parsedLine, globalAttributes);
	}

	private String trySelectStatement(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final String selectSurrogateKey = dimension.getSqlStatements().getSelectSurrogateKey();
		if (StringUtil.isEmpty(selectSurrogateKey)) {
			throw new IllegalArgumentException("Dimension select surrogate key statement is null or empty");
		}
		final String preparedStatement = this.prepareStatement(selectSurrogateKey, parsedLine, globalAttributes);
		final Long result = this.getDbHandler().executeQueryStatementAndReturnKey(preparedStatement);
		if (dbAccessCounter != null) {
			dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using select surrogate key statement " + preparedStatement);
		}
		if (isTracingEnabled) {
			log.trace("Retrieved surrogate key {} for {}", result, preparedStatement);
		}
		return result.toString();
	}

	private String tryInsertStatement(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final String insertStatement = dimension.getSqlStatements().getInsertSingle();
		if (StringUtil.isEmpty(insertStatement)) {
			throw new IllegalArgumentException("Insert statement for dimension " + dimension.getName()
					+ " is null or empty. Unable to try inserting value into database.");
		}
		final String preparedStatement = this.prepareStatement(insertStatement, parsedLine, globalAttributes);
		final Long result = this.getDbHandler().executeInsertStatementAndReturnKey(preparedStatement);
		if (dbAccessCounter != null) {
			dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using insert statement " + preparedStatement);
		}
		log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		return result.toString();
	}

	private String prepareStatement(final String statement, final String[] parsedLine, final Map<String, String> globalAttributes) {
		if (log.isDebugEnabled()) {
			final String placeHolderFormat = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + "ATTRIBUTE_NAME"
					+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			log.debug("Will prepare statement {}. Replacing all placeholders {} with their real values (from feed or header)", statement,
					placeHolderFormat);
		}
		String stat = this.replaceAllMappedColumnValues(statement, parsedLine);
		log.debug("Replacing all global attributes {}", globalAttributes);
		stat = StringUtil.replaceAllAttributes(stat, globalAttributes, dbStringLiteral);
		log.debug("Final statement is {}", stat);
		return stat;
	}

	private String replaceAllMappedColumnValues(final String statement, final String[] parsedLine) {
		if (mappedColumnNames == null || parsedLine == null) {
			return statement;
		}
		String replaced = statement;
		for (int i = 0; i < mappedColumnNames.length; i++) {
			final int mappedColumnValuePositionInFeed = mappedColumnsPositionsInFeed[i];
			if (mappedColumnValuePositionInFeed == NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION) {
				// will be replaced by header/global values
				continue;
			}
			if (parsedLine.length < mappedColumnValuePositionInFeed) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + mappedColumnValuePositionInFeed);
			}
			final String value = parsedLine[mappedColumnValuePositionInFeed];
			final String mappedColumnNamePlaceholder = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + mappedColumnNames[i]
					+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			log.debug("Replacing {} with {}", mappedColumnNamePlaceholder, value);
			replaced = StringUtil.replaceSingleAttribute(replaced, mappedColumnNamePlaceholder, value, dbStringLiteral);
		}
		return replaced;
	}

	private String getSurrogateKeyFromCache(final String cacheKey) {
		final String locallyCachedValue = localCache.get(cacheKey);
		if (locallyCachedValue != null) {
			return locallyCachedValue;
		} else {
			final String cachedValue = cacheInstance.getSurrogateKey(cacheKey);
			if (cachedValue != null) {
				this.putInLocalCache(cacheKey, cachedValue);
			}
			return cachedValue;
		}
	}

	private void putInLocalCache(final String cacheKey, final String cachedValue) {
		if (localCache.size() > MAX_ELEMENTS_LOCAL_MAP) {
			log.debug("Local cache has more than {} elements. Have to clear it!", MAX_ELEMENTS_LOCAL_MAP);
			localCache.clear();
			if (localCacheClearCounter != null) {
				localCacheClearCounter.inc();
			}
		}
		localCache.put(cacheKey, cachedValue);
	}

	private String buildNaturalKeyForCacheLookup(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final boolean parsedLineIsNull = (parsedLine == null);
		final StringBuilder sb = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		for (int i = 0; i < naturalKeyPositionsInFeed.length; i++) {
			final int key = naturalKeyPositionsInFeed[i];
			if (!parsedLineIsNull && parsedLine.length <= key) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + key);
			}
			String value = null;
			if (parsedLineIsNull || key == NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION) {
				final String attributeName = naturalKeyNames[i];
				if (attributeName != null) {
					final String globalAttributeValue = globalAttributes.get(attributeName);
					if (isDebugEnabled) {
						log.debug(
								"Natural key {}.{} is not mapped to any of declared feed attributes. Will use value [{}] found in global attributes",
								dimension.getName(), attributeName, globalAttributeValue);
					}
					value = globalAttributeValue;
				}
			} else {
				value = parsedLine[key];
			}
			if (i != 0) {
				sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
			}
			sb.append(value);
		}
		return sb.toString();
	}

	/*
	 * used for testing
	 */

	String[] getMappedColumnNames() {
		return mappedColumnNames;
	}

	int[] getMappedColumnPositions() {
		return mappedColumnsPositionsInFeed;
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
	public void calculatePerFeedValues(final Map<String, String> globalValues) {
	}

}
