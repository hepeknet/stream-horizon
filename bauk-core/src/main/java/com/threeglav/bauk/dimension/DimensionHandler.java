package com.threeglav.bauk.dimension;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

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
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class DimensionHandler extends ConfigAware implements BulkLoadOutputValueHandler {

	private static final String DIMENSION_SK_SUFFIX = ".sk";

	private static final int NOT_FOUND_IN_FEED_NATURAL_KEY_POSITION = -1;

	private static final int NUMBER_OF_PRE_CACHED_ROWS_WARNING = 500000;

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final int PRE_CACHE_EXECUTION_WARNING = ConfigurationProperties.getSystemProperty(
			SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_SYS_PARAM_NAME,
			SystemConfigurationConstants.SQL_EXECUTION_WARNING_THRESHOLD_MILLIS);

	private final String dimensionLastLineSKAttributeName;
	protected final Dimension dimension;
	private String[] mappedColumnNames;
	private int[] mappedColumnsPositionsInFeed;
	private String[] naturalKeyNames;
	private int[] naturalKeyPositionsInFeed;
	private final Counter dbAccessSelectCounter;
	private final Counter dbAccessInsertCounter;
	private final Counter dbAccessPreCachedValuesCounter;
	private final int mappedColumnsPositionOffset;
	private final String dbStringLiteral;
	private final String dbStringEscapeLiteral;
	private final boolean noNaturalKeyColumnsDefined;
	private final DimensionCache dimensionCache;

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
		dimensionCache = new DimensionCache(cacheInstance, routeIdentifier, dimension.getName());
		if (naturalKeyPositionOffset < 0) {
			throw new IllegalArgumentException("Natural key position offset must not be negative number");
		}
		dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		dbStringEscapeLiteral = this.getConfig().getDatabaseStringEscapeLiteral();
		this.validate();
		mappedColumnsPositionOffset = naturalKeyPositionOffset;
		this.calculatePositionOfMappedColumnValues();
		this.calculatePositionOfNaturalKeyValues();
		dbAccessSelectCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - total database selects executed", false);
		dbAccessInsertCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - total database inserts executed", false);
		dbAccessPreCachedValuesCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - total pre-cached values retrieved", false);
		final int numberOfNaturalKeys = this.getNumberOfNaturalKeys();
		if (numberOfNaturalKeys == 0) {
			noNaturalKeyColumnsDefined = true;
			log.warn("Did not find any defined natural keys for {}. Will disable any caching of data for this dimension!", dimension.getName());
		} else {
			noNaturalKeyColumnsDefined = false;
			log.debug("Caching for {} is enabled", dimension.getName());
		}
		dimensionLastLineSKAttributeName = dimension.getName() + DIMENSION_SK_SUFFIX;
		log.debug("Last surrogate key value for {} will be available in attributes under name {}", dimension.getName(),
				dimensionLastLineSKAttributeName);
		this.preCacheAllKeys();
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
		if (noNaturalKeyColumnsDefined) {
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
		final String preCacheStatement = dimension.getSqlStatements().getPreCacheKeys();
		if (!StringUtil.isEmpty(preCacheStatement)) {
			log.debug("For dimension {} statement for pre-caching is {}", dimension.getName(), preCacheStatement);
			final long start = System.currentTimeMillis();
			int numberOfRows = 0;
			List<String[]> retrievedValues = this.getDbHandler()
					.queryForDimensionKeys(dimension.getName(), preCacheStatement, naturalKeyNames.length);
			if (retrievedValues != null) {
				numberOfRows = retrievedValues.size();
				if (numberOfRows > NUMBER_OF_PRE_CACHED_ROWS_WARNING) {
					log.warn("For dimension {} will pre-cache {} rows. This might take a while!", dimension.getName(), numberOfRows);
				}
				final Iterator<String[]> iter = retrievedValues.iterator();
				while (iter.hasNext()) {
					final String[] row = iter.next();
					iter.remove();
					final String surrogateKeyValue = row[0];
					final String naturalKeyValue = row[1];
					dimensionCache.putInCache(naturalKeyValue, surrogateKeyValue);
				}
				retrievedValues.clear();
				retrievedValues = null;
				dbAccessPreCachedValuesCounter.inc(numberOfRows);
			}
			log.debug("Pre-cached {} keys for {}", numberOfRows, dimension.getName());
			final long total = System.currentTimeMillis() - start;
			BaukUtil.logEngineMessage("For dimension " + dimension.getName() + " pre-cached " + numberOfRows + " values. Time taken " + total + "ms");
			if (total > PRE_CACHE_EXECUTION_WARNING) {
				log.warn("Precaching of values for dimension {} took in total {}ms. Number of pre-cached values is {}", dimension.getName(), total,
						numberOfRows);
			}
		} else {
			log.info("Could not find pre-cache sql statement for {}!", dimension.getName());
		}
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes, final boolean isLastLine) {
		String surrogateKey = null;
		String naturalCacheKey = null;
		if (!noNaturalKeyColumnsDefined) {
			naturalCacheKey = this.buildNaturalKeyForCacheLookup(parsedLine, globalAttributes);
			if (naturalCacheKey != null) {
				surrogateKey = dimensionCache.getSurrogateKeyFromCache(naturalCacheKey);
			}
		}
		if (surrogateKey == null) {
			if (isTraceEnabled) {
				log.trace("Did not find surrogate key for [{}] in cache, dimension {}. Going to database", naturalCacheKey, dimension.getName());
			}
			surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes);
			if (!noNaturalKeyColumnsDefined && naturalCacheKey != null) {
				dimensionCache.putInCache(naturalCacheKey, surrogateKey);
			}
		} else if (isTraceEnabled) {
			log.trace("Found surrogate key {} for {} in cache", surrogateKey, naturalCacheKey);
		}
		if (isTraceEnabled) {
			log.trace("Resolved surrogate key is {}", surrogateKey);
		}
		if (isLastLine && globalAttributes != null) {
			globalAttributes.put(dimensionLastLineSKAttributeName, surrogateKey);
			if (isTraceEnabled) {
				log.trace("Saved last line value {}={}", dimensionLastLineSKAttributeName, surrogateKey);
			}
		}
		return surrogateKey;
	}

	private String getSurrogateKeyFromDatabase(final String[] parsedLine, final Map<String, String> globalAttributes) {
		final String insertStatement = dimension.getSqlStatements().getInsertSingle();
		final String preparedInsertStatement = this.prepareStatement(insertStatement, parsedLine, globalAttributes);
		try {
			return this.tryInsertStatement(preparedInsertStatement);
		} catch (final DuplicateKeyException dexc) {
			log.warn("Failed inserting record into database - duplicate key! Will try to select value from database! {}. Insert statement was {}",
					dexc.getMessage(), preparedInsertStatement);
		} catch (final Exception exc) {
			log.error(
					"Failed inserting record into database. This should not happen! Check database connection! Will try to select value from database!",
					exc);
			log.error("Insert statement was {}", preparedInsertStatement);
		}
		final String selectSurrogateKey = dimension.getSqlStatements().getSelectSurrogateKey();
		final String preparedSelectStatement = this.prepareStatement(selectSurrogateKey, parsedLine, globalAttributes);
		final String result = this.trySelectStatement(preparedSelectStatement);
		if (result == null) {
			log.warn("After failing to insert record could not find key by select. Select ctatement is {}", preparedSelectStatement);
		}
		return result;
	}

	private String trySelectStatement(final String preparedStatement) {
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
		return result.toString();
	}

	private String tryInsertStatement(final String preparedStatement) {
		final Long result = this.getDbHandler().executeInsertStatementAndReturnKey(preparedStatement, dimension.getName());
		if (dbAccessInsertCounter != null) {
			dbAccessInsertCounter.inc();
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
		stat = StringUtil.replaceAllAttributes(stat, globalAttributes, dbStringLiteral, dbStringEscapeLiteral);
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
				// will eventually be replaced by attribute values from context
				continue;
			}
			if (parsedLine.length < mappedColumnValuePositionInFeed) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + mappedColumnValuePositionInFeed);
			}
			final String value = parsedLine[mappedColumnValuePositionInFeed];
			final String mappedColumnNamePlaceholder = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + mappedColumnNames[i]
					+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			log.debug("Replacing {} with {}", mappedColumnNamePlaceholder, value);
			replaced = StringUtil.replaceSingleAttribute(replaced, mappedColumnNamePlaceholder, value, dbStringLiteral, dbStringEscapeLiteral);
		}
		return replaced;
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
