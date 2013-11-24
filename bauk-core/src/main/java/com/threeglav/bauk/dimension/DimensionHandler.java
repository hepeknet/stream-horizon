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
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.feed.ConfigAware;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.NaturalKey;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class DimensionHandler extends ConfigAware implements BulkLoadOutputValueHandler {

	private static final int MAX_ELEMENTS_LOCAL_MAP = getDimensionLocalCacheSize();

	private final THashMap<String, String> localCache = new THashMap<>();

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Dimension dimension;
	private final FactFeed factFeed;
	private String[] naturalKeyNames;
	private int[] naturalKeyPositionsInFeed;
	private final CacheInstance cacheInstance;
	private final DbHandler dbHandler;
	private final Counter dbAccessCounter;
	private final int naturalKeyPositionOffset;
	private final Counter localCacheClearCounter;
	private boolean skipCaching;

	public DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance, final DbHandler dbHandler,
			final int naturalKeyPositionOffset, final String routeIdentifier, final BaukConfiguration config) {
		super(factFeed, config);
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		if (factFeed == null) {
			throw new IllegalArgumentException("Fact feed must not be null");
		}
		this.factFeed = factFeed;
		if (cacheInstance == null) {
			throw new IllegalArgumentException("Cache handler must not be null");
		}
		this.cacheInstance = cacheInstance;
		if (dbHandler == null) {
			throw new IllegalArgumentException("DB handler must not be null");
		}
		this.dbHandler = dbHandler;
		if (naturalKeyPositionOffset < 0) {
			throw new IllegalArgumentException("Natural key position offset must not be negative number");
		}
		this.validate();
		this.naturalKeyPositionOffset = naturalKeyPositionOffset;
		this.calculatePositionOfNaturalKeys();
		dbAccessCounter = MetricsUtil
				.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName() + "] - total database access times");
		localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - local cache clear times");
		this.preCacheAllKeys();
	}

	private void validate() {
		if (dimension.getNaturalKeys() == null || dimension.getNaturalKeys().isEmpty()) {
			skipCaching = true;
			log.warn("Did not find any defined naturaly keys for {}. Will disable any caching of data for this dimension!", dimension.getName());
		}
		if (dimension.getSqlStatements() == null) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " did not define any sql statements! Check your configuration file!");
		}
		if (factFeed.getData() == null) {
			throw new IllegalArgumentException("Was not able to find definition of data for feed " + factFeed.getName());
		}
		if (factFeed.getData().getAttributes() == null || factFeed.getData().getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any attributes defined in feed " + factFeed.getName());
		}
		if (dimension.getNaturalKeys().size() > factFeed.getData().getAttributes().size()) {
			throw new IllegalArgumentException("Dimension " + dimension.getName()
					+ " has more defined natural keys than there are attributes in feed " + factFeed.getName());
		}
	}

	private void calculatePositionOfNaturalKeys() {
		if (skipCaching) {
			return;
		}
		final int numberOfNaturalKeys = dimension.getNaturalKeys().size();
		naturalKeyNames = new String[numberOfNaturalKeys];
		naturalKeyPositionsInFeed = new int[numberOfNaturalKeys];
		log.debug("Calculating natural key position values. Will use offset {}", naturalKeyPositionOffset);
		int i = 0;
		final Map<String, Integer> dataAttributesAndPositions = AttributeParsingUtil
				.getAttributeNamesAndPositions(factFeed.getData().getAttributes());
		for (final NaturalKey nk : dimension.getNaturalKeys()) {
			final String nkName = nk.getName();
			naturalKeyNames[i] = nkName;
			log.debug("Trying to find position in feed for natural key {} for dimension {}", nkName, dimension.getName());
			final Integer attrPosition = dataAttributesAndPositions.get(nkName);
			if (attrPosition == null) {
				throw new IllegalArgumentException("Was not able to find natural key " + nkName + " in feed " + factFeed.getName());
			}
			final int naturalKeyPositionValue = attrPosition + naturalKeyPositionOffset;
			naturalKeyPositionsInFeed[i++] = naturalKeyPositionValue;
			if (log.isDebugEnabled()) {
				log.debug("Natural key [{}] for dimension {} will be read from feed position {}", new Object[] { nkName, dimension.getName(),
						naturalKeyPositionValue });
			}
		}
	}

	private void preCacheAllKeys() {
		if (skipCaching) {
			log.info("Instructed to skip caching. Will not precache any key values for {}!", dimension.getName());
			return;
		}
		final String preCacheStatement = dimension.getSqlStatements().getPreCacheKeys();
		if (!StringUtil.isEmpty(preCacheStatement)) {
			log.debug("Statement for pre-caching is {}", preCacheStatement);
			int numberOfRows = 0;
			final List<String[]> retrievedValues = dbHandler.queryForDimensionKeys(preCacheStatement, naturalKeyNames.length);
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
				}
			}
			log.debug("Pre cached {} keys for {}", numberOfRows, dimension.getName());
		} else {
			log.info("Could not find pre cache sql statement!");
		}
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> headerAttributes, final Map<String, String> globalAttributes) {
		if (parsedLine == null || parsedLine.length == 0) {
			throw new IllegalArgumentException("Did not find any data in parsed line");
		}
		String surrogateKey = null;
		String naturalCacheKey = null;
		if (!skipCaching) {
			naturalCacheKey = this.buildNaturalKeyForCacheLookup(parsedLine);
			surrogateKey = this.getSurrogateKeyFromCache(naturalCacheKey);
		}
		if (surrogateKey == null) {
			if (log.isTraceEnabled()) {
				log.trace("Did not find surrogate key for [{}] in cache, dimension {}. Going to database", naturalCacheKey, dimension.getName());
			}
			surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, headerAttributes, globalAttributes);
			if (!skipCaching) {
				cacheInstance.put(naturalCacheKey, surrogateKey);
			}
		} else {
			log.trace("Found surrogate key {} for {} in cache", surrogateKey, naturalCacheKey);
		}
		return surrogateKey;
	}

	private String getSurrogateKeyFromDatabase(final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		try {
			return this.tryInsertStatement(parsedLine, headerAttributes, globalAttributes);
		} catch (final Exception exc) {
			log.error("Failed inserting record {}. Will try to select value from database!", exc.getMessage());
		}
		return this.trySelectStatement(parsedLine, headerAttributes, globalAttributes);
	}

	private String trySelectStatement(final String[] parsedLine, final Map<String, String> headerValues, final Map<String, String> globalAttributes) {
		final String selectSurrogateKey = dimension.getSqlStatements().getSelectSurrogateKey();
		if (StringUtil.isEmpty(selectSurrogateKey)) {
			throw new IllegalArgumentException("Dimension select surrogate key statement is null or empty");
		}
		final String preparedStatement = this.prepareStatement(selectSurrogateKey, parsedLine, headerValues, globalAttributes);
		final Long result = dbHandler.executeQueryStatementAndReturnKey(preparedStatement);
		if (dbAccessCounter != null) {
			dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using select surrogate key statement " + preparedStatement);
		}
		log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		return result.toString();
	}

	private String tryInsertStatement(final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		final String insertStatement = dimension.getSqlStatements().getInsertSingle();
		if (StringUtil.isEmpty(insertStatement)) {
			throw new IllegalArgumentException("Dimension insert statement is null or empty");
		}
		final String preparedStatement = this.prepareStatement(insertStatement, parsedLine, headerAttributes, globalAttributes);
		final Long result = dbHandler.executeInsertStatementAndReturnKey(preparedStatement);
		if (dbAccessCounter != null) {
			dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using insert statement " + preparedStatement);
		}
		log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		return result.toString();
	}

	private String prepareStatement(final String statement, final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		if (log.isDebugEnabled()) {
			final String placeHolderFormat = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + "ATTRIBUTE_NAME"
					+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			log.debug("Will prepare statement {}. Replacing all placeholders {} with their real values (from feed or header)", statement,
					placeHolderFormat);
		}
		String stat = this.replaceAllNaturalKeys(statement, parsedLine);
		log.debug("Replacing all global attributes {}", globalAttributes);
		final String dbStringLiteral = this.getConfig().getDatabaseStringLiteral();
		stat = StringUtil.replaceAllAttributes(stat, globalAttributes, BaukConstants.GLOBAL_ATTRIBUTE_PREFIX, dbStringLiteral);
		log.debug("Replacing all header attributes {}", headerAttributes);
		stat = StringUtil.replaceAllAttributes(stat, headerAttributes, BaukConstants.HEADER_ATTRIBUTE_PREFIX, dbStringLiteral);
		log.debug("Final statement is {}", stat);
		return stat;
	}

	private String replaceAllNaturalKeys(final String statement, final String[] parsedLine) {
		if (naturalKeyNames == null) {
			return statement;
		}
		String replaced = statement;
		for (int i = 0; i < naturalKeyNames.length; i++) {
			final String nkPlacholder = BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_START + naturalKeyNames[i]
					+ BaukConstants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			final int nkValuePosition = naturalKeyPositionsInFeed[i];
			if (parsedLine.length < nkValuePosition) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + nkValuePosition);
			}
			final String value = parsedLine[nkValuePosition];
			log.debug("Replacing {} with {}", nkPlacholder, value);
			replaced = replaced.replace(nkPlacholder, value);
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
				if (localCache.size() > MAX_ELEMENTS_LOCAL_MAP) {
					log.debug("Local cache has more than {} elements. Have to clear it!", MAX_ELEMENTS_LOCAL_MAP);
					localCache.clear();
					if (localCacheClearCounter != null) {
						localCacheClearCounter.inc();
					}
				}
				localCache.put(cacheKey, cachedValue);
			}
			return cachedValue;
		}
	}

	private String buildNaturalKeyForCacheLookup(final String[] parsedLine) {
		final String[] naturalKeyValues = new String[naturalKeyNames.length];
		for (int i = 0; i < naturalKeyPositionsInFeed.length; i++) {
			final int key = naturalKeyPositionsInFeed[i];
			if (parsedLine.length <= key) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + key);
			}
			naturalKeyValues[i] = parsedLine[key];
		}
		return StringUtil.getNaturalKeyCacheKey(naturalKeyValues);
	}

	/*
	 * used for testing
	 */
	String[] getNaturalKeyNames() {
		return naturalKeyNames;
	}

	/*
	 * used for testing
	 */
	int[] getNaturalKeyPositions() {
		return naturalKeyPositionsInFeed;
	}

	private static int getDimensionLocalCacheSize() {
		final String propertyValue = System.getProperty(SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME);
		if (StringUtil.isEmpty(propertyValue)) {
			return SystemConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT;
		}
		return Integer.parseInt(propertyValue);
	}

}
