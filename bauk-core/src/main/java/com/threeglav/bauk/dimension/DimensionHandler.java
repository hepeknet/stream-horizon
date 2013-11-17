package com.threeglav.bauk.dimension;

import gnu.trove.map.hash.THashMap;

import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.SystemConfigurationOptions;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.NaturalKey;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class DimensionHandler implements BulkLoadOutputValueHandler {

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

	public DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance, final DbHandler dbHandler,
			final int naturalKeyPositionOffset, final String routeIdentifier) {
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
		this.dbAccessCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - total database access times");
		this.localCacheClearCounter = MetricsUtil.createCounter("(" + routeIdentifier + ") Dimension [" + dimension.getName()
				+ "] - local cache clear times");
	}

	private void validate() {
		if (this.dimension.getNaturalKeys() == null || this.dimension.getNaturalKeys().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any natural key definitions for " + this.dimension.getName());
		}
		if (this.dimension.getSqlStatements() == null) {
			throw new IllegalArgumentException("Dimension " + this.dimension.getName()
					+ " did not define any sql statements! Check your configuration file!");
		}
		if (this.factFeed.getData() == null) {
			throw new IllegalArgumentException("Was not able to find definition of data for feed " + this.factFeed.getName());
		}
		if (this.factFeed.getData().getAttributes() == null || this.factFeed.getData().getAttributes().isEmpty()) {
			throw new IllegalArgumentException("Was not able to find any attributes defined in feed " + this.factFeed.getName());
		}
		if (this.dimension.getNaturalKeys().size() > this.factFeed.getData().getAttributes().size()) {
			throw new IllegalArgumentException("Dimension " + this.dimension.getName()
					+ " has more defined natural keys than there are attributes in feed " + this.factFeed.getName());
		}
	}

	private void calculatePositionOfNaturalKeys() {
		final int numberOfNaturalKeys = this.dimension.getNaturalKeys().size();
		this.naturalKeyNames = new String[numberOfNaturalKeys];
		this.naturalKeyPositionsInFeed = new int[numberOfNaturalKeys];
		this.log.debug("Calculating natural key position values. Will use offset {}", this.naturalKeyPositionOffset);
		int i = 0;
		for (final NaturalKey nk : this.dimension.getNaturalKeys()) {
			final String nkName = nk.getName();
			this.naturalKeyNames[i] = nkName;
			boolean found = false;
			this.log.debug("Trying to find position in feed for natural key {} for dimension {}", nkName, this.dimension.getName());
			final ArrayList<Attribute> factFeedDataAttributes = this.factFeed.getData().getAttributes();
			for (int j = 0; j < factFeedDataAttributes.size(); j++) {
				final Attribute attr = factFeedDataAttributes.get(j);
				final String attrName = attr.getName();
				if (nkName.equalsIgnoreCase(attrName)) {
					final int naturalKeyPositionValue = j + this.naturalKeyPositionOffset;
					this.naturalKeyPositionsInFeed[i++] = naturalKeyPositionValue;
					if (this.log.isDebugEnabled()) {
						this.log.debug("Natural key [{}] for dimension {} will be read from feed position {}",
								new Object[] { nkName, this.dimension.getName(), naturalKeyPositionValue });
					}
					found = true;
					break;
				}
			}
			if (!found) {
				throw new IllegalArgumentException("Was not able to find natural key " + nkName + " in feed " + this.factFeed.getName());
			}
		}
	}

	@Override
	public String getBulkLoadValue(final String[] parsedLine, final Map<String, String> headerAttributes, final Map<String, String> globalAttributes) {
		if (parsedLine == null || parsedLine.length == 0) {
			throw new IllegalArgumentException("Did not find any data in parsed line");
		}
		final String cacheKey = this.getNaturalKeyCacheKey(parsedLine);
		String surrogateKey = this.getSurrogateKeyFromCache(cacheKey);
		if (surrogateKey == null) {
			if (this.log.isTraceEnabled()) {
				this.log.trace("Did not find surrogate key for {} in cache. Going to database", this.dimension.getName());
			}
			surrogateKey = this.getSurrogateKeyFromDatabase(parsedLine, headerAttributes, globalAttributes);
			this.cacheInstance.cache(cacheKey, surrogateKey);
		} else {
			this.log.trace("Found surrogate key {} in cache. No need to hit database!", surrogateKey);
		}
		return surrogateKey;
	}

	private String getSurrogateKeyFromDatabase(final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		try {
			return this.tryInsertstatement(parsedLine, headerAttributes, globalAttributes);
		} catch (final Exception exc) {
			this.log.error("Failed inserting record {}. Will try to select value from database!", exc.getMessage());
		}
		return this.trySelectStatement(parsedLine, headerAttributes, globalAttributes);
	}

	private String trySelectStatement(final String[] parsedLine, final Map<String, String> headerValues, final Map<String, String> globalAttributes) {
		final String selectSurrogateKey = this.dimension.getSqlStatements().getSelectSurrogateKey();
		if (StringUtil.isEmpty(selectSurrogateKey)) {
			throw new IllegalArgumentException("Dimension select surrogate key statement is null or empty");
		}
		final String preparedStatement = this.prepareStatement(selectSurrogateKey, parsedLine, headerValues, globalAttributes);
		final Long result = this.dbHandler.executeQueryStatementAndReturnKey(preparedStatement);
		if (this.dbAccessCounter != null) {
			this.dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using select surrogate key statement " + preparedStatement);
		}
		this.log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		return result.toString();
	}

	private String tryInsertstatement(final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		final String insertStatement = this.dimension.getSqlStatements().getInsertSingle();
		if (StringUtil.isEmpty(insertStatement)) {
			throw new IllegalArgumentException("Dimension insert statement is null or empty");
		}
		final String preparedStatement = this.prepareStatement(insertStatement, parsedLine, headerAttributes, globalAttributes);
		final Long result = this.dbHandler.executeInsertStatementAndReturnKey(preparedStatement);
		if (this.dbAccessCounter != null) {
			this.dbAccessCounter.inc();
		}
		if (result == null) {
			throw new IllegalStateException("Was not able to retrieve surrogate key by using insert statement " + preparedStatement);
		}
		this.log.debug("Retrieved surrogate key {} for {}", result, preparedStatement);
		return result.toString();
	}

	private String prepareStatement(final String statement, final String[] parsedLine, final Map<String, String> headerAttributes,
			final Map<String, String> globalAttributes) {
		if (this.log.isDebugEnabled()) {
			final String placeHolderFormat = Constants.STATEMENT_PLACEHOLDER_DELIMITER_START + "ATTRIBUTE_NAME"
					+ Constants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			this.log.debug("Will prepare statement {}. Replacing all placeholders {} with their real values (from feed or header)", statement,
					placeHolderFormat);
		}
		String stat = this.replaceAllNaturalKeys(statement, parsedLine);
		this.log.debug("Replacing all global attributes {}", globalAttributes);
		stat = StringUtil.replaceAllAttributes(stat, globalAttributes, Constants.GLOBAL_ATTRIBUTE_PREFIX);
		this.log.debug("Replacing all header attributes {}", headerAttributes);
		stat = StringUtil.replaceAllAttributes(stat, headerAttributes, Constants.HEADER_ATTRIBUTE_PREFIX);
		this.log.debug("Final statement is {}", stat);
		return stat;
	}

	private String replaceAllNaturalKeys(final String statement, final String[] parsedLine) {
		String replaced = statement;
		for (int i = 0; i < this.naturalKeyNames.length; i++) {
			final String nkPlacholder = Constants.STATEMENT_PLACEHOLDER_DELIMITER_START + this.naturalKeyNames[i]
					+ Constants.STATEMENT_PLACEHOLDER_DELIMITER_END;
			final int nkValuePosition = this.naturalKeyPositionsInFeed[i];
			if (parsedLine.length < nkValuePosition) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + nkValuePosition);
			}
			final String value = parsedLine[nkValuePosition];
			this.log.debug("Replacing {} with {}", nkPlacholder, value);
			replaced = replaced.replace(nkPlacholder, value);
		}
		return replaced;
	}

	private String getSurrogateKeyFromCache(final String cacheKey) {
		final String locallyCachedValue = this.localCache.get(cacheKey);
		if (locallyCachedValue != null) {
			return locallyCachedValue;
		} else {
			final String cachedValue = this.cacheInstance.getSurrogateKey(cacheKey);
			if (cachedValue != null) {
				if (this.localCache.size() > MAX_ELEMENTS_LOCAL_MAP) {
					this.log.debug("Local cache has more than {} elements. Have to clear it!", MAX_ELEMENTS_LOCAL_MAP);
					this.localCache.clear();
					if (this.localCacheClearCounter != null) {
						this.localCacheClearCounter.inc();
					}
				}
				this.localCache.put(cacheKey, cachedValue);
			}
			return cachedValue;
		}
	}

	private String getNaturalKeyCacheKey(final String[] parsedLine) {
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.naturalKeyPositionsInFeed.length; i++) {
			final int key = this.naturalKeyPositionsInFeed[i];
			if (i != 0) {
				sb.append(Constants.NATURAL_KEY_DELIMITER);
			}
			if (parsedLine.length <= key) {
				throw new IllegalArgumentException("Parsed line has less values than needed " + key);
			}
			sb.append(parsedLine[key]);
		}
		this.log.trace("Calculated key for cache for {} is {}", this.dimension.getName(), sb);
		return sb.toString();
	}

	/*
	 * used for testing
	 */
	String[] getNaturalKeyNames() {
		return this.naturalKeyNames;
	}

	/*
	 * used for testing
	 */
	int[] getNaturalKeyPositions() {
		return this.naturalKeyPositionsInFeed;
	}

	private static int getDimensionLocalCacheSize() {
		final String propertyValue = System.getProperty(SystemConfigurationOptions.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME);
		if (StringUtil.isEmpty(propertyValue)) {
			return SystemConfigurationOptions.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT;
		}
		return Integer.parseInt(propertyValue);
	}

}
