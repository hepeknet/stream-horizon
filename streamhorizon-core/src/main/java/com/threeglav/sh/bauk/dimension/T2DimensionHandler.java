package com.threeglav.sh.bauk.dimension;

import java.util.Map;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;
import com.threeglav.sh.bauk.util.StringUtil;

public class T2DimensionHandler extends T1DimensionHandler {

	private final Counter dbAccessRetireCounter;
	private final StatefulAttributeReplacer retireRecordReplacer;

	public T2DimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(dimension, factFeed, cacheInstance, naturalKeyPositionOffset, config);
		if (dimension.getSqlStatements() != null && !StringUtil.isEmpty(dimension.getSqlStatements().getRetireSingleRecord())) {
			retireRecordReplacer = new StatefulAttributeReplacer(dimension.getSqlStatements().getRetireSingleRecord(), dbStringLiteral,
					dbStringEscapeLiteral);
		} else {
			throw new IllegalStateException(dimension.getType() + " dimension " + dimension.getName()
					+ " must have retireSingleRecord statement defined");
		}
		this.checkNoNaturalKeysExist();
		this.findAllNonNaturalKeys();
		dbAccessRetireCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database retire statements executed");
	}

	@Override
	protected Integer doPerformRecordUpdate(final String[] parsedLine, final Map<String, String> globalAttributes, final String lookupKey) {
		final String preparedUpdateStatement = this.prepareStatement(parsedLine, globalAttributes, retireRecordReplacer);
		this.getDbHandler().executeInsertOrUpdateStatement(preparedUpdateStatement, "Retire single record statement for " + dimension.getName());
		final Integer newlyCreatedSurrogateKey = this.doExecuteInsertStatement(parsedLine, globalAttributes, null);
		if (dbAccessRetireCounter != null) {
			dbAccessRetireCounter.inc();
		}
		this.updateKeysInCache(parsedLine, newlyCreatedSurrogateKey);
		final Integer surrogateKey = dimensionCache.getSurrogateKeyFromCache(lookupKey);
		if (surrogateKey == null) {
			throw new IllegalStateException("Performed update operation but still was not able to find surrogate key for lookup [" + lookupKey + "]");
		}
		if (isDebugEnabled) {
			log.debug("Surrogate key after update is {}. Lookup key is {}", surrogateKey, lookupKey);
		}
		return surrogateKey;
	}

	private void updateKeysInCache(final String[] parsedLine, final Integer surrogateKey) {
		final StringBuilder nonNaturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNonNaturalKeyValues(parsedLine, nonNaturalKey);
		final String nonNaturalKeyFromFeed = nonNaturalKey.toString();
		final StringBuilder naturalKey = new StringBuilder(StringUtil.DEFAULT_STRING_BUILDER_CAPACITY);
		this.concatenateAllNaturalKeyValues(parsedLine, naturalKey);
		final String naturalKeyFromFeed = naturalKey.toString();
		final String nonNaturalKeyFromCache = naturalKeyToNonNaturalKeyMapping.get(naturalKeyFromFeed);
		final String oldLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromCache;
		dimensionCache.removeFromCache(oldLookupKey);
		final String newLookupKey = naturalKeyFromFeed + BaukConstants.NATURAL_NON_NATURAL_DELIMITER + nonNaturalKeyFromFeed;
		dimensionCache.putInCache(newLookupKey, surrogateKey);
		naturalKeyToNonNaturalKeyMapping.put(naturalKeyFromFeed, nonNaturalKeyFromFeed);
	}

}
