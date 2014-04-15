package com.threeglav.sh.bauk.dimension;

import java.util.Map;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

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
		dbAccessRetireCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - total database retire statement executions");
	}

	@Override
	protected Integer doPerformRecordUpdate(final String[] parsedLine, final Map<String, String> globalAttributes, final String lookupKey) {
		final TransactionTemplate txTemplate = this.getDbHandler().getTransactionTemplate();
		return txTemplate.execute(new TransactionCallback<Integer>() {

			@Override
			public Integer doInTransaction(final TransactionStatus status) {
				final String preparedUpdateStatement = T2DimensionHandler.this.prepareStatement(parsedLine, globalAttributes, retireRecordReplacer);
				int retiredRows = 0;
				try {
					retiredRows = T2DimensionHandler.this.getDbHandler().executeInsertOrUpdateStatement(preparedUpdateStatement,
							"Retire single record statement for " + dimension.getName());
					if (dbAccessRetireCounter != null) {
						dbAccessRetireCounter.inc();
					}
				} catch (final DuplicateKeyException dexc) {
					log.warn("Duplicate key exception while trying to retire row. Statement was {}. Will try to look-up key from database",
							preparedUpdateStatement);
				}
				if (retiredRows == 1) {
					final Integer newlyCreatedSurrogateKey = T2DimensionHandler.this
							.doExecuteInsertStatement(parsedLine, globalAttributes, lookupKey);
					if (newlyCreatedSurrogateKey != null) {
						T2DimensionHandler.this.updateKeysInCache(parsedLine, newlyCreatedSurrogateKey);
					}
				}
				Integer surrogateKey = dimensionCache.getSurrogateKeyFromCache(lookupKey);
				if (surrogateKey == null) {
					final Integer surrogateKeyFromDatabase = T2DimensionHandler.this.getSurrogateKeyFromDatabase(parsedLine, globalAttributes,
							lookupKey);
					if (surrogateKeyFromDatabase == null) {
						throw new IllegalStateException(
								"Performed update operation and lookup in cache and database but still was not able to find surrogate key for lookup ["
										+ lookupKey + "]");
					}
					if (isDebugEnabled) {
						log.debug("Was not able to find record for {} in cache but found it in database = {}. Retired rows = {}", lookupKey,
								surrogateKeyFromDatabase, retiredRows);
					}
					surrogateKey = surrogateKeyFromDatabase;
				}
				if (isDebugEnabled) {
					log.debug("Surrogate key after update is {}. Lookup key is {}", surrogateKey, lookupKey);
				}
				return surrogateKey;
			}
		});
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
