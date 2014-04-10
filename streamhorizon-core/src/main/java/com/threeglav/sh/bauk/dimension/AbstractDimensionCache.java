package com.threeglav.sh.bauk.dimension;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.events.EngineEvents;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.util.CacheUtil;
import com.threeglav.sh.bauk.util.MetricsUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public abstract class AbstractDimensionCache implements Observer, DimensionCache {

	static final boolean LOCAL_CACHE_DISABLED = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.DIMENSION_LOCAL_CACHE_DISABLED, false);

	static final int MAX_ELEMENTS_LOCAL_MAP = ConfigurationProperties.getSystemProperty(
			BaukEngineConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_PARAM_NAME,
			BaukEngineConfigurationConstants.DIMENSION_LOCAL_CACHE_SIZE_DEFAULT);

	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	protected final int maxElementsInLocalCache;

	protected final CacheInstance cacheInstance;

	private final CacheInstance dimensionDataProviderCache;

	protected Counter localCacheClearCounter;

	protected final Counter dimensionCacheFlushCounter;

	protected final Dimension dimension;

	protected final boolean isDebugEnabled;

	private final DimensionDataProvider dimensionDataProvider;

	private final boolean dimensionDataProviderAvailable;

	public AbstractDimensionCache(final CacheInstance cacheInstance, final Dimension dimension) {
		if (dimension == null) {
			throw new IllegalArgumentException("Dimension must not be null");
		}
		this.dimension = dimension;
		int maxElementsInLocalCacheForDimension = MAX_ELEMENTS_LOCAL_MAP;
		if (dimension.getLocalCacheMaxSize() != null) {
			maxElementsInLocalCacheForDimension = dimension.getLocalCacheMaxSize().intValue();
		}
		log.info("For dimension {} local cache will hold at most {} elements", dimension.getName(), maxElementsInLocalCacheForDimension);
		this.cacheInstance = cacheInstance;
		if (!LOCAL_CACHE_DISABLED) {
			localCacheClearCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - fast cache resets");
		}
		dimensionCacheFlushCounter = MetricsUtil.createCounter("Dimension [" + dimension.getName() + "] - cache flush executions");
		maxElementsInLocalCache = maxElementsInLocalCacheForDimension;
		isDebugEnabled = log.isDebugEnabled();
		EngineEvents.registerForFlushDimensionCache(this);
		dimensionDataProvider = this.resolveDimensionDataProvider();
		this.initializeDimensionDataProvider();
		dimensionDataProviderAvailable = dimensionDataProvider != null;
		if (dimensionDataProviderAvailable) {
			dimensionDataProviderCache = CacheUtil.getCacheInstanceManager().getCacheInstance(dimension.getName() + "_plugged_data");
			this.populateProvidedDimensionData();
		} else {
			dimensionDataProviderCache = null;
		}
	}

	protected void initializeDimensionDataProvider() {
		if (dimensionDataProvider != null) {
			try {
				dimensionDataProvider.init(ConfigurationProperties.getEngineConfigurationProperties());
				log.debug("Successfully initialized plugin class {}", dimensionDataProvider);
			} catch (final Exception exc) {
				log.error("Exception while initializing plugin {}. Details {}", dimensionDataProvider, exc.getMessage());
				throw new IllegalStateException("Exception while initializing plugin class " + dimensionDataProvider, exc);
			}
		}
	}

	private DimensionDataProvider resolveDimensionDataProvider() {
		final String dataProviderClassName = dimension.getDimensionDataProviderClassName();
		if (StringUtil.isEmpty(dataProviderClassName)) {
			return null;
		}
		final CustomProcessorResolver<DimensionDataProvider> resolver = new CustomProcessorResolver<>(dataProviderClassName,
				DimensionDataProvider.class);
		return resolver.resolveInstance();
	}

	protected abstract Integer getFromLocalCache(final String cacheKey);

	@Override
	public Integer getSurrogateKeyFromCache(final String cacheKey) {
		final Integer valueInLocalCache = this.getFromLocalCache(cacheKey);
		if (valueInLocalCache != null) {
			return valueInLocalCache;
		}
		Integer cachedValue = cacheInstance.getSurrogateKey(cacheKey);
		if (cachedValue != null) {
			if (!LOCAL_CACHE_DISABLED) {
				this.clearLocalCacheIfNeeded();
				this.putInLocalCache(cacheKey, cachedValue);
			}
		} else {
			if (dimensionDataProviderAvailable) {
				cachedValue = dimensionDataProviderCache.getSurrogateKey(cacheKey);
			}
		}
		return cachedValue;
	}

	@Override
	public int putAllInCache(final List<DimensionKeysPair> values) {
		final int batchSize = 10000;
		final Iterator<DimensionKeysPair> iter = values.iterator();
		final Map<String, Integer> valuesToCache = new HashMap<>();
		int valuesCached = 0;
		while (iter.hasNext()) {
			if (valuesToCache.size() == batchSize) {
				cacheInstance.putAll(valuesToCache);
				this.putAllToLocalCache(valuesToCache);
				valuesCached += valuesToCache.size();
				valuesToCache.clear();
			}
			final DimensionKeysPair row = iter.next();
			final int surrogateKeyValue = row.surrogateKey;
			final String naturalKeyValue = row.lookupKey;
			valuesToCache.put(naturalKeyValue, surrogateKeyValue);
		}
		if (!valuesToCache.isEmpty()) {
			cacheInstance.putAll(valuesToCache);
			this.putAllToLocalCache(valuesToCache);
			valuesCached += valuesToCache.size();
			valuesToCache.clear();
		}
		return valuesCached;
	}

	protected abstract void putAllToLocalCache(final Map<String, Integer> valuesToCache);

	@Override
	public void putInCache(final String cacheKey, final int cachedValue) {
		cacheInstance.put(cacheKey, cachedValue);
		if (!LOCAL_CACHE_DISABLED) {
			this.clearLocalCacheIfNeeded();
			this.putInLocalCache(cacheKey, cachedValue);
		}
	}

	private void clearLocalCacheIfNeeded() {
		if (this.getLocalCacheSize() > maxElementsInLocalCache) {
			if (isDebugEnabled) {
				log.debug("Local cache for dimension {} has more than {} elements. Have to clear it!", dimension.getName(), maxElementsInLocalCache);
			}
			this.clearLocalCache();
			if (localCacheClearCounter != null) {
				localCacheClearCounter.inc();
			}
		}
	}

	protected abstract void putInLocalCache(final String cacheKey, final int cachedValue);

	protected abstract void clearLocalCache();

	@Override
	public void update(final Observable o, final Object arg) {
		final String dimensionName = (String) arg;
		log.debug("Got request to flush cache for dimension {}", dimensionName);
		if (dimensionName.equals(dimension.getName())) {
			log.debug("Matches with dimension I am responsible for {}", dimensionName);
			this.clearLocalCache();
			cacheInstance.clear();
			if (dimensionCacheFlushCounter != null) {
				dimensionCacheFlushCounter.inc();
			}
			if (dimensionDataProviderAvailable) {
				dimensionDataProviderCache.clear();
				this.populateProvidedDimensionData();
			}
			log.info("Cleared caches for dimension {}", dimensionName);
		}
	}

	private void populateProvidedDimensionData() {
		try {
			final Collection<DimensionRecord> data = dimensionDataProvider.getDimensionRecords();
			if (data != null) {
				for (final DimensionRecord dr : data) {
					final StringBuilder sb = new StringBuilder();
					final String[] naturalKeys = dr.getNaturalKeyValues();
					if (naturalKeys == null || naturalKeys.length == 0) {
						throw new IllegalArgumentException("For dimension " + dimension.getName()
								+ " one of additional dimension records has null natural key!");
					}
					final Integer surrogateKey = dr.getSurrogateKey();
					if (surrogateKey == null) {
						throw new IllegalArgumentException("For dimension " + dimension.getName()
								+ " one of additional dimension records has null surrogate key!");
					}
					for (int i = 0; i < naturalKeys.length; i++) {
						if (i != 0) {
							sb.append(BaukConstants.NATURAL_KEY_DELIMITER);
						}
						sb.append(naturalKeys[i]);
					}
					final String naturalKey = sb.toString();
					dimensionDataProviderCache.put(naturalKey, surrogateKey);
				}
				log.debug("Successfully retrieved {} additiona records for dimension {}", data.size(), dimension.getName());
			}
		} catch (final Exception exc) {
			log.error("Exception while retrieving dimension records from {}. Details {}", dimensionDataProvider, exc);
			throw new IllegalStateException("Exception while retrieving dimension records from " + dimensionDataProvider, exc);
		}
	}

	protected abstract void removeFromLocalCache(final String cacheKey);

	protected abstract int getLocalCacheSize();

	@Override
	public void removeFromCache(final String cacheKey) {
		cacheInstance.remove(cacheKey);
		this.removeFromLocalCache(cacheKey);
	}

}
