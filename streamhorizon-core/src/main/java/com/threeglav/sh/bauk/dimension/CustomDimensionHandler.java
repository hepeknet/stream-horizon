package com.threeglav.sh.bauk.dimension;

import java.util.Map;

import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.util.StringUtil;

public class CustomDimensionHandler extends AbstractDimensionHandler {

	private final SurrogateKeyProvider surrogateKeyProvider;

	public CustomDimensionHandler(final Feed factFeed, final BaukConfiguration config, final Dimension dimension, final int naturalKeyPositionOffset,
			final CacheInstance cacheInstance) {
		super(factFeed, config, dimension, naturalKeyPositionOffset, cacheInstance);
		if (dimension.getSqlStatements() != null) {
			throw new IllegalStateException("Specifying SqlStatements for dimension of type " + dimension.getType()
					+ " is not allowed. Problematic dimension is " + dimension.getName());
		}
		surrogateKeyProvider = this.loadSurrogateKeyProvider();
		log.debug("Successfully initialized surrogate key provider for dimension {}", this.getDimension().getName());
	}

	private SurrogateKeyProvider loadSurrogateKeyProvider() {
		final String clazzName = this.getDimension().getSurrogateKeyProviderClassName();
		if (StringUtil.isEmpty(clazzName)) {
			throw new IllegalStateException("For dimensions of type " + dimension.getType()
					+ " it is required to provide surrogateKeyProviderClassName attribute!");
		}
		log.debug("Surrogate key provider {}", clazzName);
		final CustomProcessorResolver<SurrogateKeyProvider> resolver = new CustomProcessorResolver<>(clazzName, SurrogateKeyProvider.class);
		final SurrogateKeyProvider provider = resolver.resolveInstance();
		if (provider == null) {
			throw new IllegalStateException("Was not able to find class by name " + clazzName);
		}
		return provider;
	}

	private String[] getOnlyMappedColumns(final String[] parsedLine) {
		final String[] mappedColumnsOnly = new String[this.getMappedColumnPositionsInFeed().length];
		for (int i = 0; i < this.getMappedColumnPositionsInFeed().length; i++) {
			mappedColumnsOnly[i] = parsedLine[this.getMappedColumnPositionsInFeed()[i]];
		}
		return mappedColumnsOnly;
	}

	@Override
	public Object getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		final String naturalCacheKey = this.buildNaturalKeyForCacheLookup(parsedLine, globalValues);
		final Object surrogateKey = dimensionCache.getSurrogateKeyFromCache(naturalCacheKey);
		if (surrogateKey == null) {
			if (isDebugEnabled) {
				log.debug("Was not able to find mapping for natural key {} in the cache, will lookup custom dimension", naturalCacheKey);
			}
			return surrogateKeyProvider.getSurrogateKeyValue(this.getOnlyMappedColumns(parsedLine), globalValues);
		}
		if (isDebugEnabled) {
			log.debug("Found mapping {}->{}", naturalCacheKey, surrogateKey);
		}
		return surrogateKey;
	}

	@Override
	public Object getLastLineBulkLoadValue(final String[] parsedLine, final Map<String, String> globalValues) {
		return this.getBulkLoadValue(parsedLine, globalValues);
	}

	@Override
	public void closeCurrentFeed() {
		// NOOP
	}

	@Override
	public boolean closeShouldBeInvoked() {
		return false;
	}

}
