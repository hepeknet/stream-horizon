package com.threeglav.bauk.dimension;

import java.util.Map;

import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;

public class CachedPerFeedDimensionHandler extends DimensionHandler {

	private Integer keyValueCachedPerFeed;

	public CachedPerFeedDimensionHandler(final Dimension dimension, final FactFeed factFeed, final CacheInstance cacheInstance,
			final int naturalKeyPositionOffset, final BaukConfiguration config) {
		super(dimension, factFeed, cacheInstance, naturalKeyPositionOffset, config);
		log.info(
				"For dimension {} caching-per-feed is enabled. This means that dimension calculation will be executed only once and before processing any data from feed (only global attributes will be accessible)!",
				dimension.getName());
	}

	@Override
	public Integer getBulkLoadValue(final String[] parsedLine, final Map<String, String> globalAttributes) {
		if (keyValueCachedPerFeed != null) {
			return keyValueCachedPerFeed;
		}
		final Integer surrogateKey = super.getBulkLoadValue(parsedLine, globalAttributes);
		log.debug("Cached surrogate key {} for dimension {} for feed", surrogateKey, dimension.getName());
		keyValueCachedPerFeed = surrogateKey;
		if (keyValueCachedPerFeed != null) {
			globalAttributes.put(dimension.getCacheKeyPerFeedInto(), String.valueOf(keyValueCachedPerFeed));
			log.trace("After caching per feed global attributes are {}", globalAttributes);
		}
		return surrogateKey;
	}

	@Override
	public void closeCurrentFeed() {
		keyValueCachedPerFeed = null;
	}

	@Override
	public void calculatePerFeedValues(final Map<String, String> globalValues) {
		log.debug("Global attributes before per-feed calculation {}", globalValues);
		this.getBulkLoadValue(null, globalValues);
		log.debug("Global attributes after per-feed calculation {}", globalValues);
	}

}
