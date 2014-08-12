package com.threeglav.sh.bauk.dimension;

import java.util.Map;

/**
 * Interface to be implemented by custom surrogate key providers.
 * 
 * 
 */
public interface SurrogateKeyProvider {

	/**
	 * Method invoked by StreamHorizon engine when it can not find mapping natural key -> surrogate key.
	 * 
	 * @param naturalKeyValues
	 *            the values for natural key for which surrogate key was not found. Ordered as defined in configuration
	 *            file.
	 * @param globalAttributes
	 *            all context attributes available
	 * @param dimensionCache
	 *            the cache for this dimension
	 * 
	 * @return surrogate key. Can be null.
	 */
	Object getSurrogateKeyValue(final String[] naturalKeyValues, final Map<String, String> globalAttributes, DimensionCache dimensionCache);

}
