package com.threeglav.sh.bauk.dimension;

import java.util.Collection;
import java.util.Map;

public interface DimensionDataProvider {

	/**
	 * Invoked once, after data provider has been created. This method is invoked only once and before any processing is
	 * done and should be used to initialize this data provider.
	 * 
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	void init(Map<String, String> engineConfigurationProperties);

	/**
	 * Invoked by engine to retrieve any additional dimension records. Can be invoked multiple times. This method should
	 * return all additional dimension records (additional to records returned by precache statement for that particular
	 * dimension, if any). This method is invoked on engine startup and after every dimension cache flushing.
	 * 
	 * @return collection of all additional dimension records
	 */
	Collection<DimensionRecord> getDimensionRecords();

}
