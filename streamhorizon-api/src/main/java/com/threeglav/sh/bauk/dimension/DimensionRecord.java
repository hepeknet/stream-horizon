package com.threeglav.sh.bauk.dimension;

public final class DimensionRecord {

	private String[] naturalKeyValues;
	private Integer surrogateKey;

	/**
	 * Sets natural keys for this dimension record. Must not be null. Must be in order as defined in engine-config.xml
	 * file.
	 * 
	 * @param naturalKeyValues
	 */
	public void setNaturalKeyValues(final String[] naturalKeyValues) {
		this.naturalKeyValues = naturalKeyValues;
	}

	/**
	 * Sets surrogate key for this dimension record. Must not be null.
	 * 
	 * @param surrogateKey
	 */
	public void setSurrogateKey(final Integer surrogateKey) {
		this.surrogateKey = surrogateKey;
	}

	public String[] getNaturalKeyValues() {
		return naturalKeyValues;
	}

	public Integer getSurrogateKey() {
		return surrogateKey;
	}

}
