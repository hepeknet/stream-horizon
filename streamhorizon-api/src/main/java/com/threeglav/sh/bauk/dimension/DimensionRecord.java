package com.threeglav.sh.bauk.dimension;

public final class DimensionRecord {

	private String[] naturalKeyValues;
	private Integer surrogateKey;

	public void setNaturalKeyValues(final String[] naturalKeyValues) {
		this.naturalKeyValues = naturalKeyValues;
	}

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
