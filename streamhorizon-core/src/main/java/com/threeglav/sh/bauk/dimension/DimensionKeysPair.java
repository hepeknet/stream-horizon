package com.threeglav.sh.bauk.dimension;

public class DimensionKeysPair {

	public String lookupKey;
	public int surrogateKey;
	public String naturalKeyOnly;
	public String nonNaturalKeyOnly;

	@Override
	public String toString() {
		return "DimensionKeysPair [lookupKey=" + lookupKey + ", surrogateKey=" + surrogateKey + ", naturalKeyOnly=" + naturalKeyOnly
				+ ", nonNaturalKeyOnly=" + nonNaturalKeyOnly + "]";
	}

}
