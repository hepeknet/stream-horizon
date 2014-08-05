package com.threeglav.sh.bauk.integration.plugins;

import java.util.Map;

import com.threeglav.sh.bauk.dimension.SurrogateKeyProvider;

public class TestSurrogateKeyProvider implements SurrogateKeyProvider {

	@Override
	public Object getSurrogateKeyValue(final String[] naturalKeyValues, final Map<String, String> globalAttributes) {
		if (naturalKeyValues.length != 2) {
			return "Error, not of length 2";
		}
		if (naturalKeyValues[0].equals("a11") && naturalKeyValues[1].equals("b22")) {
			return "314159";
		} else {
			return "42";
		}
	}

}
