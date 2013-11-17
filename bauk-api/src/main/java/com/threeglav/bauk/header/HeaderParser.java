package com.threeglav.bauk.header;

import java.util.Map;

public interface HeaderParser {

	public abstract Map<String, String> parseHeader(String headerLine, String[] declaredAttributeNames,
			String startsWithString, String delimiter);

}