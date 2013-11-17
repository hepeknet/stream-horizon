package com.threeglav.bauk;

import java.util.Map;

public interface BulkLoadOutputValueHandler {

	public abstract String getBulkLoadValue(String[] parsedLine, Map<String, String> headerValues, Map<String, String> globalValues);

}