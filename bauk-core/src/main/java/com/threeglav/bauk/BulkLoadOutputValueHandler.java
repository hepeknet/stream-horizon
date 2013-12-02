package com.threeglav.bauk;

import java.util.Map;

public interface BulkLoadOutputValueHandler {

	String getBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	void closeCurrentFeed();

}