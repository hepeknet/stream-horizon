package com.threeglav.bauk;

import java.util.Map;

public interface BulkLoadOutputValueHandler {

	void calculatePerFeedValues(Map<String, String> globalValues);

	String getBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	void closeCurrentFeed();

}