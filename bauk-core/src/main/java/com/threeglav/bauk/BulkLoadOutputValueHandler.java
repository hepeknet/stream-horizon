package com.threeglav.bauk;

import java.util.Map;

public interface BulkLoadOutputValueHandler {

	void calculatePerFeedValues(Map<String, String> globalValues);

	Object getBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	Object getLastLineBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	boolean hasCalculatePerFeedValues();

	void closeCurrentFeed();

	boolean closeShouldBeInvoked();

}