package com.threeglav.sh.bauk;

import java.util.Map;

public interface BulkLoadOutputValueHandler {

	Object getBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	Object getLastLineBulkLoadValue(String[] parsedLine, Map<String, String> globalValues);

	void closeCurrentFeed();

	boolean closeShouldBeInvoked();

}