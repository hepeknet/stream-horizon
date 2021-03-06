package com.threeglav.sh.bauk.feed.processing;

import java.util.Map;

public interface FeedDataProcessor {

	void startFeed(final Map<String, String> globalAttributes);

	void processLine(final String line, final Map<String, String> globalAttributes);

	void processLastLine(final String line, final Map<String, String> globalAttributes);

	void closeFeed(int expectedResults, final Map<String, String> globalAttributes, boolean success);

}
