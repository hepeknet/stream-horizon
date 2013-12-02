package com.threeglav.bauk.feed;

import java.util.Map;

public interface FeedDataProcessor {

	void startFeed(final Map<String, String> globalAttributes);

	void processLine(final String line);

	void closeFeed(int expectedResults);

}
