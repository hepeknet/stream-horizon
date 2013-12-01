package com.threeglav.bauk.feed;

import java.util.Map;

public interface FeedFileNameProcessor {

	Map<String, String> parseFeedFileName(String feedFileName);

}
