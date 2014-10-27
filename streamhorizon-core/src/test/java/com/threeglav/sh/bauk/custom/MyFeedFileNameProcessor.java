package com.threeglav.sh.bauk.custom;

import java.util.HashMap;
import java.util.Map;

import com.threeglav.sh.bauk.feed.FeedFileNameProcessor;

public class MyFeedFileNameProcessor implements FeedFileNameProcessor {

	@Override
	public void init(final Map<String, String> engineConfigurationProperties) {

	}

	@Override
	public Map<String, String> parseFeedFileName(final String feedFileName) {
		final Map<String, String> ctx = new HashMap<String, String>();
		ctx.put("my1", "11111");
		ctx.put("my2", "22222");
		return ctx;
	}

}
