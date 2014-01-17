package com.threeglav.bauk.feed;

import java.util.Map;

public interface FeedFileNameProcessor {

	/**
	 * Method for parsing feed file name and providing values directly as attributes in context. Whatever is returned by
	 * this method will be available as context attributes.
	 * 
	 * @param feedFileName
	 *            the name of original feed file
	 * @return map of context attribute names and values. Can return null
	 */
	Map<String, String> parseFeedFileName(String feedFileName);

}
