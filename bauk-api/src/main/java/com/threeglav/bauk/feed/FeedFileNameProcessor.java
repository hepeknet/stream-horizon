package com.threeglav.bauk.feed;

import java.util.Map;

public interface FeedFileNameProcessor {

	/**
	 * Invoked once, after processor has been created. This method is invoked only once and before any processing is
	 * done and should be used to initialize processor.
	 * 
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	void init(Map<String, String> engineConfigurationProperties);

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
