package com.threeglav.sh.bauk.feed;

import java.util.Map;

public interface FeedDataLineProcessor {

	/**
	 * Invoked once, after processor has been created. This method is invoked only once and before any processing is
	 * done and should be used to initialize processor.
	 * 
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	void init(Map<String, String> engineConfigurationProperties);

	/**
	 * Invoked once per feed line before that line is passed further for processing (just after line has been read from
	 * file). Must be fast - since it will be invoked once per every line in feed.
	 * <p>
	 * It is very important to keep this method fast because it is executed once per every row in input feed files.
	 * 
	 * @param parsedDataLine
	 *            parsed data row read from file
	 * @param globalAttributes
	 *            global attributes available at the moment when row is being parsed. It is possible to modify these
	 *            attributes in this method.
	 * @return modified data. Must be the same length as passed array
	 */
	String[] preProcessDataLine(String[] parsedDataLine, Map<String, String> globalAttributes);

}