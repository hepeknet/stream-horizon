package com.threeglav.bauk.feed;

public interface FeedDataLineProcessor {

	/**
	 * Invoked once per feed line before that line is passed further for processing (just after line has been read from
	 * file). Must be fast - since it will be invoked once per every line in feed.
	 * 
	 * @param parsedDataLine
	 *            data read from file
	 * @return modified data. Must be same length as passed array
	 */
	String[] preProcessDataLine(String[] parsedDataLine);

}