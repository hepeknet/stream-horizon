package com.threeglav.bauk.parser;

public final class FullFeedParser extends AbstractFeedParser {

	public FullFeedParser(final String delim) {
		super(delim);
	}

	public String[] parse(final String line) {
		return super.splitLine(line);
	}

}