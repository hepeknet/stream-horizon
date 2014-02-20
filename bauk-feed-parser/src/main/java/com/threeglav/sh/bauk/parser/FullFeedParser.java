package com.threeglav.sh.bauk.parser;

public final class FullFeedParser extends AbstractFeedParser {

	private static final int DEFAULT_EXPECTED_TOKENS = 50;

	public FullFeedParser(final String delim, final int expectedTokens) {
		super(delim, expectedTokens);
	}

	public FullFeedParser(final String delim) {
		super(delim, DEFAULT_EXPECTED_TOKENS);
	}

	@Override
	public String[] parse(final String line) {
		return super.splitLine(line);
	}

}