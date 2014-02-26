package com.threeglav.sh.bauk.parser;

public final class DeltaFeedParser extends AbstractFeedParser {

	private String[] previousLine;

	public DeltaFeedParser(final String delimiter, final int expectedTokens) {
		super(delimiter, expectedTokens);
	}

	@Override
	public String[] parse(final String line) {
		final String[] currentLine = super.splitLine(line);
		if (previousLine != null) {
			for (int i = 0; i < currentLine.length; i++) {
				if (this.isParsedValueNull(currentLine[i])) {
					currentLine[i] = previousLine[i];
				} else if (this.isNullStringValue(currentLine[i])) {
					currentLine[i] = null;
				}
			}
		}
		previousLine = currentLine;
		return currentLine;
	}

}
