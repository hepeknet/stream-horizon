package com.threeglav.bauk.parser;

public final class DeltaFeedParser extends AbstractFeedParser {

	private String[] previousLine;

	public DeltaFeedParser(final String delimiter) {
		super(delimiter);
	}

	public String[] parse(final String line) {
		final String[] currentLine = super.splitLine(line);
		if (previousLine != null) {
			for (int i = 0; i < currentLine.length; i++) {
				if (isNullOrEmpty(currentLine[i])) {
					currentLine[i] = previousLine[i];
				} else if (isNullStringValue(currentLine[i])) {
					currentLine[i] = null;
				}
			}
		}
		previousLine = currentLine;
		return currentLine;
	}

}
