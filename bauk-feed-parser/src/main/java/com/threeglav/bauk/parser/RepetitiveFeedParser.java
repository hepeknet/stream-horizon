package com.threeglav.bauk.parser;

public final class RepetitiveFeedParser extends AbstractFeedParser {

	private String[] previousLine;
	private final int repetitionCount;
	private final String repeatedDelimitersOnly;
	private final int charactersToSkip;

	public RepetitiveFeedParser(final String delimiter, final int repetitionCount) {
		super(delimiter);
		if (repetitionCount <= 0) {
			throw new IllegalArgumentException("Repetition count must be positive integer");
		}
		this.repetitionCount = repetitionCount;
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < repetitionCount; i++) {
			sb.append(this.delimiter);
		}
		repeatedDelimitersOnly = sb.toString();
		charactersToSkip = repeatedDelimitersOnly.length();
	}

	public String[] parse(final String line) {
		// if (false && previousLine != null && line.indexOf(repeatedDelimitersOnly) == 0) {
		// final String[] splitNonRepeatedPart = super.splitLine(line, charactersToSkip);
		// final int finalArrayLength = repetitionCount + splitNonRepeatedPart.length;
		// final String[] finalArray = new String[finalArrayLength];
		// // System.out.println(line);
		// System.arraycopy(previousLine, 0, finalArray, 0, repetitionCount);
		// System.arraycopy(splitNonRepeatedPart, 0, finalArray, repetitionCount, splitNonRepeatedPart.length);
		// return finalArray;
		// }
		// else do one by one
		final String[] currentLine = super.splitLine(line);
		if (previousLine != null) {
			if (isParsedValueNull(currentLine[0])) {
				System.arraycopy(previousLine, 0, currentLine, 0, repetitionCount);
			}
		}
		previousLine = currentLine;
		return currentLine;
	}

}
