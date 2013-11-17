package com.threeglav.bauk.chain.components;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

public class TestDataCreator {

	private static final Random r = new Random();

	public static void main(final String[] args) throws Exception {
		final String file = "d:/projects/test/inputBig.txt";
		final int numberOfLines = 1000000;
		final String delimiter = "@@";
		final BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		writer.write("a,header\n");
		for (int i = 0; i < numberOfLines; i++) {
			// final String line = generateFullFeedRow(i, delimiter);
			final String line = generateCurrencyRow(i, delimiter);
			writer.write(line + "\n");
		}
		writer.write("9" + delimiter + numberOfLines);
		writer.close();
	}

	private static String generateCurrencyRow(final int currentRow, final String delimiter) {
		final String[] currencies = { "dinar", "dollar", "pound", "euro" };
		final String[] bases = { "RSD", "USD", "GBP", "EUR" };
		final Random r = new Random();
		final int rn = r.nextInt(4);
		return "cc" + delimiter + currencies[rn] + delimiter + bases[rn] + delimiter + r.nextDouble() + delimiter
				+ r.nextDouble() + delimiter + currencies[rn] + delimiter + UUID.randomUUID().toString() + delimiter
				+ r.nextDouble() + delimiter + UUID.randomUUID().toString() + r.nextDouble() + delimiter
				+ UUID.randomUUID().toString() + delimiter + r.nextFloat() + delimiter + r.nextDouble();
	}

	private static String generateFullFeedRow(final int currentRow, final String delimiter) {
		final int numOfColumns = 33;
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < numOfColumns; i++) {
			if (i != 0) {
				sb.append(delimiter);
			}
			sb.append(UUID.randomUUID().toString());
		}
		sb.append(delimiter);
		sb.append(r.nextDouble());
		sb.append(delimiter);
		sb.append(r.nextDouble());
		sb.append("\n");
		return sb.toString();
	}

}
