/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2012
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.threeglav.bauk.wh.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class RandomizeX {

	private static final Random rand = new Random();
	private static final int[] positions = new int[] { 13, 14, 15 };
	private static final DateFormat df = new SimpleDateFormat("YYYYMMdd");
	private static final int MAX_ROWS_IN_FILE = 100000;
	private static final int INITIAL_ROWS = 200;
	private static final int REPEAT_RATIO = 30;

	/**
	 * @param args
	 */
	public static void main(final String[] args) throws Exception {
		final BufferedReader br = new BufferedReader(new FileReader("d:/projects/test/sh_sales_big_16.csv"));
		final BufferedWriter bw = new BufferedWriter(new FileWriter("d:/projects/test/sh_sales_big_16_rand.csv"));
		int totalCount = 0;
		String line = br.readLine();
		while (line != null) {
			if (totalCount >= MAX_ROWS_IN_FILE) {
				break;
			}
			line = br.readLine();
			if (line != null) {
				final String rLine = randomizeLine(line);
				if (totalCount > INITIAL_ROWS) {
					for (int i = 0; i < REPEAT_RATIO; i++) {
						if (rLine != null) {
							bw.write(rLine);
							bw.newLine();
							totalCount++;
						}
					}
				} else {
					if (rLine != null) {
						bw.write(rLine);
						bw.newLine();
						totalCount++;
					}
				}
			}
		}
		br.close();
		bw.close();
	}

	private static String randomizeLine(final String line) {
		final String[] data = line.split(",");
		if (data.length < 10) {
			return null;
		}
		final Date d = new Date();
		for (int i = 0; i < positions.length; i++) {
			final int pos = positions[i];
			d.setMonth(rand.nextInt(10) + 1);
			d.setDate(rand.nextInt(20) + 1);
			data[pos] = df.format(d);
		}
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < data.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(data[i]);
		}
		final String randomizedLine = sb.toString();
		return randomizedLine;
	}

}