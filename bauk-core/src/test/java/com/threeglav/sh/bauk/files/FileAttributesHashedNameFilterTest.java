package com.threeglav.sh.bauk.files;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.sh.bauk.files.FileAttributesHashedNameFilter;

public class FileAttributesHashedNameFilterTest {

	@Test
	public void testWithDelay() throws Exception {
		final Random r = new Random();
		final int maxNum = r.nextInt(11);
		final String extension = "baukAbc";
		final FileAttributesHashedNameFilter[] filters = new FileAttributesHashedNameFilter[maxNum];
		for (int i = 0; i < maxNum; i++) {
			filters[i] = new FileAttributesHashedNameFilter(".*" + extension, i, maxNum, 3000);
		}
		final int tests = 10;
		final File[] files = new File[tests];
		for (int i = 0; i < tests; i++) {
			files[i] = this.createAndFillTemporaryFile(extension);
		}
		// wait for last modified
		Thread.sleep(6010);
		for (int i = 0; i < 10; i++) {
			int countMatches = 0;
			final File file = files[i];
			for (final FileAttributesHashedNameFilter filter : filters) {
				if (filter.accept(file.toPath())) {
					countMatches++;
				}
			}
			Assert.assertEquals(1, countMatches);
		}
	}

	@Test
	public void testNoDelay() throws Exception {
		final Random r = new Random();
		final int maxNum = r.nextInt(11);
		final String extension = "baukAbc";
		final FileAttributesHashedNameFilter[] filters = new FileAttributesHashedNameFilter[maxNum];
		for (int i = 0; i < maxNum; i++) {
			filters[i] = new FileAttributesHashedNameFilter(".*" + extension, i, maxNum, 10);
		}
		final int tests = 10;
		final File[] files = new File[tests];
		for (int i = 0; i < tests; i++) {
			files[i] = this.createAndFillTemporaryFile(extension);
		}
		Thread.sleep(1000);
		for (int i = 0; i < 10; i++) {
			int countMatches = 0;
			final File file = files[i];
			for (final FileAttributesHashedNameFilter filter : filters) {
				if (filter.accept(file.toPath())) {
					countMatches++;
				}
			}
			Assert.assertEquals(1, countMatches);
		}
	}

	private File createAndFillTemporaryFile(final String extension) throws Exception {
		final File f = File.createTempFile("baukTempFiles", extension);
		f.deleteOnExit();
		final FileOutputStream fos = new FileOutputStream(f);
		final Random r = new Random();
		final int numberOfLines = r.nextInt(17);
		for (int i = 0; i < numberOfLines; i++) {
			final String str = UUID.randomUUID().toString();
			fos.write(str.getBytes());
		}
		fos.flush();
		fos.close();
		return f;
	}

}
