package com.threeglav.sh.bauk.integration.smoke;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.integration.BaukTestSetupUtil;

public class CustomDimensionJdbcSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/simpleCustomDimensionJdbcLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void test() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final long processedCount = EngineRegistry.getProcessedFeedFilesCount();
		final long successBulkCount = EngineRegistry.getSuccessfulBulkFilesCount();
		final File inputFile = testSetup
				.createInputFile(new String[] { "100,200,ap66,bp66", "111,222,a11,b22", "3333,4444,ap77,bp77", "9,-9,a0,b0" });
		LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(5000, TimeUnit.MILLISECONDS));
		Assert.assertEquals(processedCount + 1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(successBulkCount + 1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(4, factData.size());
		final Iterator<Map<String, String>> iterator = factData.iterator();
		int rowNum = 0;
		while (iterator.hasNext()) {
			rowNum++;
			final Map<String, String> row = iterator.next();
			if (rowNum == 2) {
				Assert.assertEquals("666", row.get("f1"));
				Assert.assertEquals("100", row.get("f2"));
				Assert.assertEquals("200", row.get("f3"));
			} else if (rowNum == 4) {
				Assert.assertEquals("314159", row.get("f1"));
				Assert.assertEquals("111", row.get("f2"));
				Assert.assertEquals("222", row.get("f3"));
			} else if (rowNum == 3) {
				Assert.assertEquals("777", row.get("f1"));
				Assert.assertEquals("3333", row.get("f2"));
				Assert.assertEquals("4444", row.get("f3"));
			} else if (rowNum == 1) {
				Assert.assertEquals("42", row.get("f1"));
				Assert.assertEquals("9", row.get("f2"));
				Assert.assertEquals("-9", row.get("f3"));
			}
			Assert.assertEquals(4, row.size());
			Assert.assertTrue(row.get("f4").contains("N/A"));
		}
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
