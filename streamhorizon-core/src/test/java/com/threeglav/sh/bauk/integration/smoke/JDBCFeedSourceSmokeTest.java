package com.threeglav.sh.bauk.integration.smoke;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.integration.BaukTestSetupUtil;

public class JDBCFeedSourceSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/jdbcFeedFromJDBCConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
		testSetup.populateFeedSourceTable();
	}

	@Test
	public void testSimple() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final long processedTotal = EngineRegistry.getProcessedFeedFilesCount();
		final long bulkTotal = EngineRegistry.getSuccessfulBulkFilesCount();
		Thread.sleep(4000);
		Assert.assertEquals(processedTotal + 2, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(bulkTotal + 2, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(6, factData.size());
		final Iterator<Map<String, String>> iterator = factData.iterator();
		int rowCounter = 1;
		while (iterator.hasNext()) {
			final Map<String, String> firstRow = iterator.next();
			Assert.assertEquals(4, firstRow.size());
			int multiply = 10;
			if (rowCounter == 1 || rowCounter == 2) {
				multiply = 10;
			} else if (rowCounter == 3 || rowCounter == 4) {
				multiply = 20;
			} else {
				multiply = 30;
			}
			Assert.assertEquals("1", firstRow.get("f1"));
			Assert.assertEquals(String.valueOf(multiply), firstRow.get("f2"));
			Assert.assertEquals(String.valueOf(multiply * 10), firstRow.get("f3"));
			Assert.assertTrue(firstRow.get("f4").contains("TEST_jdbc_Feed"));
			rowCounter++;
		}
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
