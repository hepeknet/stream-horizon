package com.threeglav.sh.bauk.integration.smoke;

import java.io.File;
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

public class T2JdbcNewRecordsTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/T2JdbcLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	protected void testT2DimensionInitialData() throws Exception {
		final Collection<Map<String, String>> dimData = testSetup.getDataFromT2Dimension();
		Assert.assertEquals(3, dimData.size());
		int rowNum = 0;
		for (final Map<String, String> row : dimData) {
			if (rowNum == 0) {
				Assert.assertEquals("a11", row.get("a"));
				Assert.assertEquals("b11", row.get("b"));
				Assert.assertEquals("c11", row.get("c"));
				Assert.assertEquals("d11", row.get("d"));
				Assert.assertEquals("Y", row.get("valid"));
			} else if (rowNum == 1) {
				Assert.assertEquals("a11", row.get("a"));
				Assert.assertEquals("b11", row.get("b"));
				Assert.assertEquals("c11", row.get("c"));
				Assert.assertEquals("d11", row.get("d"));
				Assert.assertEquals("N", row.get("valid"));
			} else if (rowNum == 2) {
				Assert.assertEquals("a22", row.get("a"));
				Assert.assertEquals("b22", row.get("b"));
				Assert.assertEquals("c22", row.get("c"));
				Assert.assertEquals("d22", row.get("d"));
				Assert.assertEquals("Y", row.get("valid"));
			}
			rowNum++;
		}
	}

	@Test
	public void testWithNewRecords() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		testSetup.deleteDataFromT2Dimension();
		final long countProcessed = EngineRegistry.getProcessedFeedFilesCount();
		final long bulkLoaded = EngineRegistry.getSuccessfulBulkFilesCount();
		final File inputFile = testSetup.createInputFile(new String[] { "100,200,a1111,b1111,c1111,d1111", "1000,2000,a2222,b2222,c2222,d2222",
				"10000,20000,a2222,b2222,c2222,d2222", "100000,200000,a2222,b2222,c2223,d2223" });
		Thread.sleep(5000);
		Assert.assertEquals(countProcessed + 1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(bulkLoaded + 1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(4, factData.size());
		final Iterator<Map<String, String>> iterator = factData.iterator();
		final Map<String, String> firstRow = iterator.next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("100", firstRow.get("f2"));
		Assert.assertEquals("200", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("TEST_jdbc_Feed"));
		final Map<String, String> secondRow = iterator.next();
		Assert.assertEquals(4, secondRow.size());
		Assert.assertEquals("2", secondRow.get("f1"));
		Assert.assertEquals("1000", secondRow.get("f2"));
		Assert.assertEquals("2000", secondRow.get("f3"));
		Assert.assertTrue(secondRow.get("f4").contains("TEST_jdbc_Feed"));

		final Map<String, String> thirdRow = iterator.next();
		Assert.assertEquals(4, thirdRow.size());
		Assert.assertEquals("2", thirdRow.get("f1"));
		Assert.assertEquals("10000", thirdRow.get("f2"));
		Assert.assertEquals("20000", thirdRow.get("f3"));
		Assert.assertTrue(thirdRow.get("f4").contains("TEST_jdbc_Feed"));

		final Map<String, String> fourthRow = iterator.next();
		Assert.assertEquals(4, fourthRow.size());
		Assert.assertEquals("3", fourthRow.get("f1"));
		Assert.assertEquals("100000", fourthRow.get("f2"));
		Assert.assertEquals("200000", fourthRow.get("f3"));
		Assert.assertTrue(fourthRow.get("f4").contains("TEST_jdbc_Feed"));

		final Collection<Map<String, String>> dimData1 = testSetup.getDataFromT2Dimension();
		Assert.assertEquals(3, dimData1.size());
		int rowNum1 = 0;
		for (final Map<String, String> row : dimData1) {
			if (rowNum1 == 0) {
				Assert.assertEquals("1", row.get("id"));
				Assert.assertEquals("a1111", row.get("a"));
				Assert.assertEquals("b1111", row.get("b"));
				Assert.assertEquals("c1111", row.get("c"));
				Assert.assertEquals("d1111", row.get("d"));
				Assert.assertEquals("Y", row.get("valid"));
			} else if (rowNum1 == 1) {
				Assert.assertEquals("2", row.get("id"));
				Assert.assertEquals("a2222", row.get("a"));
				Assert.assertEquals("b2222", row.get("b"));
				Assert.assertEquals("c2222", row.get("c"));
				Assert.assertEquals("d2222", row.get("d"));
				Assert.assertEquals("N", row.get("valid"));
			} else if (rowNum1 == 2) {
				Assert.assertEquals("3", row.get("id"));
				Assert.assertEquals("a2222", row.get("a"));
				Assert.assertEquals("b2222", row.get("b"));
				Assert.assertEquals("c2223", row.get("c"));
				Assert.assertEquals("d2223", row.get("d"));
				Assert.assertEquals("Y", row.get("valid"));
			}
			rowNum1++;
		}
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
