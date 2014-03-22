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

public class T1JdbcSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/T1JdbcLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testSimpleJDBCNoHeader() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final Collection<Map<String, String>> dimData = testSetup.getDataFromBigDimension();
		Assert.assertEquals(2, dimData.size());
		int rowNum = 0;
		for (final Map<String, String> row : dimData) {
			if (rowNum == 0) {
				Assert.assertEquals("1", row.get("id"));
				Assert.assertEquals("a11", row.get("a"));
				Assert.assertEquals("b11", row.get("b"));
				Assert.assertEquals("c11", row.get("c"));
				Assert.assertEquals("d11", row.get("d"));
			} else if (rowNum == 1) {
				Assert.assertEquals("2", row.get("id"));
				Assert.assertEquals("a22", row.get("a"));
				Assert.assertEquals("b22", row.get("b"));
				Assert.assertEquals("c22", row.get("c"));
				Assert.assertEquals("d22", row.get("d"));
			}
			rowNum++;
		}
		final File inputFile = testSetup.createInputFile(new String[] { "100,200,a11,b11,c11,d11", "1000,2000,a22,b22,c222,d222" });
		Thread.sleep(5000);
		Assert.assertEquals(1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(2, factData.size());
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
		final Collection<Map<String, String>> dimData1 = testSetup.getDataFromBigDimension();
		Assert.assertEquals(2, dimData1.size());
		int rowNum1 = 0;
		for (final Map<String, String> row : dimData1) {
			if (rowNum1 == 0) {
				Assert.assertEquals("1", row.get("id"));
				Assert.assertEquals("a11", row.get("a"));
				Assert.assertEquals("b11", row.get("b"));
				Assert.assertEquals("c11", row.get("c"));
				Assert.assertEquals("d11", row.get("d"));
			} else if (rowNum1 == 1) {
				Assert.assertEquals("2", row.get("id"));
				Assert.assertEquals("a22", row.get("a"));
				Assert.assertEquals("b22", row.get("b"));
				Assert.assertEquals("c222", row.get("c"));
				Assert.assertEquals("d222", row.get("d"));
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
