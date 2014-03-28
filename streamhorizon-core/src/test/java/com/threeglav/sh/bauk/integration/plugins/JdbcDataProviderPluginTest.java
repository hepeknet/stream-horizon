package com.threeglav.sh.bauk.integration.plugins;

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

public class JdbcDataProviderPluginTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/dimDataProviderFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testSimpleJDBCNoHeader() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "100,200,a1,b1", "101,201,ap66,bp66", "102,202,ap77,bp77" });
		Thread.sleep(5000);
		Assert.assertEquals(1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(3, factData.size());
		final Iterator<Map<String, String>> iterator = factData.iterator();
		final Map<String, String> firstRow = iterator.next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("100", firstRow.get("f2"));
		Assert.assertEquals("200", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("TEST_jdbc_Feed"));

		final Map<String, String> secondRow = iterator.next();
		Assert.assertEquals(4, secondRow.size());
		Assert.assertEquals("666", secondRow.get("f1"));
		Assert.assertEquals("101", secondRow.get("f2"));
		Assert.assertEquals("201", secondRow.get("f3"));
		Assert.assertTrue(secondRow.get("f4").contains("TEST_jdbc_Feed"));

		final Map<String, String> thirdRow = iterator.next();
		Assert.assertEquals(4, thirdRow.size());
		Assert.assertEquals("777", thirdRow.get("f1"));
		Assert.assertEquals("102", thirdRow.get("f2"));
		Assert.assertEquals("202", thirdRow.get("f3"));
		Assert.assertTrue(thirdRow.get("f4").contains("TEST_jdbc_Feed"));
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
