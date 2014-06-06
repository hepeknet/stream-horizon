package com.threeglav.sh.bauk.integration.smoke;

import java.util.Collection;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.integration.BaukTestSetupUtil;
import com.threeglav.sh.bauk.rpc.ProcessingResult;

public class RPCSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/jdbcFeedFromRPCConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testSimpleJDBCNoHeader() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final ProcessingResult result = testSetup.sendInputDataOverRPC(new String[] { "111,222,a1,b1" }, 7890);
		Thread.sleep(5000);
		Assert.assertEquals(1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(1, factData.size());
		final Map<String, String> firstRow = factData.iterator().next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("111", firstRow.get("f2"));
		Assert.assertEquals("222", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("TEST_jdbc_Feed"));
		Assert.assertEquals(result, ProcessingResult.SUCCESS);
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
