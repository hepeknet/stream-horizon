package com.threeglav.sh.bauk.integration.smoke;

import java.io.File;
import java.util.Collection;
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
import com.threeglav.sh.bauk.model.FeedTarget;

public class JdbcSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/simpleJdbcLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testSimpleJDBCNoHeader() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final long processedCount = EngineRegistry.getProcessedFeedFilesCount();
		final long successBulkCount = EngineRegistry.getSuccessfulBulkFilesCount();
		final File inputFile = testSetup.createInputFile(new String[] { "100,200,a1,b1" });
		LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(5000, TimeUnit.MILLISECONDS));
		Assert.assertEquals(processedCount + 1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(successBulkCount + 1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(1, factData.size());
		final Map<String, String> firstRow = factData.iterator().next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("100", firstRow.get("f2"));
		Assert.assertEquals("200", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("N/A"));
		inputFile.delete();
	}

	@Test
	public void testSimpleJDBCNoBulkOutputFolder() throws Exception {
		System.clearProperty(FeedTarget.FILE_TARGET_DIRECTORY_PROP_NAME);
		this.testSimpleJDBCNoHeader();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
