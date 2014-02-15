package com.threeglav.bauk.integration.smoke;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.threeglav.bauk.EngineRegistry;
import com.threeglav.bauk.integration.BaukTestSetupUtil;

public class BulkSmokeTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/simpleBulkLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromFactTable();
	}

	@Test
	public void testSimpleBulk() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "0,head1,head2,head3", "1,1000,2000,a2,b2", "9,1" });
		Thread.sleep(7000);
		Assert.assertEquals(1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(1, factData.size());
		final Map<String, String> firstRow = factData.iterator().next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("2", firstRow.get("f1"));
		Assert.assertEquals("1000", firstRow.get("f2"));
		Assert.assertEquals("2000", firstRow.get("f3"));
		Assert.assertEquals("head1", firstRow.get("f4"));
		inputFile.delete();
	}

	@Test
	public void testSimpleBulkInvalidData() throws Exception {
		final long successFeedsBefore = EngineRegistry.getProcessedFeedFilesCount();
		final long failedFeedBefore = EngineRegistry.getFailedFeedFilesCount();
		final long successBulkBefore = EngineRegistry.getSuccessfulBulkFilesCount();
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "head1,head2,head3", "1000,2000,a2,b2", "9,1" });
		Thread.sleep(7000);
		Assert.assertEquals(successFeedsBefore, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(failedFeedBefore + 1, EngineRegistry.getFailedFeedFilesCount());
		Assert.assertEquals(successBulkBefore, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(0, factData.size());
		inputFile.delete();
	}

	@Test
	public void testSimpleBulkInvalidFooter() throws Exception {
		final long successFeedsBefore = EngineRegistry.getProcessedFeedFilesCount();
		final long successBulkBefore = EngineRegistry.getSuccessfulBulkFilesCount();
		final long failedFeedBefore = EngineRegistry.getFailedFeedFilesCount();
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "0,head1,head2,head3", "1,1000,2000,a2,b2", "9,5" });
		Thread.sleep(7000);
		Assert.assertEquals(successFeedsBefore, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(failedFeedBefore + 1, EngineRegistry.getFailedFeedFilesCount());
		Assert.assertEquals(successBulkBefore, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(0, factData.size());
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
