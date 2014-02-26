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
		testSetup.deleteDataFromTables();
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
		final Collection<Map<String, String>> feedRecordData = testSetup.getDataFromFeedRecordTable();
		Assert.assertEquals(1, feedRecordData.size());
		final Map<String, String> feedRecordRow = feedRecordData.iterator().next();
		Assert.assertEquals(2, feedRecordRow.size());
		Assert.assertEquals("1", feedRecordRow.get("cnt"));
		Assert.assertEquals("S", feedRecordRow.get("flag"));

		final Collection<Map<String, String>> bulkRecordData = testSetup.getDataFromBulkRecordTable();
		Assert.assertEquals(2, bulkRecordData.size());
		final Iterator<Map<String, String>> iter = bulkRecordData.iterator();
		final Map<String, String> bulkRecordRow = iter.next();
		Assert.assertEquals(2, bulkRecordRow.size());
		Assert.assertEquals("111", bulkRecordRow.get("cnt"));
		Assert.assertNotNull(bulkRecordRow.get("filepath"));

		final Map<String, String> bulkRecordRow1 = iter.next();
		Assert.assertEquals(2, bulkRecordRow1.size());
		Assert.assertEquals("555", bulkRecordRow1.get("cnt"));
		Assert.assertNotNull(bulkRecordRow1.get("filepath"));
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
		final Collection<Map<String, String>> feedRecordData = testSetup.getDataFromFeedRecordTable();
		Assert.assertEquals(2, feedRecordData.size());
		final Iterator<Map<String, String>> iterator = feedRecordData.iterator();
		final Map<String, String> feedRecordRow1 = iterator.next();
		Assert.assertEquals(2, feedRecordRow1.size());
		Assert.assertEquals("-1", feedRecordRow1.get("cnt"));
		Assert.assertEquals("F", feedRecordRow1.get("flag"));
		final Map<String, String> feedRecordRow2 = iterator.next();
		Assert.assertEquals(2, feedRecordRow2.size());
		Assert.assertEquals("0", feedRecordRow2.get("cnt"));
		Assert.assertEquals("F", feedRecordRow2.get("flag"));
		final Collection<Map<String, String>> bulkRecordData = testSetup.getDataFromBulkRecordTable();
		Assert.assertEquals(0, bulkRecordData.size());
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
		final Collection<Map<String, String>> feedRecordData = testSetup.getDataFromFeedRecordTable();
		Assert.assertEquals(2, feedRecordData.size());
		final Iterator<Map<String, String>> iterator = feedRecordData.iterator();
		final Map<String, String> feedRecordRow1 = iterator.next();
		Assert.assertEquals(2, feedRecordRow1.size());
		Assert.assertEquals("-1", feedRecordRow1.get("cnt"));
		Assert.assertEquals("F", feedRecordRow1.get("flag"));
		final Map<String, String> feedRecordRow2 = iterator.next();
		Assert.assertEquals(2, feedRecordRow2.size());
		Assert.assertEquals("0", feedRecordRow2.get("cnt"));
		Assert.assertEquals("F", feedRecordRow2.get("flag"));
		final Collection<Map<String, String>> bulkRecordData = testSetup.getDataFromBulkRecordTable();
		Assert.assertEquals(0, bulkRecordData.size());
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
