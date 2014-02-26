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

public class BulkLoadingFailureTest {

	private static BaukTestSetupUtil testSetup;

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/simpleBulkLoadingBadBulkFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testInvalidBulkLoadingData() throws Exception {
		final long successFeedsBefore = EngineRegistry.getProcessedFeedFilesCount();
		final long failedFeedBefore = EngineRegistry.getFailedFeedFilesCount();
		final long successBulkBefore = EngineRegistry.getSuccessfulBulkFilesCount();
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "0,head1,head2,head3", "1,1000,2000,a2,b2", "9,1" });
		Thread.sleep(7000);
		Assert.assertEquals(successFeedsBefore + 1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(failedFeedBefore, EngineRegistry.getFailedFeedFilesCount());
		Assert.assertEquals(successBulkBefore, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(0, factData.size());
		final Collection<Map<String, String>> feedRecordData = testSetup.getDataFromFeedRecordTable();
		Assert.assertEquals(1, feedRecordData.size());
		final Map<String, String> feedRecordRow = feedRecordData.iterator().next();
		Assert.assertEquals(2, feedRecordRow.size());
		Assert.assertEquals("1", feedRecordRow.get("cnt"));
		Assert.assertEquals("S", feedRecordRow.get("flag"));

		final Collection<Map<String, String>> bulkRecordData = testSetup.getDataFromBulkRecordTable();
		Assert.assertEquals(3, bulkRecordData.size());
		final Iterator<Map<String, String>> bulkIterator = bulkRecordData.iterator();

		final Map<String, String> bulkRecordRow1 = bulkIterator.next();
		Assert.assertEquals(2, bulkRecordRow1.size());
		Assert.assertEquals("222", bulkRecordRow1.get("cnt"));
		Assert.assertNotNull(bulkRecordRow1.get("filepath"));

		final Map<String, String> bulkRecordRow2 = bulkIterator.next();
		Assert.assertEquals(2, bulkRecordRow2.size());
		Assert.assertEquals("333", bulkRecordRow2.get("cnt"));
		Assert.assertNotNull(bulkRecordRow2.get("filepath"));
		Assert.assertTrue(bulkRecordRow2.get("filepath").startsWith("abc_"));

		final Map<String, String> bulkRecordRow3 = bulkIterator.next();
		Assert.assertEquals(2, bulkRecordRow3.size());
		Assert.assertEquals("4444", bulkRecordRow3.get("cnt"));
		Assert.assertNotNull(bulkRecordRow3.get("filepath"));
		Assert.assertTrue(bulkRecordRow3.get("filepath").startsWith("ggg_"));
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
