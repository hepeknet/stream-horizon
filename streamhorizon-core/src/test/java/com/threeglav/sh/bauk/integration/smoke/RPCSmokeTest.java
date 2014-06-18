package com.threeglav.sh.bauk.integration.smoke;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
	public void testSimple() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final long processedTotal = EngineRegistry.getProcessedFeedFilesCount();
		final long bulkTotal = EngineRegistry.getSuccessfulBulkFilesCount();
		final ProcessingResult result = testSetup.sendInputDataOverRPC(new String[] { "111,222,a1,b1" }, 7890);
		Thread.sleep(5000);
		Assert.assertEquals(processedTotal + 1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(bulkTotal + 1, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(1, factData.size());
		final Map<String, String> firstRow = factData.iterator().next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("111", firstRow.get("f2"));
		Assert.assertEquals("222", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("N/A"));
		Assert.assertEquals(result, ProcessingResult.SUCCESS);
	}

	@Test
	public void testInvalid() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final ProcessingResult result = testSetup.sendInputDataOverRPC(null, 7890);
		Assert.assertEquals(result, ProcessingResult.INVALID_FEED);
	}

	@Test
	public void testMultiThreadedInsert() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		final int threadNumber = 10;
		final long processedTotal = EngineRegistry.getProcessedFeedFilesCount();
		final long bulkTotal = EngineRegistry.getSuccessfulBulkFilesCount();
		final ExecutorService execService = Executors.newFixedThreadPool(3);
		final List<Future<ProcessingResult>> results = new LinkedList<>();
		for (int i = 0; i < threadNumber; i++) {
			final int currentCounter = i;
			final Future<ProcessingResult> fpr = execService.submit(new Callable<ProcessingResult>() {
				@Override
				public ProcessingResult call() {
					if (currentCounter != 5) {
						final String row = currentCounter + ", " + 2 * currentCounter + ", a1,b1";
						return testSetup.sendInputDataOverRPC(new String[] { row }, 7890);
					} else {
						return testSetup.sendInputDataOverRPC(null, 7890);
					}
				}
			});
			results.add(fpr);
		}
		Thread.sleep(5000);
		Assert.assertEquals(processedTotal + 9, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(bulkTotal + 9, EngineRegistry.getSuccessfulBulkFilesCount());
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(9, factData.size());
		Assert.assertEquals(10, results.size());
		final Iterator<Map<String, String>> iterator = factData.iterator();
		for (int i = 0; i < threadNumber; i++) {
			if (i == 5) {
				Assert.assertEquals(ProcessingResult.INVALID_FEED, results.get(i).get());
			} else {
				Assert.assertEquals(ProcessingResult.SUCCESS, results.get(i).get());
				final Map<String, String> firstRow = iterator.next();
				Assert.assertEquals(4, firstRow.size());
				// Assert.assertEquals(i, firstRow.get("f1"));
				Assert.assertEquals(String.valueOf(i), firstRow.get("f2"));
				Assert.assertEquals(String.valueOf(2 * i), firstRow.get("f3"));
				Assert.assertTrue(firstRow.get("f4").contains("N/A"));
			}
		}
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
	}

}
