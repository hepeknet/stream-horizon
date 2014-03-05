package com.threeglav.sh.bauk.integration.bulk;

import java.io.File;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.integration.BaukTestSetupUtil;

public class CustomBulkWriterTest {

	private static BaukTestSetupUtil testSetup;
	static final HazelcastInstance INSTANCE = Hazelcast.newHazelcastInstance();
	private final IMap<Integer, String> values = INSTANCE.getMap("custom_bulk");

	@BeforeClass
	public static void start() throws Exception {
		testSetup = new BaukTestSetupUtil();
		Assert.assertTrue(testSetup.setupTestEnvironment("/customBulkWriterFeedConfig.xml"));
		testSetup.startBaukInstance();
	}

	@Before
	public void setup() throws Exception {
		testSetup.deleteDataFromTables();
	}

	@Test
	public void testSimpleJDBCNoHeader() throws Exception {
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		Assert.assertTrue(values.isEmpty());
		final File inputFile = testSetup.createInputFile(new String[] { "100,200,a1,b1" });
		Thread.sleep(5000);
		Assert.assertEquals(1, EngineRegistry.getProcessedFeedFilesCount());
		Assert.assertEquals(0, EngineRegistry.getSuccessfulBulkFilesCount());
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		Assert.assertEquals(1, values.size());
		final String value = values.get(1);
		Assert.assertNotNull(value);
		Assert.assertEquals("1#100#200#", value);
		inputFile.delete();
	}

	@AfterClass
	public static void stop() throws Exception {
		testSetup.stopBaukInstance();
		Hazelcast.shutdownAll();
	}

}
