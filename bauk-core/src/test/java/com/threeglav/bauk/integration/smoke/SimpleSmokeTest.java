package com.threeglav.bauk.integration.smoke;

import java.util.Collection;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.integration.BaukTestSetupUtil;

public class SimpleSmokeTest {

	private final BaukTestSetupUtil testSetup = new BaukTestSetupUtil();

	@Test
	public void test() throws Exception {
		Assert.assertTrue(testSetup.setupTestEnvironment("/simpleJdbcLoadingFeedConfig.xml"));
		testSetup.startBaukInstance();
		Assert.assertTrue(testSetup.getDataFromFactTable().isEmpty());
		testSetup.createInputFile(new String[] { "100,200,a1,b1" });
		Thread.sleep(5000);
		final Collection<Map<String, String>> factData = testSetup.getDataFromFactTable();
		Assert.assertEquals(1, factData.size());
		final Map<String, String> firstRow = factData.iterator().next();
		Assert.assertEquals(4, firstRow.size());
		Assert.assertEquals("1", firstRow.get("f1"));
		Assert.assertEquals("100", firstRow.get("f2"));
		Assert.assertEquals("200", firstRow.get("f3"));
		Assert.assertTrue(firstRow.get("f4").contains("TEST_Feed"));
		testSetup.stopBaukInstance();
	}

}
