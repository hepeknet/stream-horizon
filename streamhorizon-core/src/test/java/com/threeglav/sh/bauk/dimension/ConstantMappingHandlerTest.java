package com.threeglav.sh.bauk.dimension;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.sh.bauk.dimension.GlobalAttributeMappingHandler;

public class ConstantMappingHandlerTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new GlobalAttributeMappingHandler(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {
		new GlobalAttributeMappingHandler(" ");
	}

	@Test
	public void testSimple() {
		final GlobalAttributeMappingHandler cmh = new GlobalAttributeMappingHandler("h1");
		Map<String, String> hvalues = new HashMap<String, String>();
		hvalues.put("h1", "a");
		hvalues.put("h2", "b");
		Assert.assertEquals("a", cmh.getBulkLoadValue(null, hvalues));
		Assert.assertEquals("a", cmh.getLastLineBulkLoadValue(null, hvalues));
		hvalues = new HashMap<String, String>();
		hvalues.put("h1", "c");
		hvalues.put("h2", "b");
		Assert.assertEquals("c", cmh.getBulkLoadValue(null, hvalues));
		Assert.assertEquals("c", cmh.getLastLineBulkLoadValue(null, hvalues));

		final GlobalAttributeMappingHandler cmh1 = new GlobalAttributeMappingHandler("g1");
		Map<String, String> gvalues = new HashMap<String, String>();
		gvalues.put("g1", "a");
		gvalues.put("g2", "b");
		Assert.assertEquals("a", cmh1.getBulkLoadValue(null, gvalues));
		Assert.assertEquals("a", cmh1.getLastLineBulkLoadValue(null, gvalues));
		gvalues = new HashMap<String, String>();
		gvalues.put("g1", "c");
		gvalues.put("g2", "b");
		Assert.assertEquals("c", cmh1.getBulkLoadValue(null, gvalues));
		Assert.assertEquals("c", cmh1.getLastLineBulkLoadValue(null, gvalues));
	}

}
