package com.threeglav.bauk.dimension;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ConstantMappingHandlerTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new ConstantMappingHandler(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {
		new ConstantMappingHandler(" ");
	}

	@Test
	public void testSimple() {
		final ConstantMappingHandler cmh = new ConstantMappingHandler("header.h1");
		Map<String, String> hvalues = new HashMap<String, String>();
		hvalues.put("h1", "a");
		hvalues.put("h2", "b");
		Assert.assertEquals("a", cmh.getBulkLoadValue(null, hvalues, null));
		hvalues = new HashMap<String, String>();
		hvalues.put("h1", "c");
		hvalues.put("h2", "b");
		Assert.assertEquals("c", cmh.getBulkLoadValue(null, hvalues, null));

		final ConstantMappingHandler cmh1 = new ConstantMappingHandler("global.g1");
		Map<String, String> gvalues = new HashMap<String, String>();
		gvalues.put("g1", "a");
		gvalues.put("g2", "b");
		Assert.assertEquals("a", cmh1.getBulkLoadValue(null, null, gvalues));
		gvalues = new HashMap<String, String>();
		gvalues.put("g1", "c");
		gvalues.put("g2", "b");
		Assert.assertEquals("c", cmh1.getBulkLoadValue(null, null, gvalues));
	}

}
