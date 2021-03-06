package com.threeglav.sh.bauk.dimension;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.sh.bauk.dimension.PositionalMappingHandler;

public class PositionalMappingHandlerTest {

	@Test
	public void testNegatives() {
		try {
			new PositionalMappingHandler(-1, 10);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			new PositionalMappingHandler(10, -1);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			new PositionalMappingHandler(10, 3);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			final PositionalMappingHandler pmh = new PositionalMappingHandler(2, 3);
			pmh.getBulkLoadValue(new String[] { "1" }, null);
			fail("nok");
		} catch (final ArrayIndexOutOfBoundsException iae) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimple() {
		final PositionalMappingHandler pmh = new PositionalMappingHandler(2, 4);
		Assert.assertEquals("c", pmh.getBulkLoadValue(new String[] { "a", "b", "c", "d" }, null));
		Assert.assertEquals("c", pmh.getLastLineBulkLoadValue(new String[] { "a", "b", "c", "d" }, null));
	}

}
