package com.threeglav.bauk.feed;

import org.junit.Assert;
import org.junit.Test;

public class LineBufferTest {

	@Test
	public void testSimple() {
		final LineBuffer lb = new LineBuffer();
		Assert.assertEquals(0, lb.getSize());
		for (int i = 0; i < 10; i++) {
			Assert.assertNull(lb.getLine());
		}
		lb.add(null);
		Assert.assertEquals(0, lb.getSize());
		for (int i = 0; i < 10; i++) {
			Assert.assertNull(lb.getLine());
		}
		lb.add("line1");
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("line1", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
		lb.add("l1");
		lb.add("l2");
		Assert.assertEquals(2, lb.getSize());
		Assert.assertEquals("l1", lb.getLine());
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("l2", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
		lb.add("l11");
		lb.add("l22");
		Assert.assertEquals(2, lb.getSize());
		Assert.assertEquals("l11", lb.getLine());
		Assert.assertEquals(1, lb.getSize());
		lb.add("l33");
		Assert.assertEquals(2, lb.getSize());
		Assert.assertEquals("l22", lb.getLine());
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("l33", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
	}

	@Test
	public void testOrdering() {
		final LineBuffer lb = new LineBuffer();
		Assert.assertEquals(0, lb.getSize());
		Assert.assertTrue(lb.canAdd());
		lb.add("1");
		lb.add("2");
		Assert.assertFalse(lb.canAdd());
		Assert.assertEquals(2, lb.getSize());
		Assert.assertEquals("1", lb.getLine());
		Assert.assertTrue(lb.canAdd());
		lb.add("3");
		Assert.assertEquals(2, lb.getSize());
		Assert.assertFalse(lb.canAdd());
		Assert.assertEquals("2", lb.getLine());
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("3", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
		lb.add("4");
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("4", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
		lb.add("5");
		lb.add("6");
		Assert.assertEquals("5", lb.getLine());
		Assert.assertEquals(1, lb.getSize());
		Assert.assertEquals("6", lb.getLine());
		Assert.assertEquals(0, lb.getSize());
	}

	@Test(expected = IllegalStateException.class)
	public void testFull() {
		final LineBuffer lb = new LineBuffer();
		lb.add("abc");
		lb.add("abc1");
		lb.add("abc2");
	}

	@Test
	public void testAddign() {
		final LineBuffer lb = new LineBuffer();
		Assert.assertEquals(0, lb.getSize());
		while (lb.canAdd()) {
			lb.add("ab");
		}
		Assert.assertEquals(2, lb.getSize());
	}

}
