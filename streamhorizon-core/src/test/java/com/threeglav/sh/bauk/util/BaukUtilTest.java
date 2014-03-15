package com.threeglav.sh.bauk.util;

import org.junit.Assert;
import org.junit.Test;

public class BaukUtilTest {

	@Test
	public void test() {
		Assert.assertEquals(0, BaukUtil.calculatePartition(10, 3, 2));
		Assert.assertEquals(0, BaukUtil.calculatePartition(10, 3, 1));
		Assert.assertEquals(0, BaukUtil.calculatePartition(10, 3, 0));
		Assert.assertEquals(1, BaukUtil.calculatePartition(10, 3, 3));
		Assert.assertEquals(1, BaukUtil.calculatePartition(10, 3, 5));
		Assert.assertEquals(2, BaukUtil.calculatePartition(10, 3, 7));
		Assert.assertEquals(2, BaukUtil.calculatePartition(10, 3, 9));

		Assert.assertEquals(0, BaukUtil.calculatePartition(41, 10, 0));
		Assert.assertEquals(3, BaukUtil.calculatePartition(41, 10, 12));
		Assert.assertEquals(9, BaukUtil.calculatePartition(41, 10, 41));
		Assert.assertEquals(9, BaukUtil.calculatePartition(41, 10, 37));
	}

}
