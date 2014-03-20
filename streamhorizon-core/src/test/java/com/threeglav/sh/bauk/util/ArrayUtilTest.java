package com.threeglav.sh.bauk.util;

import org.junit.Assert;
import org.junit.Test;

public class ArrayUtilTest {

	@Test
	public void test() {
		final int[] flat = ArrayUtil.flattenArray(new int[][] { { 11 } });
		Assert.assertEquals(1, flat.length);
		Assert.assertEquals(11, flat[0]);

		final int[] flat1 = ArrayUtil.flattenArray(new int[][] { { 11, 22, 33 } });
		Assert.assertEquals(3, flat1.length);
		Assert.assertEquals(11, flat1[0]);
		Assert.assertEquals(22, flat1[1]);
		Assert.assertEquals(33, flat1[2]);

		final int[] flat2 = ArrayUtil.flattenArray(new int[][] { { 11 }, { 22 }, { 33 } });
		Assert.assertEquals(3, flat2.length);
		Assert.assertEquals(11, flat2[0]);
		Assert.assertEquals(22, flat2[1]);
		Assert.assertEquals(33, flat2[2]);

		final int[] flat3 = ArrayUtil.flattenArray(new int[][] { { 11 }, {}, { 22, 33 } });
		Assert.assertEquals(3, flat3.length);
		Assert.assertEquals(11, flat3[0]);
		Assert.assertEquals(22, flat3[1]);
		Assert.assertEquals(33, flat3[2]);
	}

}
