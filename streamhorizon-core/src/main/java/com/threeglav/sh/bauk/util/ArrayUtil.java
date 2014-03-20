package com.threeglav.sh.bauk.util;

public abstract class ArrayUtil {

	public static int[] flattenArray(final int[][] arr) {
		int size = 0;
		for (final int[] a : arr) {
			size += a.length;
		}
		final int[] flat = new int[size];
		int count = 0;
		for (final int[] a : arr) {
			for (int i = 0; i < a.length; i++) {
				flat[count++] = a[i];
			}
		}
		return flat;
	}

}
