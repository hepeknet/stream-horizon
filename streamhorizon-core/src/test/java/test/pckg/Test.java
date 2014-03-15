package test.pckg;

import org.apache.commons.lang.StringUtils;

public class Test {

	public static void main(final String[] args) {
		final String sent = "abc ${dd} ddd ${cc} eee ${ff}";
		final int count = 1000000;
		for (int i = 0; i < count; i++) {
			String s = StringUtils.replace(sent, "${dd}", "ccccc_" + i);
			s = StringUtils.replace(s, "${ff}", "qqq_" + i);
		}
		final long start = System.currentTimeMillis();
		for (int i = 0; i < count; i++) {
			String s = StringUtils.replace(sent, "${dd}", "ccccc_" + i);
			s = StringUtils.replace(s, "${ff}", "qqq_" + i);
		}
		final long total = System.currentTimeMillis() - start;
		System.out.println(total);
	}

}
