package com.threeglav.bauk.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilTest {

	@Test
	public void test() {
		Assert.assertEquals("aa", StringUtil.replaceAllNonASCII("aa"));
		Assert.assertEquals("aa", StringUtil.replaceAllNonASCII("*aa"));
		Assert.assertEquals("aa", StringUtil.replaceAllNonASCII(".*aa"));
	}

	@Test
	public void testFixPath() {
		Assert.assertEquals(null, StringUtil.fixFilePath(null));
		Assert.assertEquals(" ", StringUtil.fixFilePath(" "));
		Assert.assertEquals("/tmp/a/b/c/", StringUtil.fixFilePath("/tmp/a/b/c/"));
		Assert.assertEquals("c:/a/b/", StringUtil.fixFilePath("c:/a/b/"));
		Assert.assertEquals("c:/a/b/x", StringUtil.fixFilePath("c:\\a\\b\\x"));
	}

	@Test
	public void testReplaceAllAttributes() {
		Assert.assertEquals(null, StringUtil.replaceAllAttributes(null, null, null));
		Assert.assertEquals(" ", StringUtil.replaceAllAttributes(" ", null, null));
		Assert.assertEquals("select 1", StringUtil.replaceAllAttributes("select 1", null, null));
		final Map<String, String> attrs = new HashMap<String, String>();
		attrs.put("a", "1");
		attrs.put("b", "2");
		Assert.assertEquals("select 1 from 2", StringUtil.replaceAllAttributes("select ${h.a} from ${h.b}", attrs, "h."));
		Assert.assertEquals("select 1 from 2", StringUtil.replaceAllAttributes("select ${a} from ${b}", attrs, null));
	}

}
