package com.threeglav.bauk.util;

import gnu.trove.map.hash.THashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
	public void testGetFileNameWithoutExtension() {
		Assert.assertNull(StringUtil.getFileNameWithoutExtension(null));
		Assert.assertEquals("", StringUtil.getFileNameWithoutExtension(""));
		Assert.assertEquals("test", StringUtil.getFileNameWithoutExtension("test.xml"));
		Assert.assertEquals("c:/a/b/c/d/a", StringUtil.getFileNameWithoutExtension("c:/a/b/c/d/a.txt"));
	}

	@Test
	public void testReplaceAllAttributes() {
		try {
			StringUtil.replaceAllAttributes(null, null, null, "''");
			Assert.fail();
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			StringUtil.replaceAllAttributes(null, null, "''", null);
			Assert.fail();
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		Assert.assertNull(StringUtil.replaceAllAttributes(null, null, "'", "''"));
		Assert.assertEquals(" ", StringUtil.replaceAllAttributes(" ", null, "'", "''"));
		Assert.assertEquals("select 1", StringUtil.replaceAllAttributes("select 1", null, "'", "''"));
		final Map<String, String> attrs = new HashMap<String, String>();
		attrs.put("h.a", "1");
		attrs.put("h.b", "2");
		Assert.assertEquals("select 1 from 2", StringUtil.replaceAllAttributes("select ${h.a} from ${h.b}", attrs, "'", "''"));
		Assert.assertEquals("select ${a} from ${b}", StringUtil.replaceAllAttributes("select ${a} from ${b}", attrs, "'", "''"));
		attrs.put("h.a", "1'");
		attrs.put("h.b", "'2");
		Assert.assertEquals("select '1''' from '''2'", StringUtil.replaceAllAttributes("select '${h.a}' from '${h.b}'", attrs, "'", "''"));
		Assert.assertEquals("select '${a}' from '${b}'", StringUtil.replaceAllAttributes("select '${a}' from '${b}'", attrs, "'", "''"));
		attrs.put("h.a", "1'1");
		attrs.put("h.b", "'2'2");
		Assert.assertEquals("select '1''1' from '''2''2'", StringUtil.replaceAllAttributes("select '${h.a}' from '${h.b}'", attrs, "'", "''"));
	}

	@Test
	public void testReplaceAttributesNullValues() {
		final Map<String, String> attrs = new THashMap<String, String>();
		attrs.put("h.a", "1");
		attrs.put("h.b", "2");
		attrs.put("h.c", null);
		Assert.assertEquals("select '1' from '2' where t=NULL",
				StringUtil.replaceAllAttributes("select '${h.a}' from '${h.b}' where t='${h.c}'", attrs, "'", "''"));
		Assert.assertEquals("select \"1\" from \"2\" where t=NULL and NULL",
				StringUtil.replaceAllAttributes("select \"${h.a}\" from \"${h.b}\" where t=\"${h.c}\" and ${h.c}", attrs, "\"", "''"));
		Assert.assertEquals("call proc('1') from IS_NULL(2) where t=NULL and p = 'NULL'",
				StringUtil.replaceAllAttributes("call proc('${h.a}') from IS_NULL(${h.b}) where t='${h.c}' and p = ''${h.c}''", attrs, "'", "''"));
		attrs.put("h.a", "'1");
		attrs.put("h.b", "2'");
		attrs.put("h.c", null);
		Assert.assertEquals("select '''1' from '2''' where t=NULL",
				StringUtil.replaceAllAttributes("select '${h.a}' from '${h.b}' where t='${h.c}'", attrs, "'", "''"));
		Assert.assertEquals("select \"'1\" from \"2'\" where t=NULL and NULL",
				StringUtil.replaceAllAttributes("select \"${h.a}\" from \"${h.b}\" where t=\"${h.c}\" and ${h.c}", attrs, "\"", "''"));
		Assert.assertEquals("call proc('''1') from IS_NULL(2'') where t=NULL and p = 'NULL'",
				StringUtil.replaceAllAttributes("call proc('${h.a}') from IS_NULL(${h.b}) where t='${h.c}' and p = ''${h.c}''", attrs, "'", "''"));
	}

	@Test
	public void testGetSimpleClassName() {
		Assert.assertNull(StringUtil.getSimpleClassName(null));
		Assert.assertEquals(" ", StringUtil.getSimpleClassName(" "));
		Assert.assertEquals("abc", StringUtil.getSimpleClassName("abc"));
		Assert.assertEquals("StringUtilTest", StringUtil.getSimpleClassName(this.getClass().getName()));
	}

	@Test
	public void testCollectAllAttributes() {
		Assert.assertNull(StringUtil.collectAllAttributesFromString(null));
		Assert.assertNull(StringUtil.collectAllAttributesFromString(" "));
		Assert.assertTrue(StringUtil.collectAllAttributesFromString("select 1 from 2").isEmpty());
		Assert.assertTrue(StringUtil.collectAllAttributesFromString("select ${1 from 2").isEmpty());
		Assert.assertTrue(StringUtil.collectAllAttributesFromString("select $1 from 2").isEmpty());
		Assert.assertTrue(StringUtil.collectAllAttributesFromString("select $1 from 2}").isEmpty());
		Assert.assertTrue(StringUtil.collectAllAttributesFromString("select ${ {1{ from 2").isEmpty());
		final Set<String> res = StringUtil.collectAllAttributesFromString("${a} ${b} ${c} {kk} ${ee}");
		Assert.assertEquals(4, res.size());
		Assert.assertTrue(res.contains("a"));
		Assert.assertTrue(res.contains("b"));
		Assert.assertTrue(res.contains("c"));
		Assert.assertTrue(res.contains("ee"));

		final Set<String> res1 = StringUtil.collectAllAttributesFromString("abc def ${{ a} ${b}} ${cc$} {kk}$ ${${ee}");
		Assert.assertEquals(4, res1.size());
		Assert.assertTrue(res1.contains("{ a"));
		Assert.assertTrue(res1.contains("b"));
		Assert.assertTrue(res1.contains("cc$"));
		Assert.assertTrue(res1.contains("${ee"));

		final Set<String> res2 = StringUtil.collectAllAttributesFromString("abc def ${abc");
		Assert.assertEquals(0, res2.size());

		final Set<String> res3 = StringUtil.collectAllAttributesFromString("abc def ${");
		Assert.assertEquals(0, res3.size());
	}

}
