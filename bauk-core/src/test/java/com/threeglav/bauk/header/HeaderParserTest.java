package com.threeglav.bauk.header;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class HeaderParserTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init(null, null, null);
		hp.parseHeader("", null, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testNoExpectedCharacter() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init("0", ",", null);
		hp.parseHeader("", new String[] { "a", "b" }, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testNoHeader() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init("0", ",", null);
		hp.parseHeader("", new String[] { "a", "b" }, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testHeaderNoMatch() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init("0", ",", null);
		hp.parseHeader("1,2,3,4", null, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testNumbersDoNotMatch() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init("1", ",", null);
		hp.parseHeader("1,2,3,4", new String[] {}, null);
	}

	@Test
	public void testSimpleSuccess() {
		final HeaderParser hp = new DefaultHeaderParser();
		hp.init("1", ",", null);
		final Map<String, String> headerValues = hp.parseHeader("1,2,3,4", new String[] { "n_0", "n_1", "n_2" }, null);
		Assert.assertNotNull(headerValues);
		Assert.assertEquals(3, headerValues.size());
		Assert.assertEquals("2", headerValues.get("n_0"));
		Assert.assertEquals("3", headerValues.get("n_1"));
		Assert.assertEquals("4", headerValues.get("n_2"));
	}

}
