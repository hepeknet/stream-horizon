package org.treeglav.bauk.parser;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.RepetitiveFeedParser;

public class RepetitiveFeedParserTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNegative() {
		new RepetitiveFeedParser("||", -1);
	}

	@Test
	public void test() {
		final FeedParser fp = new RepetitiveFeedParser("||", 3);
		final String[] res = fp.parse("||||12||ab||cd");
		Assert.assertEquals(5, res.length);
		Assert.assertNull(res[0]);
		Assert.assertNull(res[1]);
		Assert.assertEquals("12", res[2]);
		Assert.assertEquals("ab", res[3]);
		Assert.assertEquals("cd", res[4]);
		final String[] res1 = fp.parse("||||||gg||hh");
		Assert.assertEquals(5, res1.length);
		Assert.assertNull(res1[0]);
		Assert.assertNull(res1[1]);
		Assert.assertEquals("12", res1[2]);
		Assert.assertEquals("gg", res1[3]);
		Assert.assertEquals("hh", res1[4]);
		final String[] res2 = fp.parse("||||||||");
		Assert.assertEquals(5, res2.length);
		Assert.assertNull(res2[0]);
		Assert.assertNull(res2[1]);
		Assert.assertEquals("12", res2[2]);
		Assert.assertNull(res2[3]);
		Assert.assertNull(res2[4]);
		final String[] res3 = fp.parse("a||b||c||d||");
		Assert.assertEquals(5, res3.length);
		Assert.assertEquals("a", res3[0]);
		Assert.assertEquals("b", res3[1]);
		Assert.assertEquals("c", res3[2]);
		Assert.assertEquals("d", res3[3]);
		Assert.assertNull(res3[4]);
		final String[] res4 = fp.parse("||b||||2||3");
		Assert.assertEquals(5, res4.length);
		Assert.assertEquals("a", res4[0]);
		Assert.assertEquals("b", res4[1]);
		Assert.assertEquals("c", res4[2]);
		Assert.assertEquals("2", res4[3]);
		Assert.assertEquals("3", res4[4]);
	}

	@Test
	public void testComplex() {
		final FeedParser fp = new RepetitiveFeedParser("|^", 4);
		final String line = "|^|^|^|^a|^b|^c|^d";
		final String[] res = fp.parse(line);
		Assert.assertEquals(8, res.length);
		Assert.assertNull(res[0]);
		Assert.assertNull(res[1]);
		Assert.assertNull(res[2]);
		Assert.assertNull(res[3]);
		Assert.assertEquals("a", res[4]);
		Assert.assertEquals("b", res[5]);
		Assert.assertEquals("c", res[6]);
		Assert.assertEquals("d", res[7]);

		final String line1 = "1|^2|^3|^4|^a|^b|^c|^d";
		final String[] res1 = fp.parse(line1);
		Assert.assertEquals(8, res1.length);
		Assert.assertEquals("1", res1[0]);
		Assert.assertEquals("2", res1[1]);
		Assert.assertEquals("3", res1[2]);
		Assert.assertEquals("4", res1[3]);
		Assert.assertEquals("a", res1[4]);
		Assert.assertEquals("b", res1[5]);
		Assert.assertEquals("c", res1[6]);
		Assert.assertEquals("d", res1[7]);

		final String line2 = "|^|^|^|^|^|^c|^";
		final String[] res2 = fp.parse(line2);
		Assert.assertEquals(8, res2.length);
		Assert.assertEquals("1", res2[0]);
		Assert.assertEquals("2", res2[1]);
		Assert.assertEquals("3", res2[2]);
		Assert.assertEquals("4", res2[3]);
		Assert.assertNull(res2[4]);
		Assert.assertNull(res2[5]);
		Assert.assertEquals("c", res2[6]);
		Assert.assertNull(res2[7]);
	}

}
