package org.treeglav.bauk.parser;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.FullFeedParser;

public class FullFeedParserTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new FullFeedParser(null, 2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {
		new FullFeedParser(" ", 2);
	}

	@Test
	public void testSimple() {
		final FeedParser fp = new FullFeedParser("@@", 4);
		final String[] res = fp.parse("1@@2@@3@@");
		Assert.assertEquals(4, res.length);
		Assert.assertEquals("1", res[0]);
		Assert.assertEquals("2", res[1]);
		Assert.assertEquals("3", res[2]);
		Assert.assertNull(res[3]);

		final String[] res1 = fp.parse("@@@@3@@");
		Assert.assertEquals(4, res1.length);
		Assert.assertNull(res1[0]);
		Assert.assertNull(res1[1]);
		Assert.assertEquals("3", res1[2]);
		Assert.assertNull(res1[3]);

		final FeedParser fp1 = new FullFeedParser("@@", 5);
		final String[] res2 = fp1.parse("@@' '@@ @@3@@");
		Assert.assertEquals(5, res2.length);
		Assert.assertNull(res2[0]);
		Assert.assertEquals("' '", res2[1]);
		Assert.assertEquals(" ", res2[2]);
		Assert.assertEquals("3", res2[3]);
		Assert.assertNull(res2[4]);
	}

	@Test
	public void testSingle() {
		final FeedParser fp = new FullFeedParser(",", 3);
		final String[] res = fp.parse("abc,def,abc");
		Assert.assertEquals(3, res.length);
	}

}
