package org.treeglav.bauk.parser;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.FullFeedParser;

public class FullFeedParserTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new FullFeedParser(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {
		new FullFeedParser(" ");
	}

	@Test
	public void testSimple() {
		final FeedParser fp = new FullFeedParser("@@");
		final String[] res = fp.parse("1@@2@@3@@");
		Assert.assertEquals(4, res.length);
		Assert.assertEquals("1", res[0]);
		Assert.assertEquals("2", res[1]);
		Assert.assertEquals("3", res[2]);
		Assert.assertEquals("", res[3]);

		final String[] res1 = fp.parse("@@@@3@@");
		Assert.assertEquals(4, res1.length);
		Assert.assertEquals("", res1[0]);
		Assert.assertEquals("", res1[1]);
		Assert.assertEquals("3", res1[2]);
		Assert.assertEquals("", res1[3]);
	}

}
