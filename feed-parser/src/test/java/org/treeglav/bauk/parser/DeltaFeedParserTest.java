package org.treeglav.bauk.parser;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.parser.AbstractFeedParser;
import com.threeglav.bauk.parser.DeltaFeedParser;
import com.threeglav.bauk.parser.FeedParser;

public class DeltaFeedParserTest {

	@Test
	public void testSimple() {
		final FeedParser fp = new DeltaFeedParser("|^");
		final String[] res = fp.parse("ab|^cd|^12|^345|^");
		Assert.assertEquals(5, res.length);
		Assert.assertEquals("ab", res[0]);
		Assert.assertEquals("cd", res[1]);
		Assert.assertEquals("12", res[2]);
		Assert.assertEquals("345", res[3]);
		Assert.assertEquals("", res[4]);

		final String[] res1 = fp.parse("|^|^122|^|^ghj");
		Assert.assertEquals(5, res1.length);
		Assert.assertEquals("ab", res1[0]);
		Assert.assertEquals("cd", res1[1]);
		Assert.assertEquals("122", res1[2]);
		Assert.assertEquals("345", res1[3]);
		Assert.assertEquals("ghj", res1[4]);

		final String[] res2 = fp.parse("kk|^mm|^|^ss|^");
		Assert.assertEquals(5, res2.length);
		Assert.assertEquals("kk", res2[0]);
		Assert.assertEquals("mm", res2[1]);
		Assert.assertEquals("122", res2[2]);
		Assert.assertEquals("ss", res2[3]);
		Assert.assertEquals("ghj", res2[4]);

	}

	@Test
	public void testAnother() {
		final FeedParser fp = new DeltaFeedParser("@@");
		final AbstractFeedParser afp = (AbstractFeedParser) fp;
		afp.setExpectedTokens(5);
		final String[] res = fp.parse("cc@@pound@@GPB@@12@@1.2");
		Assert.assertEquals(5, res.length);
		Assert.assertEquals("cc", res[0]);
		Assert.assertEquals("1.2", res[4]);
		fp.parse("cc@@pound@@GPB@@12@@1.2@@ab@@cd");
	}

}
