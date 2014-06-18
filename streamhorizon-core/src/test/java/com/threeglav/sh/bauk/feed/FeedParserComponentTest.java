package com.threeglav.sh.bauk.feed;

import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.DataProcessingType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedType;

public class FeedParserComponentTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new FeedParserComponent(null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullDelimiter() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class);
		new FeedParserComponent(ff, config);
	}

	@Test(expected = IllegalStateException.class)
	public void testNullFeedType() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter()).thenReturn("9");
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		new FeedParserComponent(ff, config);
	}

	@Test
	public void testFirstValueNoCheck() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		when(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter()).thenReturn("9");
		when(ff.getType()).thenReturn(FeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertFalse(fp.isCheckEveryLineValidity());
		Assert.assertEquals(1, fp.getExpectedTokensInEveryDataLine());
	}

	@Test
	public void testFirstValueCheck() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		when(ff.getSourceFormatDefinition().getData().getProcess()).thenReturn(DataProcessingType.NORMAL);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getSourceFormatDefinition().getData().getAttributes()).thenReturn(attrs);
		when(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter()).thenReturn("9");
		when(ff.getType()).thenReturn(FeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertTrue(fp.isCheckEveryLineValidity());
		Assert.assertEquals(11, fp.getExpectedTokensInEveryDataLine());
	}

	@Test(expected = IllegalStateException.class)
	public void testFirstValueNotSetButStrictRequired() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		when(ff.getSourceFormatDefinition().getData().getProcess()).thenReturn(DataProcessingType.NORMAL);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getSourceFormatDefinition().getData().getAttributes()).thenReturn(attrs);
		when(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter()).thenReturn(null);
		when(ff.getType()).thenReturn(FeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertTrue(fp.isCheckEveryLineValidity());
		Assert.assertEquals(10, fp.getExpectedTokensInEveryDataLine());
	}

	@Test
	public void testFirstValueNotSet() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		when(ff.getSourceFormatDefinition().getData().getProcess()).thenReturn(DataProcessingType.NO_VALIDATION);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getSourceFormatDefinition().getData().getAttributes()).thenReturn(attrs);
		when(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter()).thenReturn(null);
		when(ff.getType()).thenReturn(FeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config);
		Assert.assertNull(fp.getFirstStringInEveryLine());
		Assert.assertFalse(fp.isCheckEveryLineValidity());
		Assert.assertEquals(10, fp.getExpectedTokensInEveryDataLine());
	}

}
