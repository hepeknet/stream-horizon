package com.threeglav.bauk.feed;

import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.bauk.model.BaukAttribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.DataProcessingType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;

public class FeedParserComponentTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new FeedParserComponent(null, null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullDelimiter() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class);
		new FeedParserComponent(ff, config, null);
	}

	@Test(expected = IllegalStateException.class)
	public void testNullFeedType() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class);
		when(ff.getDelimiterString()).thenReturn(",");
		new FeedParserComponent(ff, config, null);
	}

	@Test
	public void testFirstValueNoCheck() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getDelimiterString()).thenReturn(",");
		when(ff.getData().getEachLineStartsWithCharacter()).thenReturn("9");
		when(ff.getType()).thenReturn(FactFeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config, null);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertFalse(fp.isCheckEveryLineValidity());
		Assert.assertEquals(1, fp.getExpectedTokensInEveryDataLine());
	}

	@Test
	public void testFirstValueCheck() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getDelimiterString()).thenReturn(",");
		when(ff.getData().getProcess()).thenReturn(DataProcessingType.NORMAL);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getData().getAttributes()).thenReturn(attrs);
		when(ff.getData().getEachLineStartsWithCharacter()).thenReturn("9");
		when(ff.getType()).thenReturn(FactFeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config, null);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertTrue(fp.isCheckEveryLineValidity());
		Assert.assertEquals(11, fp.getExpectedTokensInEveryDataLine());
	}

	@Test(expected = IllegalStateException.class)
	public void testFirstValueNotSetButStrictRequired() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getDelimiterString()).thenReturn(",");
		when(ff.getData().getProcess()).thenReturn(DataProcessingType.NORMAL);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getData().getAttributes()).thenReturn(attrs);
		when(ff.getData().getEachLineStartsWithCharacter()).thenReturn(null);
		when(ff.getType()).thenReturn(FactFeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config, null);
		Assert.assertEquals("9", fp.getFirstStringInEveryLine());
		Assert.assertTrue(fp.isCheckEveryLineValidity());
		Assert.assertEquals(10, fp.getExpectedTokensInEveryDataLine());
	}

	@Test
	public void testFirstValueNotSet() {
		final BaukConfiguration config = Mockito.mock(BaukConfiguration.class);
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getDelimiterString()).thenReturn(",");
		when(ff.getData().getProcess()).thenReturn(DataProcessingType.NO_VALIDATION);
		final ArrayList<BaukAttribute> attrs = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			final BaukAttribute a = new BaukAttribute();
			a.setName("n_" + i);
			attrs.add(a);
		}
		when(ff.getData().getAttributes()).thenReturn(attrs);
		when(ff.getData().getEachLineStartsWithCharacter()).thenReturn(null);
		when(ff.getType()).thenReturn(FactFeedType.FULL);
		final FeedParserComponent fp = new FeedParserComponent(ff, config, null);
		Assert.assertNull(fp.getFirstStringInEveryLine());
		Assert.assertFalse(fp.isCheckEveryLineValidity());
		Assert.assertEquals(10, fp.getExpectedTokensInEveryDataLine());
	}

}
