package com.threeglav.bauk.feed;

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.NaturalKey;
import com.threeglav.bauk.model.SqlStatements;

public class BulkOutputValuesResolverTest {

	@Test
	public void testNull() {
		try {
			new BulkOutputValuesResolver(null, Mockito.mock(Config.class), Mockito.mock(CacheInstanceManager.class), Mockito.mock(DbHandler.class),
					null);
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new BulkOutputValuesResolver(Mockito.mock(FactFeed.class), null, Mockito.mock(CacheInstanceManager.class), Mockito.mock(DbHandler.class),
					null);
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new BulkOutputValuesResolver(Mockito.mock(FactFeed.class), Mockito.mock(Config.class), null, Mockito.mock(DbHandler.class), null);
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new BulkOutputValuesResolver(Mockito.mock(FactFeed.class), Mockito.mock(Config.class), Mockito.mock(CacheInstanceManager.class), null,
					null);
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimple() {
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes()).thenReturn(this.createBulkOutputAttributes(4));
		when(ff.getData().getAttributes()).thenReturn(this.createFactFeedAttributes(5));
		when(ff.getDelimiterString()).thenReturn(",");
		final Config conf = Mockito.mock(Config.class);
		when(conf.getDimensions()).thenReturn(this.createDimensions(5));
		when(conf.getDimensionMap()).thenReturn(this.createDimensionMap(5));
		final CacheInstanceManager ch = Mockito.mock(CacheInstanceManager.class);
		final CacheInstance cacheInstance = Mockito.mock(CacheInstance.class);
		when(ch.getCacheInstance(Matchers.<String> any())).thenReturn(cacheInstance);
		when(cacheInstance.getSurrogateKey(Matchers.<String> any())).thenAnswer(new Answer<String>() {
			@Override
			public String answer(final InvocationOnMock invocation) throws Throwable {
				final Object[] args = invocation.getArguments();
				return (String) args[0];
			}
		});
		final DbHandler dh = Mockito.mock(DbHandler.class);
		final BulkOutputValuesResolver bomhc = new BulkOutputValuesResolver(ff, conf, ch, dh, null);
		final String latestValue = bomhc.resolveValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, null, null);
		Assert.assertNotNull(latestValue);
		Assert.assertEquals("2,3,6,7", latestValue);
	}

	@Test
	public void testSmallerFeed() {
		final FactFeed ff = Mockito.mock(FactFeed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getBulkLoadDefinition().getBulkLoadFormatDefinition().getAttributes()).thenReturn(this.createBulkOutputAttributes(1));
		when(ff.getData().getAttributes()).thenReturn(this.createFactFeedAttributes(4));
		when(ff.getDelimiterString()).thenReturn(",");
		final Config conf = Mockito.mock(Config.class);
		when(conf.getDimensions()).thenReturn(this.createDimensions(4));
		when(conf.getDimensionMap()).thenReturn(this.createDimensionMap(4));
		final CacheInstanceManager ch = Mockito.mock(CacheInstanceManager.class);
		final CacheInstance cacheInstance = Mockito.mock(CacheInstance.class);
		when(ch.getCacheInstance(Matchers.<String> any())).thenReturn(cacheInstance);
		when(cacheInstance.getSurrogateKey(Matchers.<String> any())).thenAnswer(new Answer<String>() {
			@Override
			public String answer(final InvocationOnMock invocation) throws Throwable {
				final Object[] args = invocation.getArguments();
				return (String) args[0];
			}
		});
		final DbHandler dh = Mockito.mock(DbHandler.class);
		final BulkOutputValuesResolver bomhc = new BulkOutputValuesResolver(ff, conf, ch, dh, null);
		final String latestReceivedValue = bomhc.resolveValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
				"14", "15", "16" }, null, null);
		Assert.assertNotNull(latestReceivedValue);
		Assert.assertEquals("2", latestReceivedValue);
	}

	private ArrayList<Attribute> createFactFeedAttributes(final int num) {
		final ArrayList<Attribute> attrs = new ArrayList<Attribute>();
		for (int i = 0; i < num; i++) {
			final Attribute at = new Attribute();
			at.setName("at_" + i);
			attrs.add(at);
			final Attribute at1 = new Attribute();
			at1.setName("nk" + i);
			attrs.add(at1);
		}
		return attrs;
	}

	private ArrayList<Attribute> createBulkOutputAttributes(final int num) {
		final ArrayList<Attribute> attrs = new ArrayList<Attribute>();
		for (int i = 0; i < num; i++) {
			final Attribute at = new Attribute();
			if (i % 2 == 0) {
				at.setName("dimension.dim_" + i);
			} else {
				at.setName("feed.at_" + i);
			}
			attrs.add(at);
		}
		return attrs;
	}

	private Map<String, Dimension> createDimensionMap(final int num) {
		final ArrayList<Dimension> dims = this.createDimensions(num);
		final Map<String, Dimension> mapped = new HashMap<String, Dimension>();
		for (final Dimension d : dims) {
			mapped.put(d.getName(), d);
		}
		return mapped;
	}

	private ArrayList<Dimension> createDimensions(final int num) {
		final ArrayList<Dimension> dims = new ArrayList<Dimension>();
		for (int i = 0; i < num; i++) {
			final Dimension d = new Dimension();
			d.setName("dim_" + i);
			final ArrayList<NaturalKey> nat = new ArrayList<NaturalKey>();
			final NaturalKey nk = new NaturalKey();
			nk.setName("nk" + i);
			nat.add(nk);
			d.setNaturalKeys(nat);
			dims.add(d);
			final SqlStatements ss = new SqlStatements();
			d.setSqlStatements(ss);
		}
		return dims;
	}

}
