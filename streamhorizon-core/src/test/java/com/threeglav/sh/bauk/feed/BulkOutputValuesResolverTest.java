package com.threeglav.sh.bauk.feed;

import static org.mockito.Mockito.spy;
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

import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.DimensionType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.model.SqlStatements;

public class BulkOutputValuesResolverTest {

	@Test
	public void testNull() {
		try {
			new BulkOutputValuesResolver(null, Mockito.mock(BaukConfiguration.class), Mockito.mock(CacheInstanceManager.class));
			Assert.fail("nok");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new BulkOutputValuesResolver(Mockito.mock(Feed.class), null, Mockito.mock(CacheInstanceManager.class));
			Assert.fail("nok");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new BulkOutputValuesResolver(Mockito.mock(Feed.class), Mockito.mock(BaukConfiguration.class), null);
			Assert.fail("nok");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimple() {
		BulkOutputValuesResolver.cachedDimensionHandlers.clear();
		BulkOutputValuesResolver.alreadyStartedCreatingDimensionNames.clear();
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getBulkLoadDefinition().getTargetFormatDefinition().getAttributes()).thenReturn(this.createBulkOutputAttributes(4));
		when(ff.getSourceFormatDefinition().getData().getAttributes()).thenReturn(this.createFactFeedAttributes(5));
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		when(conf.getDimensions()).thenReturn(this.createDimensions(5));
		when(conf.getDimensionMap()).thenReturn(this.createDimensionMap(5));
		when(conf.getDatabaseStringLiteral()).thenReturn("'");
		final CacheInstanceManager ch = Mockito.mock(CacheInstanceManager.class);
		final CacheInstance cacheInstance = Mockito.mock(CacheInstance.class);
		when(ch.getCacheInstance(Matchers.<String> any())).thenReturn(cacheInstance);
		when(cacheInstance.getSurrogateKey(Matchers.<String> any())).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(final InvocationOnMock invocation) throws Throwable {
				final Object[] args = invocation.getArguments();
				return Integer.valueOf(String.valueOf(args[0]));
			}
		});
		final BulkOutputValuesResolver bomhc = spy(new BulkOutputValuesResolver(ff, conf, ch));
		final Object[] latestValue = bomhc.resolveValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, null);
		Assert.assertEquals(4, latestValue.length);
		Assert.assertEquals("2", String.valueOf(latestValue[0]));
		Assert.assertEquals("3", String.valueOf(latestValue[1]));
		Assert.assertEquals("6", String.valueOf(latestValue[2]));
		Assert.assertEquals("CONST_VAL", latestValue[3]);

		final Object[] lastLineValues = bomhc.resolveLastLineValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" }, null);
		Assert.assertEquals(4, lastLineValues.length);
		Assert.assertEquals("2", String.valueOf(lastLineValues[0]));
		Assert.assertEquals("3", String.valueOf(lastLineValues[1]));
		Assert.assertEquals("6", String.valueOf(lastLineValues[2]));
		Assert.assertEquals("CONST_VAL", lastLineValues[3]);
	}

	@Test
	public void testSmallerFeed() {
		BulkOutputValuesResolver.cachedDimensionHandlers.clear();
		BulkOutputValuesResolver.alreadyStartedCreatingDimensionNames.clear();
		final Feed ff = Mockito.mock(Feed.class, Mockito.RETURNS_DEEP_STUBS);
		when(ff.getBulkLoadDefinition().getTargetFormatDefinition().getAttributes()).thenReturn(this.createBulkOutputAttributes(1));
		when(ff.getSourceFormatDefinition().getData().getAttributes()).thenReturn(this.createFactFeedAttributes(4));
		when(ff.getSourceFormatDefinition().getDelimiterString()).thenReturn(",");
		final BaukConfiguration conf = Mockito.mock(BaukConfiguration.class);
		when(conf.getDimensions()).thenReturn(this.createDimensions(4));
		when(conf.getDimensionMap()).thenReturn(this.createDimensionMap(4));
		when(conf.getDatabaseStringLiteral()).thenReturn("'");
		final CacheInstanceManager ch = Mockito.mock(CacheInstanceManager.class);
		final CacheInstance cacheInstance = Mockito.mock(CacheInstance.class);
		when(ch.getCacheInstance(Matchers.<String> any())).thenReturn(cacheInstance);
		when(cacheInstance.getSurrogateKey(Matchers.<String> any())).thenAnswer(new Answer<Integer>() {
			@Override
			public Integer answer(final InvocationOnMock invocation) throws Throwable {
				final Object[] args = invocation.getArguments();
				return Integer.valueOf(String.valueOf(args[0]));
			}
		});
		final BulkOutputValuesResolver bomhc = spy(new BulkOutputValuesResolver(ff, conf, ch));
		final Object[] latestReceivedValue = bomhc.resolveValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
				"14", "15", "16" }, null);
		Assert.assertEquals(1, latestReceivedValue.length);
		Assert.assertEquals("2", String.valueOf(latestReceivedValue[0]));

		final Object[] lastLineValue = bomhc.resolveLastLineValues(new String[] { "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
				"13", "14", "15", "16" }, null);
		Assert.assertEquals(1, lastLineValue.length);
		Assert.assertEquals("2", String.valueOf(lastLineValue[0]));
	}

	private ArrayList<BaukAttribute> createFactFeedAttributes(final int num) {
		final ArrayList<BaukAttribute> attrs = new ArrayList<BaukAttribute>();
		for (int i = 0; i < num; i++) {
			final BaukAttribute at = new BaukAttribute();
			at.setName("at_" + i);
			attrs.add(at);
			final BaukAttribute at1 = new BaukAttribute();
			at1.setName("nk" + i);
			attrs.add(at1);
		}
		return attrs;
	}

	private ArrayList<BaukAttribute> createBulkOutputAttributes(final int num) {
		final ArrayList<BaukAttribute> attrs = new ArrayList<BaukAttribute>();
		for (int i = 0; i < num; i++) {
			final BaukAttribute at = new BaukAttribute();
			if (i % 2 == 0) {
				at.setName("dimension.dim_" + i);
			} else if (i != 3) {
				at.setName("feed.at_" + i);
			} else {
				at.setName("");
				at.setConstantValue("CONST_VAL");
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
			d.setType(DimensionType.INSERT_ONLY);
			d.setName("dim_" + i);
			final ArrayList<MappedColumn> nat = new ArrayList<MappedColumn>();
			final MappedColumn nk = new MappedColumn();
			nk.setName("nk" + i);
			nk.setNaturalKey(true);
			nat.add(nk);
			d.setMappedColumns(nat);
			dims.add(d);
			final SqlStatements ss = new SqlStatements();
			ss.setSelectRecordIdentifier("select 1");
			d.setSqlStatements(ss);
		}
		return dims;
	}

}
