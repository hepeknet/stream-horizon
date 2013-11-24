package com.threeglav.bauk.dimension;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.BulkLoadOutputValueHandler;
import com.threeglav.bauk.dimension.cache.CacheInstance;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.Data;
import com.threeglav.bauk.model.Dimension;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.NaturalKey;
import com.threeglav.bauk.model.SqlStatements;

public class DimensionHandlerTest {

	private String lastRequiredFromCache;
	private String lastStatementToExecute;

	@Test
	public void testNulls() {
		try {
			new DimensionHandler(null, this.createFactFeed(), this.createCacheHandler(), this.createDbHandler(), 0, null, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new DimensionHandler(this.createDimension(), null, this.createCacheHandler(), this.createDbHandler(), 0, null, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new DimensionHandler(this.createDimension(), this.createFactFeed(), null, this.createDbHandler(), 0, null, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), null, 0, null, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimple() {
		final DimensionHandler dh = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 0, null, this.createConfig());
		Assert.assertEquals(5, dh.getNaturalKeyPositions().length);
		Assert.assertEquals(5, dh.getNaturalKeyNames().length);
		Assert.assertEquals("nk_2", dh.getNaturalKeyNames()[2]);
		Assert.assertEquals(6, dh.getNaturalKeyPositions()[2]);
		Assert.assertEquals(3, dh.getNaturalKeyPositions()[1]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullParsedLine() {
		final BulkLoadOutputValueHandler dh = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 0, null, this.createConfig());
		dh.getBulkLoadValue(null, null, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSmallParsedLine() {
		final BulkLoadOutputValueHandler dh = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 0, null, this.createConfig());
		dh.getBulkLoadValue(new String[] { "a", "b", "c" }, null, null);
	}

	@Test
	public void testLookups() {
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final BulkLoadOutputValueHandler dh = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 0, null, this.createConfig());
		final String[] parsedLine = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final String key = dh.getBulkLoadValue(parsedLine, null, null);
		Assert.assertEquals("100", key);
		final String nkLookup = "c" + BaukConstants.NATURAL_KEY_DELIMITER + "d" + BaukConstants.NATURAL_KEY_DELIMITER + "g"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "f" + BaukConstants.NATURAL_KEY_DELIMITER + "a";
		Assert.assertEquals(nkLookup, lastRequiredFromCache);
		Assert.assertEquals(
				"insert into dim where nk_0=c and nk_4=a and nk_2=g and a='b' and nk_100=${nk_100} or p='${header.h1}' or p1='header.h2'",
				lastStatementToExecute);
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final BulkLoadOutputValueHandler dh1 = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 0, null, this.createConfig());
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		final String key1 = dh1.getBulkLoadValue(parsedLine1, headerValues, null);
		Assert.assertEquals("100", key1);
		final String nkLookup1 = "c" + BaukConstants.NATURAL_KEY_DELIMITER + "d" + BaukConstants.NATURAL_KEY_DELIMITER + "g"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "f" + BaukConstants.NATURAL_KEY_DELIMITER + "a";
		Assert.assertEquals(nkLookup1, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=c and nk_4=a and nk_2=g and a='b' and nk_100=${nk_100} or p='100' or p1='header.h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupsWithOffset() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final BulkLoadOutputValueHandler dh1 = new DimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 1, null, this.createConfig());
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		final String key1 = dh1.getBulkLoadValue(parsedLine1, headerValues, null);
		Assert.assertEquals("100", key1);
		final String nkLookup1 = "d" + BaukConstants.NATURAL_KEY_DELIMITER + "e" + BaukConstants.NATURAL_KEY_DELIMITER + "h"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "g" + BaukConstants.NATURAL_KEY_DELIMITER + "b";
		Assert.assertEquals(nkLookup1, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=d and nk_4=b and nk_2=h and a='b' and nk_100=${nk_100} or p='100' or p1='header.h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupNoNaturalKeys() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final BulkLoadOutputValueHandler dh1 = new DimensionHandler(this.createDimension(0), this.createFactFeed(), this.createCacheHandler(),
				this.createDbHandler(), 1, null, this.createConfig());
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		final String key1 = dh1.getBulkLoadValue(parsedLine1, headerValues, null);
		Assert.assertEquals("100", key1);
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertEquals(
				"insert into dim where nk_0=${nk_0} and nk_4=${nk_4} and nk_2=${nk_2} and a='b' and nk_100=${nk_100} or p='100' or p1='header.h2'",
				lastStatementToExecute);
	}

	private CacheInstance createCacheHandler() {

		return new CacheInstance() {

			@Override
			public String getSurrogateKey(final String naturalKey) {
				lastRequiredFromCache = naturalKey;
				return null;
			}

			@Override
			public void put(final String naturalKey, final String surrogateKey) {

			}
		};
	}

	private BaukConfiguration createConfig() {
		final BaukConfiguration bc = Mockito.mock(BaukConfiguration.class);
		when(bc.getDatabaseStringLiteral()).thenReturn("'");
		return bc;
	}

	private DbHandler createDbHandler() {
		return new DbHandler() {

			@Override
			public Long executeQueryStatementAndReturnKey(final String statement) {
				lastStatementToExecute = statement;
				return 100l;
			}

			@Override
			public Long executeInsertStatementAndReturnKey(final String statement) {
				lastStatementToExecute = statement;
				return 100l;
			}

			@Override
			public void executeInsertOrUpdateStatement(final String statement) {

			}

			@Override
			public List<String[]> queryForDimensionKeys(final String statement, final int numberOfNaturalKeyColumns) {
				return null;
			}
		};
	}

	private Dimension createDimension(final int naturalKeysCount) {
		final Dimension dim = new Dimension();
		dim.setName("dim1");
		final ArrayList<NaturalKey> naturalKeys = new ArrayList<NaturalKey>();
		for (int i = 0; i < naturalKeysCount; i++) {
			final String nkName = "nk_" + i;
			final NaturalKey nk = new NaturalKey();
			nk.setName(nkName);
			naturalKeys.add(nk);
		}
		dim.setNaturalKeys(naturalKeys);
		final SqlStatements sqlStatements = new SqlStatements();
		sqlStatements
				.setInsertSingle("insert into dim where nk_0=${nk_0} and nk_4=${nk_4} and nk_2=${nk_2} and a='b' and nk_100=${nk_100} or p='${header.h1}' or p1='header.h2'");
		dim.setSqlStatements(sqlStatements);
		return dim;
	}

	private Dimension createDimension() {
		return this.createDimension(5);
	}

	private FactFeed createFactFeed() {
		final FactFeed ff = new FactFeed();
		ff.setName("ff1");
		final Data data = new Data();
		final ArrayList<Attribute> attributes = new ArrayList<Attribute>();

		final Attribute atr = new Attribute();
		atr.setName("nk_4");
		attributes.add(atr);

		final Attribute atr1 = new Attribute();
		atr1.setName("x_4");
		attributes.add(atr1);

		final Attribute atr2 = new Attribute();
		atr2.setName("nk_0");
		attributes.add(atr2);

		final Attribute atr3 = new Attribute();
		atr3.setName("nk_1");
		attributes.add(atr3);

		final Attribute atr4 = new Attribute();
		atr4.setName("yyy");
		attributes.add(atr4);

		final Attribute atr5 = new Attribute();
		atr5.setName("nk_3");
		attributes.add(atr5);

		final Attribute atr6 = new Attribute();
		atr6.setName("nk_2");
		attributes.add(atr6);
		data.setAttributes(attributes);
		ff.setData(data);
		return ff;
	}

}
