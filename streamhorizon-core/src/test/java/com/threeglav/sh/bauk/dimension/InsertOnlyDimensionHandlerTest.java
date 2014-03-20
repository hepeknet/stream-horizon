package com.threeglav.sh.bauk.dimension;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.dimension.cache.CacheInstance;
import com.threeglav.sh.bauk.dimension.db.DbHandler;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Data;
import com.threeglav.sh.bauk.model.Dimension;
import com.threeglav.sh.bauk.model.FactFeed;
import com.threeglav.sh.bauk.model.MappedColumn;
import com.threeglav.sh.bauk.model.SqlStatements;

public class InsertOnlyDimensionHandlerTest {

	private String lastRequiredFromCache;
	private String lastStatementToExecute;

	@Test
	public void testNulls() {
		try {
			new InsertOnlyDimensionHandler(null, this.createFactFeed(), this.createCacheHandler(), 0, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new InsertOnlyDimensionHandler(this.createDimension(), null, this.createCacheHandler(), 0, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
		try {
			new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), null, 0, this.createConfig());
			fail("Should fail");
		} catch (final IllegalArgumentException ok) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimple() {
		final InsertOnlyDimensionHandler dh = new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig());
		Assert.assertEquals(5, dh.getMappedColumnPositionsInFeed().length);
		Assert.assertEquals(5, dh.getNaturalKeyPositionsInFeed().length);
		Assert.assertEquals(5, dh.getMappedColumnNames().length);
		Assert.assertEquals(5, dh.getNaturalKeyNames().length);
		Assert.assertEquals("nk_2", dh.getMappedColumnNames()[2]);
		Assert.assertEquals(6, dh.getMappedColumnPositionsInFeed()[2]);
		Assert.assertEquals(3, dh.getMappedColumnPositionsInFeed()[1]);
		Assert.assertEquals("nk_2", dh.getNaturalKeyNames()[2]);
		Assert.assertEquals(6, dh.getNaturalKeyPositionsInFeed()[2]);
		Assert.assertEquals(3, dh.getNaturalKeyPositionsInFeed()[1]);
	}

	@Test
	public void testNaturalKeyMappedToHeader() {
		final InsertOnlyDimensionHandler dh = new InsertOnlyDimensionHandler(this.createDimension(10, false), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig());
		Assert.assertEquals(10, dh.getMappedColumnPositionsInFeed().length);
		Assert.assertEquals(10, dh.getNaturalKeyPositionsInFeed().length);
		Assert.assertEquals(10, dh.getMappedColumnNames().length);
		Assert.assertEquals(10, dh.getNaturalKeyNames().length);
		Assert.assertEquals("nk_2", dh.getMappedColumnNames()[2]);
		Assert.assertEquals(6, dh.getMappedColumnPositionsInFeed()[2]);
		Assert.assertEquals(3, dh.getMappedColumnPositionsInFeed()[1]);
		Assert.assertEquals("nk_2", dh.getNaturalKeyNames()[2]);
		Assert.assertEquals(6, dh.getNaturalKeyPositionsInFeed()[2]);
		Assert.assertEquals(3, dh.getNaturalKeyPositionsInFeed()[1]);

		Assert.assertEquals("nk_9", dh.getMappedColumnNames()[9]);
		Assert.assertEquals(-1, dh.getMappedColumnPositionsInFeed()[9]);
		Assert.assertEquals("nk_9", dh.getNaturalKeyNames()[9]);
		Assert.assertEquals(-1, dh.getNaturalKeyPositionsInFeed()[9]);
	}

	@Test
	public void testNaturalAndMapped() {
		final InsertOnlyDimensionHandler dh = spy(new InsertOnlyDimensionHandler(this.createDimensionNaturalAndMapped(5), this.createFactFeed(),
				this.createCacheHandler(), 0, this.createConfig()));
		doReturn(this.createDbHandler()).when(dh).getDbHandler();
		Assert.assertEquals(7, dh.getMappedColumnPositionsInFeed().length);
		Assert.assertEquals(5, dh.getNaturalKeyPositionsInFeed().length);
		Assert.assertEquals(7, dh.getMappedColumnNames().length);
		Assert.assertEquals(5, dh.getNaturalKeyNames().length);
		Assert.assertEquals("nk_2", dh.getMappedColumnNames()[2]);
		Assert.assertEquals(6, dh.getMappedColumnPositionsInFeed()[2]);
		Assert.assertEquals("mapped1", dh.getMappedColumnNames()[5]);
		Assert.assertEquals(7, dh.getMappedColumnPositionsInFeed()[5]);
		Assert.assertEquals("mapped2", dh.getMappedColumnNames()[6]);
		Assert.assertEquals(9, dh.getMappedColumnPositionsInFeed()[6]);
		Assert.assertEquals(3, dh.getMappedColumnPositionsInFeed()[1]);
		Assert.assertEquals("nk_2", dh.getNaturalKeyNames()[2]);
		Assert.assertEquals(6, dh.getNaturalKeyPositionsInFeed()[2]);
		Assert.assertEquals(3, dh.getNaturalKeyPositionsInFeed()[1]);

		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final String[] parsedLine = { "aa", "bb", "cc", "dd", "ee", "ff", "gg", "hh", "ii", "jj" };
		final HashMap<String, String> globalAttrs = new HashMap<String, String>();
		globalAttrs.put("nk_100", "g1");
		globalAttrs.put("h1", "g2");
		final Object key = dh.getBulkLoadValue(parsedLine, globalAttrs);
		Assert.assertEquals("100", "" + key);
		final String nkLookup = "cc" + BaukConstants.NATURAL_KEY_DELIMITER + "dd" + BaukConstants.NATURAL_KEY_DELIMITER + "gg"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "ff" + BaukConstants.NATURAL_KEY_DELIMITER + "aa";
		Assert.assertEquals(nkLookup, lastRequiredFromCache);
		Assert.assertEquals(
				"insert into dim where nk_0=cc and nk_4=aa and nk_2=gg and a='b' and nk_100=g1 or p='g2' or p1='h2' and mapped1='hh' or mapped2>'jj'",
				lastStatementToExecute);
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
	}

	@Test(expected = NullPointerException.class)
	public void testNullParsedLine() {
		final InsertOnlyDimensionHandler dh = new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig());
		dh.getBulkLoadValue(null, null);
	}

	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testSmallParsedLine() {
		final InsertOnlyDimensionHandler dh = new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig());
		dh.getBulkLoadValue(new String[] { "a", "b", "c" }, null);
	}

	@Test
	public void testLookups() {
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh = spy(new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig()));
		doReturn(this.createDbHandler()).when(dh).getDbHandler();
		final String[] parsedLine = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final HashMap<String, String> globalAttrs = new HashMap<String, String>();
		globalAttrs.put("nk_100", "100");
		final Object key = dh.getLastLineBulkLoadValue(parsedLine, globalAttrs);
		Assert.assertEquals("100", "" + key);
		final String nkLookup = "c" + BaukConstants.NATURAL_KEY_DELIMITER + "d" + BaukConstants.NATURAL_KEY_DELIMITER + "g"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "f" + BaukConstants.NATURAL_KEY_DELIMITER + "a";
		Assert.assertEquals(nkLookup, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=c and nk_4=a and nk_2=g and a='b' and nk_100=100 or p=NULL or p1='h2'",
				lastStatementToExecute);
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh1 = spy(new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 0,
				this.createConfig()));
		doReturn(this.createDbHandler()).when(dh1).getDbHandler();
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		headerValues.put("nk_100", "g100");
		Assert.assertEquals(3, headerValues.size());
		final Object key1 = dh1.getBulkLoadValue(parsedLine1, headerValues);
		Assert.assertEquals(3, headerValues.size());
		Assert.assertEquals("100", "" + key1);
		final String nkLookup1 = "c" + BaukConstants.NATURAL_KEY_DELIMITER + "d" + BaukConstants.NATURAL_KEY_DELIMITER + "g"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "f" + BaukConstants.NATURAL_KEY_DELIMITER + "a";
		Assert.assertEquals(nkLookup1, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=c and nk_4=a and nk_2=g and a='b' and nk_100=g100 or p='100' or p1='h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupsWithOffsetLastLineDisabled() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh1 = spy(new InsertOnlyDimensionHandler(this.createDimension(), this.createFactFeed(), this.createCacheHandler(), 1,
				this.createConfig()));
		doReturn(this.createDbHandler()).when(dh1).getDbHandler();
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		Assert.assertEquals(2, headerValues.size());
		final Object key1 = dh1.getLastLineBulkLoadValue(parsedLine1, headerValues);
		Assert.assertEquals(2, headerValues.size());
		Assert.assertNull(headerValues.get("dim1.sk"));
		Assert.assertEquals("100", "" + key1);
		final String nkLookup1 = "d" + BaukConstants.NATURAL_KEY_DELIMITER + "e" + BaukConstants.NATURAL_KEY_DELIMITER + "h"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "g" + BaukConstants.NATURAL_KEY_DELIMITER + "b";
		Assert.assertEquals(nkLookup1, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=d and nk_4=b and nk_2=h and a='b' and nk_100=NULL or p='100' or p1='h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupsWithOffsetLastLineEnabled() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh1 = spy(new InsertOnlyDimensionHandler(this.createDimension(true), this.createFactFeed(), this.createCacheHandler(), 1,
				this.createConfig()));
		doReturn(this.createDbHandler()).when(dh1).getDbHandler();
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		Assert.assertEquals(2, headerValues.size());
		final Object key1 = dh1.getLastLineBulkLoadValue(parsedLine1, headerValues);
		Assert.assertEquals(3, headerValues.size());
		Assert.assertEquals("100", headerValues.get("dim1.sk"));
		Assert.assertEquals("100", "" + key1);
		final String nkLookup1 = "d" + BaukConstants.NATURAL_KEY_DELIMITER + "e" + BaukConstants.NATURAL_KEY_DELIMITER + "h"
				+ BaukConstants.NATURAL_KEY_DELIMITER + "g" + BaukConstants.NATURAL_KEY_DELIMITER + "b";
		Assert.assertEquals(nkLookup1, lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=d and nk_4=b and nk_2=h and a='b' and nk_100=NULL or p='100' or p1='h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupNoNaturalKeysLastLineEnabled() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh1 = Mockito.spy(new InsertOnlyDimensionHandler(this.createDimension(0, true), this.createFactFeed(),
				this.createCacheHandler(), 1, this.createConfig()));
		doReturn(this.createDbHandler()).when(dh1).getDbHandler();
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		headerValues.put("nk_0", "g1");
		headerValues.put("nk_4", "g2");
		headerValues.put("nk_2", "g3");
		final Object key1 = dh1.getLastLineBulkLoadValue(parsedLine1, headerValues);
		Assert.assertEquals("" + key1, headerValues.get("dim1.sk"));
		Assert.assertEquals("100", "" + key1);
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=g1 and nk_4=g2 and nk_2=g3 and a='b' and nk_100=NULL or p='100' or p1='h2'",
				lastStatementToExecute);
	}

	@Test
	public void testLookupNoNaturalKeysLastLineDisabled() {
		lastRequiredFromCache = null;
		lastStatementToExecute = null;
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertNull(lastStatementToExecute);
		final InsertOnlyDimensionHandler dh1 = Mockito.spy(new InsertOnlyDimensionHandler(this.createDimension(0, false), this.createFactFeed(), this
				.createCacheHandler(), 1, this.createConfig()));
		doReturn(this.createDbHandler()).when(dh1).getDbHandler();
		final String[] parsedLine1 = { "a", "b", "c", "d", "e", "f", "g", "h" };
		final Map<String, String> headerValues = new HashMap<String, String>();
		headerValues.put("h1", "100");
		headerValues.put("h2", "200");
		headerValues.put("nk_0", "nk_0v");
		headerValues.put("nk_4", "nk_4v");
		headerValues.put("nk_2", "nk_2v");
		final Object key1 = dh1.getLastLineBulkLoadValue(parsedLine1, headerValues);
		Assert.assertNull(headerValues.get("dim1.sk"));
		Assert.assertEquals("100", "" + key1);
		Assert.assertNull(lastRequiredFromCache);
		Assert.assertEquals("insert into dim where nk_0=nk_0v and nk_4=nk_4v and nk_2=nk_2v and a='b' and nk_100=NULL or p='100' or p1='h2'",
				lastStatementToExecute);
	}

	private CacheInstance createCacheHandler() {

		return new CacheInstance() {

			@Override
			public Integer getSurrogateKey(final String naturalKey) {
				lastRequiredFromCache = naturalKey;
				return null;
			}

			@Override
			public void put(final String naturalKey, final Integer surrogateKey) {

			}

			@Override
			public void putAll(final Map<String, Integer> values) {

			}

			@Override
			public void clear() {

			}
		};
	}

	private BaukConfiguration createConfig() {
		final BaukConfiguration bc = Mockito.mock(BaukConfiguration.class);
		when(bc.getDatabaseStringLiteral()).thenReturn("'");
		when(bc.getDatabaseStringEscapeLiteral()).thenReturn("''");
		return bc;
	}

	private DbHandler createDbHandler() {
		return new DbHandler() {

			@Override
			public Long executeQueryStatementAndReturnKey(final String statement, final String dimensionName) {
				lastStatementToExecute = statement;
				return 100l;
			}

			@Override
			public Long executeInsertStatementAndReturnKey(final String statement, final String description) {
				lastStatementToExecute = statement;
				return 100l;
			}

			@Override
			public void executeInsertOrUpdateStatement(final String statement, final String description) {

			}

			@Override
			public List<DimensionKeysPair> queryForDimensionKeys(final String dimName, final String statement, final int numberOfNaturalKeyColumns) {
				return null;
			}

			@Override
			public Map<String, String> executeSelectStatement(final String statement, final String description) {
				return null;
			}
		};
	}

	private Dimension createDimension(final int naturalKeysCount, final boolean exposeLastLine) {
		final Dimension dim = new Dimension();
		dim.setName("dim1");
		dim.setExposeLastLineValueInContext(exposeLastLine);
		final ArrayList<MappedColumn> naturalKeys = new ArrayList<MappedColumn>();
		for (int i = 0; i < naturalKeysCount; i++) {
			final String nkName = "nk_" + i;
			final MappedColumn nk = new MappedColumn();
			nk.setName(nkName);
			nk.setNaturalKey(true);
			naturalKeys.add(nk);
		}
		dim.setMappedColumns(naturalKeys);
		final SqlStatements sqlStatements = new SqlStatements();
		sqlStatements
				.setInsertSingle("insert into dim where nk_0=${nk_0} and nk_4=${nk_4} and nk_2=${nk_2} and a='b' and nk_100=${nk_100} or p='${h1}' or p1='h2'");
		sqlStatements.setSelectRecordIdentifier("select 1 from 2");
		dim.setSqlStatements(sqlStatements);
		return dim;
	}

	private Dimension createDimensionNaturalAndMapped(final int naturalKeysCount) {
		final Dimension dim = new Dimension();
		dim.setName("dim1");
		final ArrayList<MappedColumn> naturalKeys = new ArrayList<MappedColumn>();
		for (int i = 0; i < naturalKeysCount; i++) {
			final String nkName = "nk_" + i;
			final MappedColumn nk = new MappedColumn();
			nk.setName(nkName);
			nk.setNaturalKey(true);
			naturalKeys.add(nk);
		}
		final MappedColumn m1 = new MappedColumn();
		m1.setName("mapped1");
		naturalKeys.add(m1);
		final MappedColumn m2 = new MappedColumn();
		m2.setName("mapped2");
		naturalKeys.add(m2);
		dim.setMappedColumns(naturalKeys);
		final SqlStatements sqlStatements = new SqlStatements();
		sqlStatements
				.setInsertSingle("insert into dim where nk_0=${nk_0} and nk_4=${nk_4} and nk_2=${nk_2} and a='b' and nk_100=${nk_100} or p='${h1}' or p1='h2' and mapped1='${mapped1}' or mapped2>'${mapped2}'");
		dim.setSqlStatements(sqlStatements);
		return dim;
	}

	private Dimension createDimension() {
		return this.createDimension(false);
	}

	private Dimension createDimension(final boolean exposeLastLine) {
		return this.createDimension(5, exposeLastLine);
	}

	private FactFeed createFactFeed() {
		final FactFeed ff = new FactFeed();
		ff.setName("ff1");
		final Data data = new Data();
		final ArrayList<BaukAttribute> attributes = new ArrayList<BaukAttribute>();

		final BaukAttribute atr = new BaukAttribute();
		atr.setName("nk_4");
		attributes.add(atr);

		final BaukAttribute atr1 = new BaukAttribute();
		atr1.setName("x_4");
		attributes.add(atr1);

		final BaukAttribute atr2 = new BaukAttribute();
		atr2.setName("nk_0");
		attributes.add(atr2);

		final BaukAttribute atr3 = new BaukAttribute();
		atr3.setName("nk_1");
		attributes.add(atr3);

		final BaukAttribute atr4 = new BaukAttribute();
		atr4.setName("yyy");
		attributes.add(atr4);

		final BaukAttribute atr5 = new BaukAttribute();
		atr5.setName("nk_3");
		attributes.add(atr5);

		final BaukAttribute atr6 = new BaukAttribute();
		atr6.setName("nk_2");
		attributes.add(atr6);

		final BaukAttribute mapped1 = new BaukAttribute();
		mapped1.setName("mapped1");
		attributes.add(mapped1);

		final BaukAttribute mapped3 = new BaukAttribute();
		mapped3.setName("mapped3");
		attributes.add(mapped3);

		final BaukAttribute mapped2 = new BaukAttribute();
		mapped2.setName("mapped2");
		attributes.add(mapped2);

		data.setAttributes(attributes);
		ff.setData(data);
		return ff;
	}

}
