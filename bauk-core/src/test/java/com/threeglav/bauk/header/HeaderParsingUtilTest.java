package com.threeglav.bauk.header;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.bauk.model.Attribute;

public class HeaderParsingUtilTest {

	@Test
	public void testNull() {
		try {
			HeaderParsingUtil.getAttributeNames(null);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			HeaderParsingUtil.getAttributeNamesAndPositions(null);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testGetNames() {
		final String[] names = HeaderParsingUtil.getAttributeNames(this.createAttributes(3));
		Assert.assertEquals(3, names.length);
		Assert.assertEquals("a_0", names[0]);
		Assert.assertEquals("a_1", names[1]);
		Assert.assertEquals("a_2", names[2]);
		final String[] names1 = HeaderParsingUtil.getAttributeNames(this.createAttributes(0));
		Assert.assertEquals(0, names1.length);
		final String[] names2 = HeaderParsingUtil.getAttributeNames(this.createAttributes(1));
		Assert.assertEquals(1, names2.length);
		Assert.assertEquals("a_0", names2[0]);
	}

	@Test
	public void testGetNamesAndPositions() {
		final Map<String, Integer> np = HeaderParsingUtil.getAttributeNamesAndPositions(this.createAttributes(3));
		Assert.assertEquals(3, np.size());
		Assert.assertEquals(new Integer(0), np.get("a_0"));
		Assert.assertEquals(new Integer(1), np.get("a_1"));
		Assert.assertEquals(new Integer(2), np.get("a_2"));
	}

	private ArrayList<Attribute> createAttributes(final int count) {
		final ArrayList<Attribute> attrs = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			final Attribute a = new Attribute();
			a.setName("a_" + i);
			attrs.add(a);
		}
		return attrs;
	}

}
