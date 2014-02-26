package com.threeglav.sh.bauk.util;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.threeglav.sh.bauk.util.StatefulAttributeReplacer;

public class StatefulAttributeReplacerTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {
		new StatefulAttributeReplacer(null, null, null);
	}

	@Test
	public void testEmpty() {
		final StatefulAttributeReplacer sar = new StatefulAttributeReplacer("select 1 2 ${ abc", "'", "''");
		Assert.assertFalse(sar.isHasReplacementToDo());
		Assert.assertNull(sar.getAttributeNamePlaceholdersToReplace());
		Assert.assertNull(sar.getAttributeNamesToReplace());
	}

	@Test
	public void testSimple() {
		final StatefulAttributeReplacer sar = new StatefulAttributeReplacer("select ${a} from ${b} where ${a}", "'", "''");
		Assert.assertTrue(sar.isHasReplacementToDo());
		Assert.assertEquals(2, sar.getAttributeNamePlaceholdersToReplace().length);
		Assert.assertEquals(2, sar.getAttributeNamesToReplace().length);
		Assert.assertEquals("a", sar.getAttributeNamesToReplace()[0]);
		Assert.assertEquals("b", sar.getAttributeNamesToReplace()[1]);
		Assert.assertEquals("${a}", sar.getAttributeNamePlaceholdersToReplace()[0]);
		Assert.assertEquals("${b}", sar.getAttributeNamePlaceholdersToReplace()[1]);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		globalAttrs.put("a", "1");
		globalAttrs.put("b", "2");
		final String replaced = sar.replaceAttributes(globalAttrs);
		Assert.assertEquals("select 1 from 2 where 1", replaced);
	}

	@Test
	public void testSimple1() {
		final StatefulAttributeReplacer sar = new StatefulAttributeReplacer("select ${a}+${a} from ${c} where ${a}=${b}", "'", "''");
		Assert.assertTrue(sar.isHasReplacementToDo());
		Assert.assertEquals(3, sar.getAttributeNamePlaceholdersToReplace().length);
		Assert.assertEquals(3, sar.getAttributeNamesToReplace().length);
		Assert.assertEquals("a", sar.getAttributeNamesToReplace()[0]);
		Assert.assertEquals("b", sar.getAttributeNamesToReplace()[1]);
		Assert.assertEquals("c", sar.getAttributeNamesToReplace()[2]);
		Assert.assertEquals("${a}", sar.getAttributeNamePlaceholdersToReplace()[0]);
		Assert.assertEquals("${b}", sar.getAttributeNamePlaceholdersToReplace()[1]);
		Assert.assertEquals("${c}", sar.getAttributeNamePlaceholdersToReplace()[2]);
		final Map<String, String> globalAttrs = new HashMap<String, String>();
		globalAttrs.put("a", "1");
		globalAttrs.put("b", "3");
		globalAttrs.put("c", "'2");
		final String replaced = sar.replaceAttributes(globalAttrs);
		Assert.assertEquals("select 1+1 from ''2 where 1=3", replaced);
	}

}
