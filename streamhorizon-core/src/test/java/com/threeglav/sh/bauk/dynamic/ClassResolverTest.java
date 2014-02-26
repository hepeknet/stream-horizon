package com.threeglav.sh.bauk.dynamic;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

public class ClassResolverTest {

	@Test
	public void testNull() {
		try {
			new ClassResolver(null, this.getClass());
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			new ClassResolver(" ", this.getClass());
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
		try {
			new ClassResolver("abc", null);
			fail("nok");
		} catch (final IllegalArgumentException iae) {
			Assert.assertTrue(true);
		}
	}

	@Test
	public void testSimpleClasspath() {
		final ClassResolver cr = new ClassResolver("com.threeglav.sh.bauk.dynamic.CustomTestPojo", CustomTestPojo.class);
		final CustomTestPojo inst1 = (CustomTestPojo) cr.createInstanceFromClasspath();
		Assert.assertNotNull(inst1);
		Assert.assertEquals("a", inst1.getName());
		final CustomTestPojo inst2 = (CustomTestPojo) cr.createInstanceFromClasspath();
		Assert.assertNotNull(inst2);
		Assert.assertEquals("a", inst2.getName());
		Assert.assertFalse(inst1 == inst2);
	}

	@Test
	public void testSimpleClasspathInheritance() {
		final ClassResolver cr = new ClassResolver("com.threeglav.sh.bauk.dynamic.CustomTestSubPojo", CustomTestPojo.class);
		final CustomTestSubPojo inst1 = (CustomTestSubPojo) cr.createInstanceFromClasspath();
		Assert.assertNotNull(inst1);
		Assert.assertEquals("a", inst1.getName());
		try {
			final ClassResolver<CustomTestPojo> cr1 = new ClassResolver<CustomTestPojo>("com.threeglav.sh.bauk.dynamic.ClassResolverTest",
					CustomTestPojo.class);
			cr1.createInstanceFromClasspath();
			fail("nok");
		} catch (final IllegalStateException ise) {

		}
	}

	@Test
	public void testSimpleSource() {
		final ClassResolver<CustomTestPojo> cr = new ClassResolver<CustomTestPojo>("com.threeglav.sh.bauk.dynamic.CustomTestDynamicSubPojo",
				CustomTestPojo.class);
		final String source = "package com.threeglav.sh.bauk.dynamic; import com.threeglav.sh.bauk.dynamic.CustomTestPojo; public class CustomTestDynamicSubPojo extends CustomTestPojo { public String getName(){ return \"b\"; } }";
		final CustomTestPojo inst1 = cr.createInstanceFromSource(source);
		Assert.assertNotNull(inst1);
		Assert.assertEquals("b", inst1.getName());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncompatibleSource() {
		final ClassResolver<CustomTestPojo> cr = new ClassResolver("com.threeglav.sh.bauk.dynamic.Abc", CustomTestPojo.class);
		final String source = "package com.threeglav.sh.bauk.dynamic; import com.threeglav.sh.bauk.dynamic.CustomTestPojo; public class Abc { public String getName(){ return \"b\"; } }";
		cr.createInstanceFromSource(source);
	}

}
