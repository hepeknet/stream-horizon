import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CustomizedHeaderParserTest {

	@Test
	public void testCustomizations() {
		final CustomizedHeaderParser chp = new CustomizedHeaderParser();
		chp.init("0", ",", new HashMap<String, String>());
		final String line = "0,one,two,three,four,five,six,seven,eight,nine,11/12/2001 10:12:13,eleven";
		final String[] attributes = new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "date", "eleven" };
		final Map<String, String> parsedAttributes = chp.parseHeader(line, attributes, createGlobalAttributes());
		Assert.assertNotNull(parsedAttributes);
		System.out.println(parsedAttributes);
		for (final String attrName : attributes) {
			if (!"date".equals(attrName)) {
				Assert.assertEquals(attrName, parsedAttributes.get(attrName));
			} else {
				Assert.assertEquals("11/12/2001 10:12:13", parsedAttributes.get(attrName));
			}
		}
		Assert.assertEquals(attributes.length + 2, parsedAttributes.size());
		Assert.assertEquals("20011211", parsedAttributes.get("customized.formattedHeaderDate"));
		Assert.assertEquals("9", parsedAttributes.get("customized.feedProcessingThreadID.modulo"));
	}

	private static final Map<String, String> createGlobalAttributes() {
		final Map<String, String> attrs = new HashMap<>();
		attrs.put("feedProcessingThreadID", "19");
		return attrs;
	}

}
