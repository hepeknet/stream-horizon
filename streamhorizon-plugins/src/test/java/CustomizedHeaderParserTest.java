import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CustomizedHeaderParserTest {

	@Test
	public void testCustomizations() {
		final CustomizedHeaderParser chp = new CustomizedHeaderParser();
		chp.init("0", ",", new HashMap<String, String>());
		final String line = "0,one,two,three,four,five,six,seven,eight,11/12/2001 10:12:13,ten";
		final String[] attributes = new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight", "date", "ten" };
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
		Assert.assertEquals("1", parsedAttributes.get("customized.feedProcessingThreadID.modulo"));
	}

	@Test
	public void testCustomizationsReconfigured() {
		final CustomizedHeaderParser chp = new CustomizedHeaderParser();
		final Map<String, String> engineConfigProperties = new HashMap<>();
		engineConfigProperties.put(CustomizedHeaderParser.CUSTOMIZED_FEED_PROCESSSING_THREAD_MODULO_PARAM_NAME, "1");
		engineConfigProperties.put(CustomizedHeaderParser.CUSTOMIZED_DATE_POSITION_IN_HEADER_PARAM_NAME, "9");
		chp.init("0", ",", engineConfigProperties);
		final String line = "0,one,two,three,four,five,six,seven,eight,11/12/2001 10:12:13,eleven";
		final String[] attributes = new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight", "date", "eleven" };
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
		Assert.assertEquals("0", parsedAttributes.get("customized.feedProcessingThreadID.modulo"));
	}

	private static final Map<String, String> createGlobalAttributes() {
		final Map<String, String> attrs = new HashMap<>();
		attrs.put("feedProcessingThreadID", "101");
		return attrs;
	}

}
