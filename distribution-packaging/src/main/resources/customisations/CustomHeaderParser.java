import com.threeglav.bauk.header.HeaderParser;

import java.util.Map;
import java.util.HashMap;

public class CustomHeaderParser implements HeaderParser {

	/**
	 * Method to parse header line and returning all parsed, individual header attributes, used later.
	 * 
	 * @param headerLine
	 *            full, unparsed header line. Might start with control character.
	 * @param declaredHeaderAttributeNames
	 *            all declared header attributes (as in configuration)
	 * @param configuredHeaderStartsWithString
	 *            string that header line should start with (as in configuration)
	 * @param configuredDelimiter
	 *            delimiter string (as in configuration)
	 * @return map of all parsed header attributes. This map is later passed as-is and used later in processing. Must
	 *         not return null.
	 */
	public Map<String, String> parseHeader(String headerLine, String[] declaredHeaderAttributeNames, String configuredHeaderStartsWithString, String configuredDelimiter){
			Map<String, String> headerValues = new HashMap();
			header.values.put("abc", "someCustomValue");
			return headerValues;		
	}
}