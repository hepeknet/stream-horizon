import com.threeglav.bauk.header.HeaderParser;

import java.util.Map;
import java.util.HashMap;

public class CustomHeaderParser implements HeaderParser {
	
	private String configuredHeaderStartsWithString;
	private String configuredDelimiter;
	
	/**
	 * Invoked after parsers has been created. Values provided here will most probably be needed to parse header line.
	 * This method is invoked only once and before any processing is done.
	 * 
	 * @param configuredHeaderStartsWithString
	 *            read from configuration file. String with which header line should start with
	 * @param configuredDelimiter
	 *            read from configuration file. Delimiter string used to split header values
	 */
	@Override
	public void init(final String configuredHeaderStartsWithString, final String configuredDelimiter){
		this.configuredHeaderStartsWithString = configuredHeaderStartsWithString;
		this.configuredDelimiter = configuredDelimiter;
		// do init some other expensive resources
	}

	/**
	 * Method to parse header line and returning all parsed, individual header attributes, used later.
	 * 
	 * @param headerLine
	 *            full, unparsed header line. Might start with control character.
	 * @param declaredHeaderAttributeNames
	 *            all declared header attributes (as in configuration)
	 * @return map of all parsed header attributes. This map is later passed as-is and used later in processing. Must
	 *         not return null.
	 */
	@Override
	public Map<String, String> parseHeader(final String headerLine, final String[] declaredHeaderAttributeNames){
			Map<String, String> headerValues = new HashMap();
			header.values.put("abc", "someCustomValue");
			return headerValues;		
	}
}