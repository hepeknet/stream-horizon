import java.util.Map;

import com.threeglav.sh.bauk.header.HeaderParser;

/**
 * Example how to write custom header parser
 * 
 */
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
	 * @param engineConfigurationProperties
	 *            configuration properties supplied to engine at startup
	 */
	@Override
	public void init(final String configuredHeaderStartsWithString, final String configuredDelimiter, final Map<String, String> engineConfigProperties) {
		this.configuredHeaderStartsWithString = configuredHeaderStartsWithString;
		this.configuredDelimiter = configuredDelimiter;
		// do initialize some other expensive resources
	}

	/**
	 * Method to parse header line and returning all parsed, individual header attributes, used later.
	 * 
	 * @param headerLine
	 *            full, unparsed header line. Might start with control character.
	 * @param declaredHeaderAttributeNames
	 *            all declared header attributes (as in configuration) * @param globalAttributes global attributes
	 *            available before header parsing (engine specific or attributes derived from file name)
	 * 
	 * @return map of all parsed header attributes. This map is later passed as-is and used later in processing. Must
	 *         not return null.
	 */
	@Override
	public Map<String, String> parseHeader(final String headerLine, final String[] declaredHeaderAttributeNames,
			final Map<String, String> globalAttributes) {
		if (!headerLine.startsWith(configuredHeaderStartsWithString)) {
			// do something
		}
		final String[] dataFromHeader = headerLine.split(configuredDelimiter);
		globalAttributes.put("abc", "someCustomValue");
		// assume we know what is at position 2
		globalAttributes.put("customAttribute", dataFromHeader[2]);
		return globalAttributes;
	}
}