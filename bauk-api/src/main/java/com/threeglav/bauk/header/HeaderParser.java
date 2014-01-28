package com.threeglav.bauk.header;

import java.util.Map;

/**
 * Interface for header parser.
 * 
 * @author Borisa
 * 
 */
public interface HeaderParser {

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
	void init(String configuredHeaderStartsWithString, String configuredDelimiter, Map<String, String> engineConfigurationProperties);

	/**
	 * Method to parse header line and returning all parsed, individual header attributes, used later.
	 * 
	 * @param headerLine
	 *            full, unparsed header line. Might start with control character.
	 * @param declaredHeaderAttributeNames
	 *            all declared header attributes (as in configuration)
	 * @param globalAttributes
	 *            global attributes available before header parsing (engine specific or attributes derived from file
	 *            name)
	 * 
	 * @return map of all parsed header attributes. This map is later passed as-is and used later in processing. Must
	 *         not return null.
	 */
	public Map<String, String> parseHeader(String headerLine, String[] declaredHeaderAttributeNames, Map<String, String> globalAttributes);

}