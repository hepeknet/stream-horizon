import gnu.trove.map.hash.THashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.threeglav.bauk.header.HeaderParser;
import com.threeglav.bauk.parser.FullFeedParser;

public class CustomizedHeaderParser implements HeaderParser {

	/*
	 * zero based counter - pointing to position where header date is, including first (control) character
	 */
	private static final int DATE_POSITION_IN_HEADER = 10;
	private static final String INPUT_DATE_FORMAT = "dd/MM/yyyy HH:mm:ss";
	private static final String OUTPUT_DATE_FORMAT = "yyyyMMdd";
	/*
	 * what divisor value to use when calculating module for feedProcessingThreadID
	 */
	private static final int FEED_PROCESSING_THREAD_ID_MODULO = 10;

	/*
	 * under what attribute name we can find feed processing thread id
	 */
	private static final String FEED_PROCESSING_THREAD_ID_PARAM_NAME = "feedProcessingThreadID";

	/*
	 * we expose two custom attributes - which can be accessed by these two names
	 */
	private static final String FORMATTED_DATE_IN_HEADER_ATTRIBUTE_NAME = "customized.formattedHeaderDate";
	private static final String FEED_PROCESSING_THREAD_ID_MODULO_ATTRIBUTE_NAME = "customized.feedProcessingThreadID.modulo";

	private static final DateTimeFormatter INPUT_DATE_FORMATTER = DateTimeFormat.forPattern(INPUT_DATE_FORMAT);
	private static final DateTimeFormatter OUTPUT_DATE_FORMATTER = DateTimeFormat.forPattern(OUTPUT_DATE_FORMAT);

	private final Logger log = LoggerFactory.getLogger(this.getClass());
	private String startsWithString;
	private FullFeedParser fullFeedParser;
	private final boolean isDebugEnabled;

	public CustomizedHeaderParser() {
		isDebugEnabled = log.isDebugEnabled();
		log.info("Will generate two custom attributes {} and {}", FORMATTED_DATE_IN_HEADER_ATTRIBUTE_NAME,
				FEED_PROCESSING_THREAD_ID_MODULO_ATTRIBUTE_NAME);
	}

	@Override
	public void init(final String configuredHeaderStartsWithString, final String configuredDelimiter,
			final Map<String, String> engineConfigurationProperties) {
		startsWithString = configuredHeaderStartsWithString;
		fullFeedParser = new FullFeedParser(configuredDelimiter);
		if (startsWithString == null) {
			throw new IllegalStateException("Expected character for header not set");
		}
		if (isDebugEnabled) {
			log.debug("Expected character is header line should start with is [{}]", startsWithString);
		}
	}

	@Override
	public Map<String, String> parseHeader(final String headerLine, final String[] declaredHeaderAttributeNames,
			final Map<String, String> globalAttributes) {
		if (headerLine == null) {
			return new HashMap<String, String>();
		}
		final Map<String, String> headerValues = new THashMap<String, String>();
		final String[] parsed = fullFeedParser.parse(headerLine);
		if (parsed == null || parsed.length == 0) {
			throw new IllegalArgumentException("Header is null or has zero length");
		}
		if (!startsWithString.equals(parsed[0])) {
			throw new IllegalStateException("First header character [" + parsed[0] + "] does not match expected character [" + startsWithString + "]");
		}
		final int declaredAttributesSize = declaredHeaderAttributeNames.length;
		final int parsedLength = parsed.length - 1;
		if (parsedLength != declaredAttributesSize) {
			throw new IllegalStateException("Number of defined header attributes " + declaredAttributesSize
					+ " is different than number of parsed items " + parsedLength + " from " + Arrays.toString(parsed));
		}
		for (int i = 1; i < parsed.length; i++) {
			headerValues.put(declaredHeaderAttributeNames[i - 1], parsed[i]);
		}
		try {
			this.addCustomAttributes(parsed, globalAttributes, headerValues);
		} catch (final Exception exc) {
			log.error("Exception while adding custom attributes!", exc);
		}
		return headerValues;
	}

	private void addCustomAttributes(final String[] parsed, final Map<String, String> globalAttributes, final Map<String, String> headerValues) {
		// find and convert date
		final String inputDateValue = parsed[DATE_POSITION_IN_HEADER];
		final long headerDateInMillis = INPUT_DATE_FORMATTER.parseMillis(inputDateValue);
		final String formattedOutputDate = OUTPUT_DATE_FORMATTER.print(headerDateInMillis);
		headerValues.put(FORMATTED_DATE_IN_HEADER_ATTRIBUTE_NAME, formattedOutputDate);
		if (isDebugEnabled) {
			log.debug("Added customized header attribute {}={}", FORMATTED_DATE_IN_HEADER_ATTRIBUTE_NAME, formattedOutputDate);
		}

		// calculate modulo
		final String feedProcessingThreadId = globalAttributes.get(FEED_PROCESSING_THREAD_ID_PARAM_NAME);
		if (feedProcessingThreadId != null) {
			final int feedProcessingThread = Integer.parseInt(feedProcessingThreadId);
			final int feedProcessingThreadModuloValue = feedProcessingThread % FEED_PROCESSING_THREAD_ID_MODULO;
			headerValues.put(FEED_PROCESSING_THREAD_ID_MODULO_ATTRIBUTE_NAME, String.valueOf(feedProcessingThreadModuloValue));
			if (isDebugEnabled) {
				log.debug("Adding customized header attribute {}={}", FEED_PROCESSING_THREAD_ID_MODULO_ATTRIBUTE_NAME,
						feedProcessingThreadModuloValue);
			}
		}
	}

}
