package com.threeglav.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.codahale.metrics.Histogram;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.bauk.header.DefaultHeaderParser;
import com.threeglav.bauk.header.HeaderParser;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.HeaderFooter;
import com.threeglav.bauk.model.HeaderFooterProcessType;
import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.FullFeedParser;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class TextFileReaderComponent extends ConfigAware {

	private final int bufferSize;
	private HeaderParser headerParser;
	private final boolean processAndValidateFooter;
	private final Histogram feedFileSizeHistogram;
	private final FeedParser footerLineParser;
	private final String footerFirstString;
	private final String[] declaredHeaderAttributes;
	private final boolean shouldProcessHeader;
	private final FeedDataProcessor feedDataProcessor;

	public TextFileReaderComponent(final FactFeed factFeed, final BaukConfiguration config, final FeedDataProcessor feedDataProcessor,
			final String routeIdentifier) {
		super(factFeed, config);
		this.validate();
		this.feedDataProcessor = feedDataProcessor;
		processAndValidateFooter = factFeed.getFooter().getProcess() != HeaderFooterProcessType.SKIP;
		feedFileSizeHistogram = MetricsUtil.createHistogram("(" + routeIdentifier + ") - number of lines in feed");
		footerLineParser = new FullFeedParser(this.getFactFeed().getDelimiterString());
		footerFirstString = this.getFactFeed().getFooter().getEachLineStartsWithCharacter();
		shouldProcessHeader = this.checkProcessHeader();
		if (shouldProcessHeader) {
			log.debug("Extracting header attributes for {}", factFeed.getName());
			declaredHeaderAttributes = AttributeParsingUtil.getAttributeNames(this.getFactFeed().getHeader().getAttributes());
			this.initializeHeaderProcessor();
		} else {
			declaredHeaderAttributes = null;
		}
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
		log.debug("Read buffer size is {}", bufferSize);
	}

	private boolean checkProcessHeader() {
		final HeaderFooter header = this.getFactFeed().getHeader();
		final HeaderFooterProcessType headerProcessingType = header.getProcess();
		final String feedName = this.getFactFeed().getName();
		log.debug("For feed {} header processing set to {}", feedName, headerProcessingType);
		if (headerProcessingType == HeaderFooterProcessType.NO_HEADER || headerProcessingType == HeaderFooterProcessType.SKIP) {
			log.debug("Will skip header processing for {}", feedName);
			return false;
		}
		log.debug("Not skipping header processing for {}", feedName);
		return true;
	}

	private void initializeHeaderProcessor() {
		String headerParserClassName = DefaultHeaderParser.class.getName();
		final String configuredHeaderParserClass = this.getFactFeed().getHeader().getHeaderParserClassName();
		if (!StringUtil.isEmpty(configuredHeaderParserClass)) {
			headerParserClassName = configuredHeaderParserClass;
			log.debug("Will try to use custom header parser class {}", configuredHeaderParserClass);
		} else {
			log.debug("Will use default header parser class {}", headerParserClassName);
		}
		final CustomProcessorResolver<HeaderParser> headerParserInstanceResolver = new CustomProcessorResolver<>(headerParserClassName,
				HeaderParser.class);
		headerParser = headerParserInstanceResolver.resolveInstance();
	}

	private void validate() {
		if (this.getFactFeed().getHeader() == null) {
			throw new IllegalArgumentException("Header configuration must not be null. Check your configuration file!");
		}
		if (this.getFactFeed().getFooter() == null) {
			throw new IllegalArgumentException("Footer configuration must not be null. Check your configuration file!");
		}
		final HeaderFooterProcessType footerProcessing = this.getFactFeed().getFooter().getProcess();
		if (footerProcessing != HeaderFooterProcessType.SKIP && footerProcessing != HeaderFooterProcessType.STRICT) {
			throw new IllegalArgumentException("Footer processing type " + footerProcessing + " not allowed. Only skip or strict are allowed!");
		}
		if (footerProcessing == HeaderFooterProcessType.STRICT && StringUtil.isEmpty(this.getFactFeed().getFooter().getEachLineStartsWithCharacter())) {
			throw new IllegalStateException(
					"Footer is set to strict processing but can not find first character in configuration! Check your configuration file!");
		}
	}

	public void readFile(final InputStream fileInputStream, final Map<String, String> globalAttributes) {
		final BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream), bufferSize);
		try {
			int feedLinesNumber = 0;
			String line = br.readLine();
			boolean processedHeader = false;
			String footerLine = null;
			Map<String, String> headerAttributes = null;
			while (line != null) {
				if (!processedHeader) {
					feedDataProcessor.startFeed(globalAttributes, headerAttributes);
					processedHeader = true;
					if (shouldProcessHeader) {
						headerAttributes = this.processHeader(line);
					} else {
						feedDataProcessor.processLine(line);
					}
					line = br.readLine();
				} else {
					final String nextLine = br.readLine();
					if (nextLine == null) {
						if (processAndValidateFooter) {
							footerLine = line;
						} else {
							feedDataProcessor.processLine(line);
							footerLine = null;
						}
						line = null;
					} else {
						feedDataProcessor.processLine(line);
						feedLinesNumber++;
						line = nextLine;
					}
				}
			}
			log.debug("Successfully processed {} lines in file", feedLinesNumber);
			if (feedFileSizeHistogram != null) {
				feedFileSizeHistogram.update(feedLinesNumber);
			}
			feedDataProcessor.closeFeed(feedLinesNumber);
			this.processFooter(feedLinesNumber, footerLine);
			IOUtils.closeQuietly(br);
		} catch (final IOException ie) {
			log.error("IOException {}", ie.getMessage());
		} finally {
			IOUtils.closeQuietly(fileInputStream);
		}
	}

	private void processFooter(final int feedLinesNumber, final String footerLine) {
		if (processAndValidateFooter) {
			log.debug("Validating footer. Processed in total {} lines, comparing with values in footer line {}", feedLinesNumber, footerLine);
			final String[] footerParsedValues = footerLineParser.parse(footerLine);
			if (footerParsedValues.length > 2) {
				throw new IllegalStateException("Found " + footerParsedValues.length + " values in footer. Expected at most 2!");
			}
			if (!footerParsedValues[0].equals(footerFirstString)) {
				throw new IllegalStateException("First character of footer line " + footerParsedValues[0]
						+ " does not match the one given in configuration file " + footerFirstString);
			}
			try {
				final Integer footerIntValue = Integer.parseInt(footerParsedValues[1]);
				if (feedLinesNumber != footerIntValue) {
					throw new IllegalStateException("Footer value " + footerIntValue + " does not match with total number of processed lines "
							+ feedLinesNumber);
				}
			} catch (final NumberFormatException nfe) {
				throw new IllegalStateException("Footer value [" + footerParsedValues[1] + "] can not be converted to integer value!");
			}
		}
	}

	private Map<String, String> processHeader(final String line) {
		final HeaderFooter header = this.getFactFeed().getHeader();
		final String feedName = this.getFactFeed().getName();
		final Map<String, String> parsedHeaderValues = headerParser.parseHeader(line, declaredHeaderAttributes,
				header.getEachLineStartsWithCharacter(), this.getFactFeed().getDelimiterString());
		log.debug("Parsed header values for {} are {}", feedName, parsedHeaderValues);
		return parsedHeaderValues;
	}

	public void process(final InputStream input, final Map<String, String> globalAttributes) {
		this.readFile(input, globalAttributes);
	}

}
