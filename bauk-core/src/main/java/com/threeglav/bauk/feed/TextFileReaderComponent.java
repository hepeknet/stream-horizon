package com.threeglav.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.codahale.metrics.Histogram;
import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.SystemConfigurationConstants;
import com.threeglav.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.bauk.header.DefaultHeaderParser;
import com.threeglav.bauk.header.HeaderParser;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
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
	private final FeedDataProcessor feedDataProcessor;
	private final HeaderFooterProcessType headerProcessingType;
	private final boolean isControlFeed;

	public TextFileReaderComponent(final FactFeed factFeed, final BaukConfiguration config, final FeedDataProcessor feedDataProcessor,
			final String routeIdentifier) {
		super(factFeed, config);
		this.validate();
		this.feedDataProcessor = feedDataProcessor;
		processAndValidateFooter = factFeed.getFooter().getProcess() != HeaderFooterProcessType.SKIP;
		feedFileSizeHistogram = MetricsUtil.createHistogram("(" + routeIdentifier + ") - number of lines in feed");
		footerLineParser = new FullFeedParser(this.getFactFeed().getDelimiterString());
		footerFirstString = this.getFactFeed().getFooter().getEachLineStartsWithCharacter();
		headerProcessingType = this.getFactFeed().getHeader().getProcess();
		final boolean shouldProcessHeader = this.checkProcessHeader();
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
		isControlFeed = this.getFactFeed().getType() == FactFeedType.CONTROL;
		if (isControlFeed) {
			log.info("Feed {} will be treated as control feed", this.getFactFeed().getName());
		}
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

	public int readFile(final InputStream fileInputStream, final Map<String, String> globalAttributes) {
		final BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream), bufferSize);
		try {
			int feedLinesNumber = 0;
			String line = br.readLine();
			boolean processedHeader = false;
			String footerLine = null;
			final boolean headerExists = headerProcessingType != HeaderFooterProcessType.NO_HEADER;
			final boolean skipHeader = headerProcessingType == HeaderFooterProcessType.SKIP;
			log.debug("For {} header process={}", this.getFactFeed().getName(), headerProcessingType);
			while (line != null) {
				if (!processedHeader) {
					feedDataProcessor.startFeed(globalAttributes);
					processedHeader = true;
					if (headerExists) {
						if (!skipHeader) {
							final Map<String, String> headerAttributes = this.processHeader(line);
							if (headerAttributes != null) {
								globalAttributes.putAll(headerAttributes);
							}
						} else {
							log.debug("Skipping header line {}", line);
						}
					} else {
						feedDataProcessor.processLine(line, globalAttributes);
						feedLinesNumber++;
					}
					final String nextLine = br.readLine();
					if (nextLine == null) {
						if (isControlFeed) {
							this.exposeFeedDataAsAttributes(line, globalAttributes);
						}
					}
					line = nextLine;
				} else {
					final String nextLine = br.readLine();
					if (nextLine == null) {
						if (isControlFeed) {
							this.exposeFeedDataAsAttributes(line, globalAttributes);
						}
						if (processAndValidateFooter) {
							footerLine = line;
						} else {
							feedDataProcessor.processLine(line, globalAttributes);
							footerLine = null;
						}
						line = null;
					} else {
						feedDataProcessor.processLine(line, globalAttributes);
						feedLinesNumber++;
						line = nextLine;
					}
				}
			}
			log.debug("Successfully processed {} lines in file", feedLinesNumber);
			if (feedFileSizeHistogram != null) {
				feedFileSizeHistogram.update(feedLinesNumber);
			}
			feedDataProcessor.closeFeed(feedLinesNumber, globalAttributes);
			this.processFooter(feedLinesNumber, footerLine);
			IOUtils.closeQuietly(br);
			return feedLinesNumber;
		} catch (final IOException ie) {
			throw new IllegalStateException(ie);
		} finally {
			IOUtils.closeQuietly(fileInputStream);
		}
	}

	private void exposeFeedDataAsAttributes(final String feedDataLine, final Map<String, String> globalAttributes) {
		final String ffName = this.getFactFeed().getName();
		if (StringUtil.isEmpty(feedDataLine)) {
			throw new IllegalStateException("Last line for control feed " + ffName + " is null or empty!");
		}
		log.debug("Exposing last line {} as global attributes for control feed {}", feedDataLine, ffName);
		final FeedParser feedParser = new FullFeedParser(this.getFactFeed().getDelimiterString());
		final String[] splitLine = feedParser.parse(feedDataLine);
		final ArrayList<Attribute> ffDataAttrs = this.getFactFeed().getData().getAttributes();
		if (splitLine.length != ffDataAttrs.size()) {
			log.error("Control feed " + ffName + " declared " + ffDataAttrs.size() + " data attributes but last line only has " + splitLine.length
					+ " values!");
			throw new IllegalStateException("Control feed " + ffName + " declared " + ffDataAttrs.size() + " data attributes but last line only has "
					+ splitLine.length + " values!");
		}
		for (int i = 0; i < ffDataAttrs.size(); i++) {
			final Attribute attr = ffDataAttrs.get(i);
			final String attrName = attr.getName();
			globalAttributes.put(attrName, splitLine[i]);
			log.debug("Added {}={} to global attributes for control feed {}", attrName, splitLine[i], ffName);
		}
		log.trace("After adding control attributes {}", globalAttributes);
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

	public int process(final InputStream input, final Map<String, String> globalAttributes) {
		return this.readFile(input, globalAttributes);
	}

}
