package com.threeglav.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
import com.threeglav.bauk.model.FooterProcessingType;
import com.threeglav.bauk.model.Header;
import com.threeglav.bauk.model.HeaderProcessingType;
import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.FullFeedParser;
import com.threeglav.bauk.util.AttributeParsingUtil;
import com.threeglav.bauk.util.BaukUtil;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class TextFileReaderComponent extends ConfigAware {

	private static final String AVERAGE_NUMBER_OUTPUT_FORMAT = "%.1f";

	private final int bufferSize;
	private HeaderParser headerParser;
	private final boolean processAndValidateFooter;
	private final Histogram feedFileSizeHistogram;
	private final FeedParser footerLineParser;
	private final String footerFirstString;
	private final String[] declaredHeaderAttributes;
	private final FeedDataProcessor feedDataProcessor;
	private final HeaderProcessingType headerProcessingType;
	private final boolean isControlFeed;
	private final boolean headerShouldExist;
	private final boolean skipHeader;
	public static final AtomicLong TOTAL_INPUT_FILES_PROCESSED = new AtomicLong(0);
	public static final AtomicLong TOTAL_ROWS_PROCESSED = new AtomicLong(0);
	private final boolean metricsOff;
	private final boolean outputProcessingStatistics;

	public TextFileReaderComponent(final FactFeed factFeed, final BaukConfiguration config, final FeedDataProcessor feedDataProcessor,
			final String routeIdentifier) {
		super(factFeed, config);
		this.validate();
		this.feedDataProcessor = feedDataProcessor;
		processAndValidateFooter = factFeed.getFooter().getProcess() != FooterProcessingType.SKIP;
		feedFileSizeHistogram = MetricsUtil.createHistogram("(" + routeIdentifier + ") - number of lines per feed");
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
		bufferSize = (int) (ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.READ_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE);
		log.info("Read buffer size is {} bytes", bufferSize);
		isControlFeed = this.getFactFeed().getType() == FactFeedType.CONTROL;
		headerShouldExist = headerProcessingType != HeaderProcessingType.NO_HEADER;
		if (isControlFeed && headerShouldExist) {
			throw new IllegalStateException("Control feed " + factFeed.getName() + " must not have header!");
		}
		if (isControlFeed && processAndValidateFooter) {
			throw new IllegalStateException("Control feed " + factFeed.getName() + " must not have footer!");
		}
		if (isControlFeed) {
			log.info("Feed {} will be treated as control feed", this.getFactFeed().getName());
		}
		skipHeader = headerProcessingType == HeaderProcessingType.SKIP;
		metricsOff = MetricsUtil.isMetricsOff();
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME,
				false);
		if (outputProcessingStatistics) {
			log.info("Will output processing statistics!");
		}
		log.debug("For feed {} footer processing is {}", this.getFactFeed().getName(), factFeed.getFooter().getProcess());
	}

	private boolean checkProcessHeader() {
		final Header header = this.getFactFeed().getHeader();
		final HeaderProcessingType headerProcessingType = header.getProcess();
		final String feedName = this.getFactFeed().getName();
		log.debug("For feed {} header processing set to {}", feedName, headerProcessingType);
		if (headerProcessingType == HeaderProcessingType.NO_HEADER || headerProcessingType == HeaderProcessingType.SKIP) {
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
		final Header header = this.getFactFeed().getHeader();
		final String startsWithString = header.getEachLineStartsWithCharacter();
		final String delimiterString = this.getFactFeed().getDelimiterString();
		headerParser.init(startsWithString, delimiterString);
	}

	private void validate() {
		if (this.getFactFeed().getHeader() == null) {
			throw new IllegalArgumentException("Header configuration must not be null. Check your configuration file!");
		}
		if (this.getFactFeed().getFooter() == null) {
			throw new IllegalArgumentException("Footer configuration must not be null. Check your configuration file!");
		}
		final FooterProcessingType footerProcessing = this.getFactFeed().getFooter().getProcess();
		if (footerProcessing == FooterProcessingType.STRICT && StringUtil.isEmpty(this.getFactFeed().getFooter().getEachLineStartsWithCharacter())) {
			throw new IllegalStateException(
					"Footer is set to strict processing but can not find first character in configuration! Check your configuration file!");
		}
	}

	private void processFirstLine(final LineBuffer lines, final Map<String, String> globalAttributes) {
		if (headerShouldExist) {
			final String line = lines.getLine();
			if (!skipHeader) {
				final Map<String, String> headerAttributes = this.processHeader(line);
				if (headerAttributes != null) {
					globalAttributes.putAll(headerAttributes);
					if (isTraceEnabled) {
						log.trace(
								"Added all attributes {} from header to global attributes. All attributes, before starting to process body for feed {} are {}",
								headerAttributes, this.getFactFeed().getName(), globalAttributes);
					}
				}
			} else if (isDebugEnabled) {
				log.debug("Skipping header line {} for feed {}", line, this.getFactFeed().getName());
			}
		}
	}

	private boolean hasMoreDataLines(final LineBuffer lines) {
		if (processAndValidateFooter) {
			return lines.getSize() > 1;
		} else {
			return lines.getSize() > 0;
		}
	}

	private boolean hasAnyOtherLines(final LineBuffer lines) {
		return lines.getSize() > 1;
	}

	enum FeedProcessingPhase {
		STOP_AND_COUNT, STOP, ONLY_FOOTER_LEFT, PROCESSED_DATA_LINE;
	}

	private FeedProcessingPhase doProcessSingleLine(final LineBuffer lines, final Map<String, String> globalAttributes, final BufferedReader br)
			throws IOException {
		final boolean noMoreLinesAvailable = !this.hasAnyOtherLines(lines);
		if (noMoreLinesAvailable) {
			final String line = lines.getLine();
			this.fillBuffer(lines, br);
			if (isControlFeed) {
				this.exposeControlFeedDataAsAttributes(line, globalAttributes);
				return FeedProcessingPhase.STOP;
			} else if (processAndValidateFooter) {
				lines.add(line);
				return FeedProcessingPhase.ONLY_FOOTER_LEFT;
			} else {
				feedDataProcessor.processLastLine(line, globalAttributes);
				return FeedProcessingPhase.STOP_AND_COUNT;
			}
		} else {
			lines.getSize();
			final String line = lines.getLine();
			this.fillBuffer(lines, br);
			final boolean isLastLine = !this.hasMoreDataLines(lines);
			if (isLastLine) {
				feedDataProcessor.processLastLine(line, globalAttributes);
			} else {
				feedDataProcessor.processLine(line, globalAttributes);
			}
			return FeedProcessingPhase.PROCESSED_DATA_LINE;
		}
	}

	private void fillBuffer(final LineBuffer buffer, final BufferedReader br) throws IOException {
		while (buffer.canAdd()) {
			final String line = br.readLine();
			if (line == null) {
				break;
			}
			buffer.add(line);
		}
	}

	private int readFile(final InputStream fileInputStream, final Map<String, String> globalAttributes) {
		final long start = System.currentTimeMillis();
		final BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream), bufferSize);
		int feedLinesNumber = 0;
		String footerLine = null;
		try {
			final LineBuffer lineBuffer = new LineBuffer();
			feedDataProcessor.startFeed(globalAttributes);
			this.fillBuffer(lineBuffer, br);
			this.processFirstLine(lineBuffer, globalAttributes);
			while (true) {
				this.fillBuffer(lineBuffer, br);
				final FeedProcessingPhase fpp = this.doProcessSingleLine(lineBuffer, globalAttributes, br);
				if (fpp == FeedProcessingPhase.STOP) {
					break;
				} else if (fpp == FeedProcessingPhase.STOP_AND_COUNT) {
					feedLinesNumber++;
					break;
				} else if (fpp == FeedProcessingPhase.ONLY_FOOTER_LEFT) {
					footerLine = lineBuffer.getLine();
					final int currentBufferSize = lineBuffer.getSize();
					if (currentBufferSize > 0) {
						throw new IllegalStateException("Only footer should be here. Found " + currentBufferSize + " elements left!");
					}
					break;
				} else if (fpp == FeedProcessingPhase.PROCESSED_DATA_LINE) {
					feedLinesNumber++;
					continue;
				}
			}
			if (isDebugEnabled) {
				log.debug("Successfully processed {} lines in file", feedLinesNumber);
			}
			if (feedFileSizeHistogram != null) {
				feedFileSizeHistogram.update(feedLinesNumber);
			}
			if (!metricsOff) {
				TOTAL_ROWS_PROCESSED.addAndGet(feedLinesNumber);
			}
			return feedLinesNumber;
		} catch (final IOException ie) {
			throw new IllegalStateException("IOException while processing feed", ie);
		} finally {
			feedDataProcessor.closeFeed(feedLinesNumber, globalAttributes);
			long filesProcessedSoFar = 0;
			if (!metricsOff) {
				filesProcessedSoFar = TOTAL_INPUT_FILES_PROCESSED.incrementAndGet();
			}
			this.outputFeedProcessingStatistics(feedLinesNumber, start, filesProcessedSoFar);
			if (processAndValidateFooter) {
				this.processFooter(feedLinesNumber, footerLine);
			}
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fileInputStream);
		}
	}

	private void outputFeedProcessingStatistics(final int feedLinesNumber, final long start, final long filesProcessedSoFar) {
		if (outputProcessingStatistics) {
			final float total = System.currentTimeMillis() - start;
			final float totalSec = total / 1000;
			String averagePerSec = "N/A";
			if (totalSec > 0 && feedLinesNumber > 0) {
				final float val = feedLinesNumber / totalSec;
				averagePerSec = String.format(AVERAGE_NUMBER_OUTPUT_FORMAT, val) + " rows/second";
			} else if (totalSec == 0 && total > 0) {
				final float val = feedLinesNumber / total;
				averagePerSec = String.format(AVERAGE_NUMBER_OUTPUT_FORMAT, val) + " rows/millisecond";
			}
			final String currentThreadName = Thread.currentThread().getName();
			String messageToOutput = "(" + currentThreadName + ") Processed " + feedLinesNumber + " rows in " + total + "ms ("
					+ String.format(AVERAGE_NUMBER_OUTPUT_FORMAT, totalSec) + " sec). Average " + averagePerSec;
			if (filesProcessedSoFar > 0) {
				messageToOutput += ". So far processed " + filesProcessedSoFar + " input feed files";
			}
			BaukUtil.logEngineMessage(messageToOutput);
		}
	}

	private void exposeControlFeedDataAsAttributes(final String feedDataLine, final Map<String, String> globalAttributes) {
		final String ffName = this.getFactFeed().getName();
		if (StringUtil.isEmpty(feedDataLine)) {
			throw new IllegalStateException("Last line for control feed " + ffName + " is null or empty!");
		}
		if (isDebugEnabled) {
			log.debug("Exposing last line {} as global attributes for control feed {}", feedDataLine, ffName);
		}
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
		if (isDebugEnabled) {
			log.debug("Validating footer for {}. Processed in total {} lines, comparing with values in footer line {}", this.getFactFeed().getName(),
					feedLinesNumber, footerLine);
		}
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

	private Map<String, String> processHeader(final String line) {
		final Map<String, String> parsedHeaderValues = headerParser.parseHeader(line, declaredHeaderAttributes);
		if (isDebugEnabled) {
			final String feedName = this.getFactFeed().getName();
			log.debug("Parsed header values for {} are {}", feedName, parsedHeaderValues);
		}
		return parsedHeaderValues;
	}

	public int process(final InputStream input, final Map<String, String> globalAttributes) {
		return this.readFile(input, globalAttributes);
	}

}
