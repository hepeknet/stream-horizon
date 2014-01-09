package com.threeglav.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
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

	private static final int READ_AHEAD_LINES = 32;

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
	public static final AtomicLong TOTAL_INPUT_FILES_PROCESSED = new AtomicLong(-1);
	public static final AtomicLong TOTAL_ROWS_PROCESSED = new AtomicLong(-1);
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
		bufferSize = ConfigurationProperties.getSystemProperty(SystemConfigurationConstants.WRITE_BUFFER_SIZE_SYS_PARAM_NAME,
				SystemConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE;
		log.info("Read buffer size is {} MB", bufferSize);
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

	private void processFirstLine(final ArrayDeque<String> lines, final Map<String, String> globalAttributes) {
		if (headerShouldExist) {
			final String line = lines.poll();
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

	private boolean hasMoreDataLines(final ArrayDeque<String> lines) {
		if (processAndValidateFooter) {
			return lines.size() > 1;
		} else {
			return lines.size() > 0;
		}
	}

	private boolean hasAnyOtherLines(final ArrayDeque<String> lines) {
		return !lines.isEmpty();
	}

	enum FeedProcessingPhase {
		STOP_AND_COUNT, STOP, ONLY_FOOTER_LEFT, PROCESSED_DATA_LINE;
	}

	private FeedProcessingPhase processLine(final ArrayDeque<String> lines, final Map<String, String> globalAttributes) {
		final String line = lines.poll();
		final boolean hasMoreDataLines = this.hasMoreDataLines(lines);
		final boolean hasAnyOtherLines = this.hasAnyOtherLines(lines);
		if (!hasAnyOtherLines) {
			if (isControlFeed) {
				this.exposeControlFeedDataAsAttributes(line, globalAttributes);
				return FeedProcessingPhase.STOP;
			} else if (processAndValidateFooter) {
				lines.offer(line);
				return FeedProcessingPhase.ONLY_FOOTER_LEFT;
			} else {
				feedDataProcessor.processLine(line, globalAttributes, true);
				return FeedProcessingPhase.STOP_AND_COUNT;
			}
		} else {
			feedDataProcessor.processLine(line, globalAttributes, !hasMoreDataLines);
			return FeedProcessingPhase.PROCESSED_DATA_LINE;
		}
	}

	public int readFile(final InputStream fileInputStream, final Map<String, String> globalAttributes) {
		final BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream), bufferSize);
		int feedLinesNumber = 0;
		String footerLine = null;
		final long start = System.currentTimeMillis();
		try {
			final ArrayDeque<String> lineQueue = new ArrayDeque<>(READ_AHEAD_LINES);
			feedDataProcessor.startFeed(globalAttributes);
			this.fillArrayQueue(br, lineQueue);
			this.processFirstLine(lineQueue, globalAttributes);
			while (true) {
				this.fillArrayQueue(br, lineQueue);
				final FeedProcessingPhase fpp = this.processLine(lineQueue, globalAttributes);
				if (fpp == FeedProcessingPhase.STOP) {
					break;
				} else if (fpp == FeedProcessingPhase.STOP_AND_COUNT) {
					feedLinesNumber++;
					break;
				} else if (fpp == FeedProcessingPhase.ONLY_FOOTER_LEFT) {
					footerLine = lineQueue.poll();
					if (!lineQueue.isEmpty()) {
						throw new IllegalStateException("Only footer should be here. Found " + lineQueue.size() + " elements left!");
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
				BaukUtil.logEngineMessage("Processed " + feedLinesNumber + " rows in " + total + "ms (" + totalSec + " seconds). Average "
						+ averagePerSec);
			}
			if (!metricsOff) {
				TOTAL_INPUT_FILES_PROCESSED.incrementAndGet();
			}
			if (processAndValidateFooter) {
				this.processFooter(feedLinesNumber, footerLine);
			}
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(fileInputStream);
		}
	}

	private void fillArrayQueue(final BufferedReader br, final ArrayDeque<String> lineQueue) throws IOException {
		for (int i = 0; i < READ_AHEAD_LINES; i++) {
			if (lineQueue.size() < READ_AHEAD_LINES) {
				final String line = br.readLine();
				if (line != null) {
					lineQueue.add(line);
				}
			} else {
				break;
			}
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
		final Header header = this.getFactFeed().getHeader();
		final String feedName = this.getFactFeed().getName();
		final Map<String, String> parsedHeaderValues = headerParser.parseHeader(line, declaredHeaderAttributes,
				header.getEachLineStartsWithCharacter(), this.getFactFeed().getDelimiterString());
		if (isDebugEnabled) {
			log.debug("Parsed header values for {} are {}", feedName, parsedHeaderValues);
		}
		return parsedHeaderValues;
	}

	public int process(final InputStream input, final Map<String, String> globalAttributes) {
		return this.readFile(input, globalAttributes);
	}

}
