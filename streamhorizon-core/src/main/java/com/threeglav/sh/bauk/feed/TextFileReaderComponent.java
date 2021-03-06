package com.threeglav.sh.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.threeglav.sh.bauk.BaukConstants;
import com.threeglav.sh.bauk.BaukEngineConfigurationConstants;
import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.EngineRegistry;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.feed.processing.FeedDataProcessor;
import com.threeglav.sh.bauk.header.DefaultHeaderParser;
import com.threeglav.sh.bauk.header.HeaderParser;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedType;
import com.threeglav.sh.bauk.model.FooterProcessingType;
import com.threeglav.sh.bauk.model.Header;
import com.threeglav.sh.bauk.model.HeaderProcessingType;
import com.threeglav.sh.bauk.parser.FeedParser;
import com.threeglav.sh.bauk.parser.FullFeedParser;
import com.threeglav.sh.bauk.util.AttributeParsingUtil;
import com.threeglav.sh.bauk.util.BaukUtil;
import com.threeglav.sh.bauk.util.StringUtil;

public class TextFileReaderComponent extends ConfigAware {

	private final int bufferSize;
	private HeaderParser headerParser;
	private final boolean processAndValidateFooter;
	private final FeedParser footerLineParser;
	private final String footerFirstString;
	private final String[] declaredHeaderAttributes;
	private final FeedDataProcessor feedDataProcessor;
	private final HeaderProcessingType headerProcessingType;
	private final boolean isControlFeed;
	private final boolean headerShouldExist;
	private final boolean skipHeader;
	private final boolean outputProcessingStatistics;
	private final int footerRecordCountPosition;
	private String currentThreadName;

	public TextFileReaderComponent(final Feed factFeed, final BaukConfiguration config, final FeedDataProcessor feedDataProcessor,
			final String routeIdentifier) {
		super(factFeed, config);
		this.validate();
		this.feedDataProcessor = feedDataProcessor;
		processAndValidateFooter = factFeed.getSourceFormatDefinition().getFooter() != null
				&& factFeed.getSourceFormatDefinition().getFooter().getProcess() == FooterProcessingType.STRICT;
		if (processAndValidateFooter) {
			footerLineParser = new FullFeedParser(this.getFactFeed().getSourceFormatDefinition().getDelimiterString(), 10);
			footerFirstString = this.getFactFeed().getSourceFormatDefinition().getFooter().getEachLineStartsWithCharacter();
			footerRecordCountPosition = this.getFactFeed().getSourceFormatDefinition().getFooter().getRecordCountAttributePosition();
			if (footerRecordCountPosition <= 0) {
				throw new IllegalStateException("Footer record count attribute position must be > 0. Provided value is " + footerRecordCountPosition);
			}
			log.info("Footer record count position is {}", footerRecordCountPosition);
			log.debug("For feed {} footer processing is {}", this.getFactFeed().getName(), factFeed.getSourceFormatDefinition().getFooter()
					.getProcess());
		} else {
			footerLineParser = null;
			footerFirstString = null;
			footerRecordCountPosition = -1;
		}
		headerProcessingType = this.getFactFeed().getSourceFormatDefinition().getHeader().getProcess();
		final boolean shouldProcessHeader = this.checkProcessHeader();
		if (shouldProcessHeader) {
			log.debug("Extracting header attributes for {}", factFeed.getName());
			declaredHeaderAttributes = AttributeParsingUtil.getAttributeNames(this.getFactFeed().getSourceFormatDefinition().getHeader()
					.getAttributes());
			this.initializeHeaderProcessor();
		} else {
			declaredHeaderAttributes = null;
		}
		bufferSize = (int) (ConfigurationProperties.getSystemProperty(BaukEngineConfigurationConstants.READ_BUFFER_SIZE_SYS_PARAM_NAME,
				BaukEngineConfigurationConstants.DEFAULT_READ_WRITE_BUFFER_SIZE_MB) * BaukConstants.ONE_MEGABYTE);
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("Read buffer size must not be <= 0");
		}
		log.info("Read buffer size is {} bytes", bufferSize);
		isControlFeed = this.getFactFeed().getType() == FeedType.CONTROL;
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
		outputProcessingStatistics = ConfigurationProperties.getSystemProperty(
				BaukEngineConfigurationConstants.PRINT_PROCESSING_STATISTICS_PARAM_NAME, false);
		if (outputProcessingStatistics) {
			log.info("Will output processing statistics!");
		}
	}

	private boolean checkProcessHeader() {
		final Header header = this.getFactFeed().getSourceFormatDefinition().getHeader();
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
		final String configuredHeaderParserClass = this.getFactFeed().getSourceFormatDefinition().getHeader().getHeaderParserClassName();
		if (!StringUtil.isEmpty(configuredHeaderParserClass)) {
			headerParserClassName = configuredHeaderParserClass;
			log.debug("Will try to use custom header parser class {}", configuredHeaderParserClass);
		} else {
			log.debug("Will use default header parser class {}", headerParserClassName);
		}
		final CustomProcessorResolver<HeaderParser> headerParserInstanceResolver = new CustomProcessorResolver<>(headerParserClassName,
				HeaderParser.class);
		headerParser = headerParserInstanceResolver.resolveInstance();
		if (headerParser == null) {
			throw new IllegalStateException("Problem while loading header parser class! Possibly problems with compilation!");
		}
		final Header header = this.getFactFeed().getSourceFormatDefinition().getHeader();
		final String startsWithString = header.getEachLineStartsWithCharacter();
		final String delimiterString = this.getFactFeed().getSourceFormatDefinition().getDelimiterString();
		headerParser.init(startsWithString, delimiterString, ConfigurationProperties.getEngineConfigurationProperties());
	}

	private void validate() {
		if (this.getFactFeed().getSourceFormatDefinition().getHeader() == null) {
			throw new IllegalArgumentException("Header configuration must not be null. Check your configuration file!");
		}
		if (this.getFactFeed().getSourceFormatDefinition().getFooter() != null) {
			final FooterProcessingType footerProcessing = this.getFactFeed().getSourceFormatDefinition().getFooter().getProcess();
			if (footerProcessing == FooterProcessingType.STRICT
					&& StringUtil.isEmpty(this.getFactFeed().getSourceFormatDefinition().getFooter().getEachLineStartsWithCharacter())) {
				throw new IllegalStateException(
						"Footer is set to strict processing but can not find first character in configuration! Check your configuration file!");
			}
		}
	}

	private void processFirstLine(final LineBuffer lines, final Map<String, String> globalAttributes) {
		if (headerShouldExist) {
			final String line = lines.getLine();
			if (!skipHeader) {
				try {
					final Map<String, String> headerAttributes = this.processHeader(line, globalAttributes);
					if (headerAttributes != null) {
						globalAttributes.putAll(headerAttributes);
						if (isTraceEnabled) {
							log.trace(
									"Added all attributes {} from header to global attributes. All attributes, before starting to process body for feed {} are {}",
									headerAttributes, this.getFactFeed().getName(), globalAttributes);
						}
					}
				} catch (final Exception exc) {
					log.error("Exception while parsing header values! Header attributes will not be available!", exc);
					throw exc;
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

	static enum FeedProcessingPhase {
		STOP_AND_COUNT, STOP, ONLY_FOOTER_LEFT, PROCESSED_DATA_LINE;
	}

	private final FeedProcessingPhase doProcessSingleLine(final LineBuffer lines, final Map<String, String> globalAttributes, final BufferedReader br)
			throws IOException {
		final boolean noMoreLinesAvailable = !this.hasAnyOtherLines(lines);
		if (noMoreLinesAvailable) {
			final String line = lines.getLine();
			this.fillBuffer(lines, br);
			if (isControlFeed) {
				this.exposeControlFeedDataAsAttributes(line, globalAttributes);
				return FeedProcessingPhase.STOP;
			}
			if (processAndValidateFooter) {
				lines.add(line);
				return FeedProcessingPhase.ONLY_FOOTER_LEFT;
			}
			// else
			feedDataProcessor.processLastLine(line, globalAttributes);
			return FeedProcessingPhase.STOP_AND_COUNT;
		} else {
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

	private final void fillBuffer(final LineBuffer buffer, final BufferedReader br) throws IOException {
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
		boolean success = false;
		try {
			final LineBuffer lineBuffer = new LineBuffer();
			this.fillBuffer(lineBuffer, br);
			this.processFirstLine(lineBuffer, globalAttributes);
			feedDataProcessor.startFeed(globalAttributes);
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
			globalAttributes.put(BaukConstants.IMPLICIT_ATTRIBUTE_FILE_INPUT_FEED_PROCESSING_FINISHED_TIMESTAMP,
					String.valueOf(System.currentTimeMillis()));
			success = true;
			return feedLinesNumber;
		} catch (final IOException ie) {
			log.error("IOException while processing feed", ie);
			throw new IllegalStateException("IOException while processing feed. Total lines processed so far in this input file " + feedLinesNumber,
					ie);
		} catch (final Exception exc) {
			log.error("Exception while processing feed", exc);
			throw new RuntimeException("Exception while processing feed. Total lines processed so far in this input file " + feedLinesNumber, exc);
		} finally {
			try {
				if (processAndValidateFooter) {
					this.processFooter(feedLinesNumber, footerLine);
				}
			} catch (final Exception exc) {
				success = false;
				throw exc;
			} finally {
				feedDataProcessor.closeFeed(feedLinesNumber, globalAttributes, success);
				if (success) {
					this.outputFeedProcessingStatistics(feedLinesNumber, start);
				}
				IOUtils.closeQuietly(br);
				IOUtils.closeQuietly(fileInputStream);
			}
		}
	}

	private final String getCurrentThreadName() {
		if (currentThreadName == null) {
			currentThreadName = Thread.currentThread().getName();
		}
		return currentThreadName;
	}

	private void outputFeedProcessingStatistics(final int feedLinesNumber, final long start) {
		if (outputProcessingStatistics) {
			final float totalMillis = System.currentTimeMillis() - start;
			final float totalSec = totalMillis / 1000;
			String averagePerSec;
			if (totalSec > 0 && feedLinesNumber > 0) {
				final float val = feedLinesNumber / totalSec;
				averagePerSec = BaukUtil.DEC_FORMAT.format(val) + " rows/second";
			} else if (totalSec == 0 && totalMillis > 0) {
				final float val = feedLinesNumber / totalMillis;
				averagePerSec = BaukUtil.DEC_FORMAT.format(val) + " rows/millisecond";
			} else {
				averagePerSec = "N/A";
			}
			String messageToOutput = this.getCurrentThreadName() + " - Processed " + feedLinesNumber + " rows in "
					+ BaukUtil.DEC_FORMAT.format(totalMillis) + "ms";
			if (totalMillis > 1000) {
				messageToOutput += " (" + BaukUtil.DEC_FORMAT.format(totalSec) + " sec)";
			}
			messageToOutput += ". Average " + averagePerSec;
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
		final ArrayList<BaukAttribute> ffDataAttrs = this.getFactFeed().getSourceFormatDefinition().getData().getAttributes();
		final FeedParser feedParser = new FullFeedParser(this.getFactFeed().getSourceFormatDefinition().getDelimiterString(), ffDataAttrs.size());
		final String[] splitLine = feedParser.parse(feedDataLine);
		if (splitLine.length != ffDataAttrs.size()) {
			log.error("Control feed " + ffName + " declared " + ffDataAttrs.size() + " data attributes but last line only has " + splitLine.length
					+ " values!");
			throw new IllegalStateException("Control feed " + ffName + " declared " + ffDataAttrs.size() + " data attributes but last line only has "
					+ splitLine.length + " values!");
		}
		for (int i = 0; i < ffDataAttrs.size(); i++) {
			final BaukAttribute attr = ffDataAttrs.get(i);
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
		if (footerParsedValues == null) {
			throw new IllegalStateException("Did not find any values in footer. But footer record count position is set to be found at position "
					+ footerRecordCountPosition);
		}
		if (footerParsedValues.length < footerRecordCountPosition) {
			throw new IllegalStateException("Found " + footerParsedValues.length
					+ " values in footer. But footer record count position is set to be found at position " + footerRecordCountPosition);
		}
		if (!footerParsedValues[0].equals(footerFirstString)) {
			throw new IllegalStateException("First character of footer line " + footerParsedValues[0]
					+ " does not match the one given in configuration file " + footerFirstString);
		}
		try {
			final Integer footerIntValue = Integer.parseInt(footerParsedValues[footerRecordCountPosition]);
			if (feedLinesNumber != footerIntValue) {
				throw new IllegalStateException("Footer value " + footerIntValue + " does not match with total number of processed lines "
						+ feedLinesNumber + ". Feed name is [" + this.getFactFeed().getName() + "].");
			}
		} catch (final NumberFormatException nfe) {
			throw new IllegalStateException("Footer value [" + footerParsedValues[footerRecordCountPosition]
					+ "] can not be converted to integer value!");
		}
	}

	private Map<String, String> processHeader(final String line, final Map<String, String> globalAttrs) {
		final Map<String, String> parsedHeaderValues = headerParser.parseHeader(line, declaredHeaderAttributes, globalAttrs);
		if (isDebugEnabled) {
			final String feedName = this.getFactFeed().getName();
			log.debug("Parsed header values for {} are {}", feedName, parsedHeaderValues);
		}
		return parsedHeaderValues;
	}

	public int process(final InputStream inputStream, final Map<String, String> globalAttributes) {
		try {
			final int feedLineNum = this.readFile(inputStream, globalAttributes);
			EngineRegistry.registerProcessedFeedRows(feedLineNum);
			return feedLineNum;
		} catch (final Exception exc) {
			EngineRegistry.registerFailedFeedFile();
			throw exc;
		}
	}

}
