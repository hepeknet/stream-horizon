package com.threeglav.bauk.feed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.codahale.metrics.Histogram;
import com.threeglav.bauk.Constants;
import com.threeglav.bauk.dimension.cache.CacheInstanceManager;
import com.threeglav.bauk.dimension.db.DbHandler;
import com.threeglav.bauk.header.DefaultHeaderParser;
import com.threeglav.bauk.header.HeaderParser;
import com.threeglav.bauk.header.HeaderParsingUtil;
import com.threeglav.bauk.model.Config;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.HeaderFooter;
import com.threeglav.bauk.model.HeaderFooterProcessType;
import com.threeglav.bauk.parser.FeedParser;
import com.threeglav.bauk.parser.FullFeedParser;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class TextFileReaderComponent extends ConfigAware {

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 1;
	private final int bufferSize = DEFAULT_BUFFER_SIZE;
	private final HeaderParser headerParser;
	private final boolean processAndValidateFooter;
	private final Histogram feedFileSizeHistogram;
	private final BulkFileWriter bulkWriter;
	private final BulkOutputValuesResolver bulkoutputResolver;
	private final FeedParserComponent feedParserComponent;

	public TextFileReaderComponent(final FactFeed factFeed, final Config config, final DbHandler dbHandler,
			final CacheInstanceManager cacheInstanceManager, final String routeIdentifier) {
		super(factFeed, config);
		this.validate();
		this.headerParser = new DefaultHeaderParser();
		this.bulkWriter = new BulkFileWriter(factFeed, config);
		this.bulkoutputResolver = new BulkOutputValuesResolver(factFeed, config, cacheInstanceManager, dbHandler, routeIdentifier);
		this.feedParserComponent = new FeedParserComponent(factFeed, config, routeIdentifier);
		this.processAndValidateFooter = factFeed.getFooter().getProcess() != HeaderFooterProcessType.SKIP;
		this.feedFileSizeHistogram = MetricsUtil.createHistogram("Input feeds (" + routeIdentifier + ") - number of lines");
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
		final BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream), this.bufferSize);
		try {
			int feedLinesNumber = 0;
			String line = br.readLine();
			boolean processedHeader = false;
			String footerLine = null;
			this.bulkWriter.startWriting(globalAttributes.get(Constants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH));
			Map<String, String> headerAttributes = null;
			while (line != null) {
				if (!processedHeader) {
					headerAttributes = this.processHeader(line);
					processedHeader = true;
					line = br.readLine();
				} else {
					final String nextLine = br.readLine();
					if (nextLine == null) {
						if (this.processAndValidateFooter) {
							footerLine = line;
						} else {
							this.processLine(line, headerAttributes, globalAttributes);
							footerLine = null;
						}
						line = null;
					} else {
						this.processLine(line, headerAttributes, globalAttributes);
						feedLinesNumber++;
						line = nextLine;
					}
				}
			}
			this.log.debug("Successfully processed {} lines in file", feedLinesNumber);
			if (this.feedFileSizeHistogram != null) {
				this.feedFileSizeHistogram.update(feedLinesNumber);
			}
			this.bulkWriter.closeResources();
			this.processFooter(feedLinesNumber, footerLine);
			br.close();
		} catch (final IOException ie) {
			this.log.error("IOException {}", ie.getMessage());
		} finally {
			IOUtils.closeQuietly(fileInputStream);
		}
	}

	private void processLine(final String line, final Map<String, String> headerAttributes, final Map<String, String> globalAttributes) {
		final String[] parsedData = this.feedParserComponent.parseData(line);
		final String lineForOutput = this.bulkoutputResolver.resolveValues(parsedData, headerAttributes, globalAttributes);
		this.bulkWriter.write(lineForOutput);
		this.bulkWriter.write("\n");
	}

	private void processFooter(final int feedLinesNumber, final String footerLine) {
		if (this.processAndValidateFooter) {
			this.log.debug("Validating footer. Processed in total {} lines, comparing with values in footer line {}", feedLinesNumber, footerLine);
			final FeedParser feedParser = new FullFeedParser(this.getFactFeed().getDelimiterString());
			final String[] footerParsedValues = feedParser.parse(footerLine);
			if (footerParsedValues.length > 2) {
				throw new IllegalStateException("Found " + footerParsedValues.length + " values in footer. Expected at most 2!");
			}
			if (!footerParsedValues[0].equals(this.getFactFeed().getFooter().getEachLineStartsWithCharacter())) {
				throw new IllegalStateException("First character of footer line " + footerParsedValues[0]
						+ " does not match the one given in configuration file!");
			}
			try {
				final Integer footerIntValue = Integer.parseInt(footerParsedValues[1]);
				if (feedLinesNumber != footerIntValue) {
					throw new IllegalStateException("Footer value " + footerIntValue + " does not match with total number of processed lines "
							+ feedLinesNumber);
				}
			} catch (final NumberFormatException nfe) {
				throw new IllegalStateException("Footer value " + footerParsedValues[1] + " can not be converted to integer!");
			}
		}
	}

	private Map<String, String> processHeader(final String line) {
		final HeaderFooter header = this.getFactFeed().getHeader();
		final HeaderFooterProcessType headerProcessingType = header.getProcess();
		final String feedName = this.getFactFeed().getName();
		this.log.debug("For feed {} header processing set to {}", feedName, headerProcessingType);
		if (headerProcessingType == HeaderFooterProcessType.NO_HEADER || headerProcessingType == HeaderFooterProcessType.SKIP) {
			this.log.debug("Will skip header processing for {}", feedName);
			return null;
		}
		this.log.debug("Not skipping header processing for {}", feedName);
		final String[] declaredHeaderAttributes = HeaderParsingUtil.getAttributeNames(header.getAttributes());
		final Map<String, String> parsedHeaderValues = this.headerParser.parseHeader(line, declaredHeaderAttributes,
				header.getEachLineStartsWithCharacter(), this.getFactFeed().getDelimiterString());
		this.log.debug("Parsed header values for {} are {}", feedName, parsedHeaderValues);
		return parsedHeaderValues;
	}

	public void process(final InputStream input, final Map<String, String> globalAttributes) {
		final InputStream is = input;
		this.readFile(is, globalAttributes);
	}

}
