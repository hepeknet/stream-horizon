package com.threeglav.bauk.feed;

import java.util.ArrayList;

import com.codahale.metrics.Meter;
import com.threeglav.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.bauk.model.Attribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.model.HeaderFooterProcessType;
import com.threeglav.bauk.parser.AbstractFeedParser;
import com.threeglav.bauk.parser.DeltaFeedParser;
import com.threeglav.bauk.parser.FullFeedParser;
import com.threeglav.bauk.parser.RepetitiveFeedParser;
import com.threeglav.bauk.util.MetricsUtil;
import com.threeglav.bauk.util.StringUtil;

public class FeedParserComponent extends ConfigAware {

	private AbstractFeedParser feedParser;
	private final Meter parsedLinesMeter;
	private final boolean checkEveryLineValidity;
	private final String firstStringInEveryLine;
	private final int expectedTokensInEveryDataLine;
	private FeedProcessor feedProcessor;

	public FeedParserComponent(final FactFeed ff, final BaukConfiguration config, final String routeIdentifier) {
		super(ff, config);
		final FactFeedType fft = ff.getType();
		final String delimiter = ff.getDelimiterString();
		if (StringUtil.isEmpty(delimiter)) {
			throw new IllegalArgumentException("Delimiter must not be null or empty string");
		}
		if (fft == FactFeedType.DELTA) {
			feedParser = new DeltaFeedParser(delimiter);
		} else if (fft == FactFeedType.FULL) {
			feedParser = new FullFeedParser(delimiter);
		} else if (fft == FactFeedType.REPETITIVE) {
			feedParser = new RepetitiveFeedParser(delimiter, 0);
		} else {
			throw new IllegalStateException("Unknown fact feed type " + fft);
		}
		expectedTokensInEveryDataLine = this.getExpectedAttributesNumber(ff);
		feedParser.setExpectedTokens(expectedTokensInEveryDataLine);
		if (log.isDebugEnabled()) {
			log.debug("For feed {} expect to find {} attributes in every data line, delimiter {}", ff.getName(), expectedTokensInEveryDataLine,
					ff.getDelimiterString());
		}
		parsedLinesMeter = MetricsUtil.createMeter("(" + routeIdentifier + ") - Parsed lines");
		firstStringInEveryLine = this.getFactFeed().getData().getEachLineStartsWithCharacter();
		final boolean isStrictCheckingRequired = this.getFactFeed().getData().getProcess() == HeaderFooterProcessType.STRICT;
		this.validate(isStrictCheckingRequired);
		checkEveryLineValidity = isStrictCheckingRequired && !StringUtil.isEmpty(firstStringInEveryLine);
		if (checkEveryLineValidity) {
			log.debug("Will check validity of every line in feed by comparing it with {}", firstStringInEveryLine);
		}
		this.resolveFeedProcessor();
	}

	private void resolveFeedProcessor() {
		final String dataMappingClassName = this.getFactFeed().getData().getDataMappingClassName();
		if (!StringUtil.isEmpty(dataMappingClassName)) {
			log.debug("Will try to resolve data mapper {}", dataMappingClassName);
			final CustomProcessorResolver<FeedProcessor> headerParserInstanceResolver = new CustomProcessorResolver<>(dataMappingClassName,
					FeedProcessor.class);
			feedProcessor = headerParserInstanceResolver.resolveInstance();
			log.debug("Found data mapper {}", feedParser);
		} else {
			log.debug("Will not use any data mapping");
		}
	}

	private void validate(final boolean strictCheckingRequired) {
		final HeaderFooterProcessType dataProcessType = this.getFactFeed().getData().getProcess();
		if (dataProcessType == HeaderFooterProcessType.NO_HEADER || dataProcessType == HeaderFooterProcessType.SKIP) {
			throw new IllegalStateException("Invalid value " + dataProcessType + ". Not valid for feed data processing!");
		}
		if (strictCheckingRequired && StringUtil.isEmpty(firstStringInEveryLine)) {
			throw new IllegalStateException(
					"Configured to strict check every feed data line but start character not set in configuration! Check your configuration file!");
		}
	}

	private int getExpectedAttributesNumber(final FactFeed ff) {
		final boolean expectFirstAttribute = !StringUtil.isEmpty(ff.getData().getEachLineStartsWithCharacter());
		int expectedTokens;
		final ArrayList<Attribute> attributes = ff.getData().getAttributes();
		if (expectFirstAttribute) {
			expectedTokens = attributes.size() + 1;
		} else {
			expectedTokens = attributes.size();
		}
		if (expectFirstAttribute) {
			log.info("Will expect that every data line in feed starts with {}", ff.getData().getEachLineStartsWithCharacter());
		}
		log.info("Regardless of how many fields are in feed files, only {} fields have been declared in configuration and will be used",
				expectedTokens);
		return expectedTokens;
	}

	private void checkDataLineIsValidAndSetToNull(final String[] parsed) {
		if (parsed[0].equals(firstStringInEveryLine)) {
			parsed[0] = null;
		} else {
			throw new IllegalStateException("Failed to validate feed data line. Expected " + firstStringInEveryLine + " but got " + parsed[0]
					+ " instead!");
		}
	}

	public String[] parseData(final String line) {
		if (line == null) {
			return null;
		}
		final String[] parsed = feedParser.parse(line);
		if (checkEveryLineValidity) {
			this.checkDataLineIsValidAndSetToNull(parsed);
		}
		if (parsedLinesMeter != null) {
			parsedLinesMeter.mark();
		}
		if (feedProcessor != null) {
			return feedProcessor.preProcess(parsed);
		}
		return parsed;
	}

	/*
	 * used for testing
	 */

	boolean isCheckEveryLineValidity() {
		return checkEveryLineValidity;
	}

	String getFirstStringInEveryLine() {
		return firstStringInEveryLine;
	}

	int getExpectedTokensInEveryDataLine() {
		return expectedTokensInEveryDataLine;
	}

}
