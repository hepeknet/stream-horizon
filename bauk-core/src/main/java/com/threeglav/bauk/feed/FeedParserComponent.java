package com.threeglav.bauk.feed;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.ConfigurationProperties;
import com.threeglav.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.bauk.model.BaukAttribute;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.DataProcessingType;
import com.threeglav.bauk.model.FactFeed;
import com.threeglav.bauk.model.FactFeedType;
import com.threeglav.bauk.parser.AbstractFeedParser;
import com.threeglav.bauk.parser.DeltaFeedParser;
import com.threeglav.bauk.parser.FullFeedParser;
import com.threeglav.bauk.parser.RepetitiveFeedParser;
import com.threeglav.bauk.util.StringUtil;

public final class FeedParserComponent extends ConfigAware {

	private final AbstractFeedParser feedParser;
	private final boolean checkEveryLineValidity;
	private final String firstStringInEveryLine;
	private final int expectedTokensInEveryDataLine;
	private final FeedDataLineProcessor feedDataLineProcessor;

	public FeedParserComponent(final FactFeed ff, final BaukConfiguration config, final String routeIdentifier) {
		super(ff, config);
		final FactFeedType fft = ff.getType();
		final String delimiter = ff.getDelimiterString();
		if (StringUtil.isEmpty(delimiter)) {
			throw new IllegalArgumentException("Delimiter must not be null or empty string");
		}
		expectedTokensInEveryDataLine = this.getExpectedAttributesNumber(ff);
		if (fft == FactFeedType.DELTA) {
			feedParser = new DeltaFeedParser(delimiter, expectedTokensInEveryDataLine);
			feedParser.setNullString(ff.getNullString());
			log.debug("Will use delta feed parser for feed {}. Null string is set to [{}]", ff.getName(), ff.getNullString());
		} else if (fft == FactFeedType.FULL || fft == FactFeedType.CONTROL) {
			feedParser = new FullFeedParser(delimiter, expectedTokensInEveryDataLine);
		} else if (fft == FactFeedType.REPETITIVE) {
			feedParser = new RepetitiveFeedParser(delimiter, 0, expectedTokensInEveryDataLine);
			log.debug("Will use repetitive feed parser for feed {}", ff.getName());
		} else {
			throw new IllegalStateException("Unknown fact feed type " + fft);
		}
		if (log.isDebugEnabled()) {
			log.debug("For feed {} expect to find {} attributes in every data line, delimiter {}", ff.getName(), expectedTokensInEveryDataLine,
					ff.getDelimiterString());
		}
		firstStringInEveryLine = this.getFactFeed().getData().getEachLineStartsWithCharacter();
		/*
		 * should we check every data line for validity or not?
		 */
		final boolean isStrictCheckingRequired = this.getFactFeed().getData().getProcess() == DataProcessingType.NORMAL;
		this.validate(isStrictCheckingRequired);
		checkEveryLineValidity = isStrictCheckingRequired && !StringUtil.isEmpty(firstStringInEveryLine);
		if (checkEveryLineValidity) {
			log.debug("Will check validity of every line in feed by comparing first value in every line with [{}]", firstStringInEveryLine);
		}
		feedDataLineProcessor = this.resolveFeedProcessor();
		if (feedDataLineProcessor != null) {
			try {
				feedDataLineProcessor.init(ConfigurationProperties.getEngineConfigurationProperties());
			} catch (final Exception exc) {
				log.error("Exception while initializing custom feed data line processor {}", feedDataLineProcessor);
				throw exc;
			}
		}
	}

	private FeedDataLineProcessor resolveFeedProcessor() {
		final String dataMappingClassName = this.getFactFeed().getData().getFeedDataProcessorClassName();
		if (!StringUtil.isEmpty(dataMappingClassName)) {
			log.info("Will try to resolve feed data processor class [{}]", dataMappingClassName);
			final CustomProcessorResolver<FeedDataLineProcessor> dataMapperInstanceResolver = new CustomProcessorResolver<>(dataMappingClassName,
					FeedDataLineProcessor.class);
			final FeedDataLineProcessor fdlp = dataMapperInstanceResolver.resolveInstance();
			log.debug("Found data mapper {}", feedParser);
			return fdlp;
		} else {
			log.info("Will not use any custom data mapping logic");
			return null;
		}
	}

	private void validate(final boolean strictCheckingRequired) {
		if (strictCheckingRequired && StringUtil.isEmpty(firstStringInEveryLine)) {
			throw new IllegalStateException(
					"Configured to strict check every feed data line but start character not set in configuration! Check your configuration file!");
		}
	}

	private int getExpectedAttributesNumber(final FactFeed ff) {
		final boolean expectFirstAttribute = !StringUtil.isEmpty(ff.getData().getEachLineStartsWithCharacter());
		int expectedTokens;
		final ArrayList<BaukAttribute> attributes = ff.getData().getAttributes();
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

	private final void checkDataLineIsValidAndSetToNull(final String[] parsed) {
		if (parsed[0].equals(firstStringInEveryLine)) {
			parsed[0] = null;
		} else {
			throw new IllegalStateException("Failed to validate feed data line. Expected first element to be [" + firstStringInEveryLine
					+ "] but got [" + parsed[0] + "] instead! Change your configuration or data format!");
		}
	}

	public String[] parseData(final String line, final Map<String, String> globalAttributes) {
		final String[] parsed = feedParser.parse(line);
		if (checkEveryLineValidity) {
			this.checkDataLineIsValidAndSetToNull(parsed);
		}
		try {
			if (feedDataLineProcessor != null) {
				return feedDataLineProcessor.preProcessDataLine(parsed, globalAttributes);
			}
		} catch (final Exception exc) {
			log.error("Exception caught while invoking customized data line processor. Passed line [{}], global attributes {}. Details {}", line,
					globalAttributes, exc.getMessage());
			throw exc;
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
