package com.threeglav.sh.bauk.feed;

import java.util.ArrayList;
import java.util.Map;

import com.threeglav.sh.bauk.ConfigAware;
import com.threeglav.sh.bauk.ConfigurationProperties;
import com.threeglav.sh.bauk.dynamic.CustomProcessorResolver;
import com.threeglav.sh.bauk.model.BaukAttribute;
import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.DataProcessingType;
import com.threeglav.sh.bauk.model.Feed;
import com.threeglav.sh.bauk.model.FeedType;
import com.threeglav.sh.bauk.parser.AbstractFeedParser;
import com.threeglav.sh.bauk.parser.DeltaFeedParser;
import com.threeglav.sh.bauk.parser.FullFeedParser;
import com.threeglav.sh.bauk.parser.RepetitiveFeedParser;
import com.threeglav.sh.bauk.util.StringUtil;

public final class FeedParserComponent extends ConfigAware {

	private final AbstractFeedParser feedParser;
	private final boolean checkEveryLineValidity;
	private final String firstStringInEveryLine;
	private final int expectedTokensInEveryDataLine;
	private final FeedDataLineProcessor feedDataLineProcessor;

	public FeedParserComponent(final Feed ff, final BaukConfiguration config) {
		super(ff, config);
		final FeedType fft = ff.getType();
		if (ff.getSourceFormatDefinition() == null) {
			throw new IllegalArgumentException("Unable to find delimiter");
		}
		final String delimiter = ff.getSourceFormatDefinition().getDelimiterString();
		if (StringUtil.isEmpty(delimiter)) {
			throw new IllegalArgumentException("Delimiter must not be null or empty string");
		}
		expectedTokensInEveryDataLine = this.getExpectedAttributesNumber(ff);
		if (fft == FeedType.DELTA) {
			feedParser = new DeltaFeedParser(delimiter, expectedTokensInEveryDataLine);
			feedParser.setNullString(ff.getSourceFormatDefinition().getNullString());
			log.debug("Will use delta feed parser for feed {}. Null string is set to [{}]", ff.getName(), ff.getSourceFormatDefinition()
					.getNullString());
		} else if (fft == FeedType.FULL || fft == FeedType.CONTROL) {
			feedParser = new FullFeedParser(delimiter, expectedTokensInEveryDataLine);
		} else if (fft == FeedType.REPETITIVE) {
			feedParser = new RepetitiveFeedParser(delimiter, ff.getRepetitionCount(), expectedTokensInEveryDataLine);
			log.debug("Will use repetitive feed parser for feed {}", ff.getName());
		} else {
			throw new IllegalStateException("Unknown fact feed type " + fft);
		}
		if (log.isDebugEnabled()) {
			log.debug("For feed {} expect to find {} attributes in every data line, delimiter {}", ff.getName(), expectedTokensInEveryDataLine, ff
					.getSourceFormatDefinition().getDelimiterString());
		}
		firstStringInEveryLine = this.getFactFeed().getSourceFormatDefinition().getData().getEachLineStartsWithCharacter();
		/*
		 * should we check every data line for validity or not?
		 */
		final boolean isStrictCheckingRequired = this.getFactFeed().getSourceFormatDefinition().getData().getProcess() == DataProcessingType.NORMAL;
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
		final String dataMappingClassName = this.getFactFeed().getSourceFormatDefinition().getData().getFeedDataProcessorClassName();
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

	private int getExpectedAttributesNumber(final Feed ff) {
		final boolean expectFirstAttribute = !StringUtil.isEmpty(ff.getSourceFormatDefinition().getData().getEachLineStartsWithCharacter());
		int expectedTokens;
		final ArrayList<BaukAttribute> attributes = ff.getSourceFormatDefinition().getData().getAttributes();
		if (expectFirstAttribute) {
			expectedTokens = attributes.size() + 1;
		} else {
			expectedTokens = attributes.size();
		}
		if (expectFirstAttribute) {
			log.info("Will expect that every data line in feed starts with {}", ff.getSourceFormatDefinition().getData()
					.getEachLineStartsWithCharacter());
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
