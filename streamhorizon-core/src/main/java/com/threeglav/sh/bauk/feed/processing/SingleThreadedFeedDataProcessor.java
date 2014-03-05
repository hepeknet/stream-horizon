package com.threeglav.sh.bauk.feed.processing;

import java.util.Map;

import com.threeglav.sh.bauk.model.BaukConfiguration;
import com.threeglav.sh.bauk.model.FactFeed;

public final class SingleThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	public SingleThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config) {
		super(factFeed, config);
	}

	@Override
	public void processLine(final String line, final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Processing line {} - attributes {}", line, globalAttributes);
		}
		final String[] parsedData = feedParserComponent.parseData(line, globalAttributes);
		final Object[] resolvedData = bulkoutputResolver.resolveValues(parsedData, globalAttributes);
		bulkOutputWriter.doWriteOutput(resolvedData, globalAttributes);
	}

	@Override
	public void processLastLine(final String line, final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Processing last line {} - attributes {}", line, globalAttributes);
		}
		final String[] parsedData = feedParserComponent.parseData(line, globalAttributes);
		final Object[] resolvedData = bulkoutputResolver.resolveLastLineValues(parsedData, globalAttributes);
		bulkOutputWriter.doWriteOutput(resolvedData, globalAttributes);
	}

}
