package com.threeglav.bauk.feed.processing;

import java.util.Map;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public final class SingleThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	public SingleThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config, routeIdentifier);
	}

	@Override
	public void processLine(final String line, final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Processing line {} - attributes {}", line, globalAttributes);
		}
		final String[] parsedData = feedParserComponent.parseData(line);
		final Object[] resolvedData = bulkoutputResolver.resolveValues(parsedData, globalAttributes);
		bulkOutputWriter.doOutput(resolvedData);
	}

	@Override
	public void processLastLine(final String line, final Map<String, String> globalAttributes) {
		if (isDebugEnabled) {
			log.debug("Processing last line {} - attributes {}", line, globalAttributes);
		}
		final String[] parsedData = feedParserComponent.parseData(line);
		final Object[] resolvedData = bulkoutputResolver.resolveLastLineValues(parsedData, globalAttributes);
		bulkOutputWriter.doOutput(resolvedData);
	}

}
