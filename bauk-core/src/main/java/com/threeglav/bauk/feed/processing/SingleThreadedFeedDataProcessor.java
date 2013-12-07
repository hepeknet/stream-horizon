package com.threeglav.bauk.feed.processing;

import java.util.Map;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public class SingleThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	public SingleThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config, routeIdentifier);
	}

	@Override
	public void processLine(final String line, final Map<String, String> globalAttributes) {
		final String[] parsedData = feedParserComponent.parseData(line);
		final String lineForOutput = bulkoutputResolver.resolveValuesAsSingleLine(parsedData, globalAttributes);
		bulkOutputWriter.doOutput(lineForOutput);
		bulkOutputWriter.doOutput("\n");
	}

}
