package com.threeglav.bauk.feed;

import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.FactFeed;

public class SingleThreadedFeedDataProcessor extends AbstractFeedDataProcessor {

	public SingleThreadedFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config, routeIdentifier);
	}

	@Override
	public void processLine(final String line) {
		final String[] parsedData = feedParserComponent.parseData(line);
		final String lineForOutput = bulkoutputResolver.resolveValues(parsedData, headerAttributes, globalAttributes);
		bulkWriter.write(lineForOutput);
		bulkWriter.write("\n");
	}

}
