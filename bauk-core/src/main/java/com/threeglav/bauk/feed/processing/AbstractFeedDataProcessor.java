package com.threeglav.bauk.feed.processing;

import java.util.Map;

import com.threeglav.bauk.BaukConstants;
import com.threeglav.bauk.ConfigAware;
import com.threeglav.bauk.dimension.cache.HazelcastCacheInstanceManager;
import com.threeglav.bauk.feed.BulkOutputValuesResolver;
import com.threeglav.bauk.feed.FeedParserComponent;
import com.threeglav.bauk.feed.bulk.writer.BulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.FileBulkOutputWriter;
import com.threeglav.bauk.feed.bulk.writer.NullBulkOutputWriter;
import com.threeglav.bauk.model.BaukConfiguration;
import com.threeglav.bauk.model.BulkLoadDefinitionOutputType;
import com.threeglav.bauk.model.FactFeed;

public abstract class AbstractFeedDataProcessor extends ConfigAware implements FeedDataProcessor {

	protected final BulkOutputWriter bulkOutputWriter;
	protected final BulkOutputValuesResolver bulkoutputResolver;
	protected final FeedParserComponent feedParserComponent;

	public AbstractFeedDataProcessor(final FactFeed factFeed, final BaukConfiguration config, final String routeIdentifier) {
		super(factFeed, config);
		final BulkLoadDefinitionOutputType outputType = factFeed.getBulkLoadDefinition().getOutputType();
		if (outputType == BulkLoadDefinitionOutputType.FILE) {
			log.info("Will output bulk output results for feed {} to file", factFeed.getName());
			bulkOutputWriter = new FileBulkOutputWriter(factFeed, config);
			// bulkOutputWriter = new NIOFileBulkOutputWriter(factFeed, config);
		} else if (outputType == BulkLoadDefinitionOutputType.NONE) {
			log.info("Will not output any bulk output results for feed {}", factFeed.getName());
			bulkOutputWriter = new NullBulkOutputWriter();
		} else {
			throw new IllegalStateException("Unknown bulk output writer type " + outputType);
		}
		bulkoutputResolver = new BulkOutputValuesResolver(factFeed, config, routeIdentifier, new HazelcastCacheInstanceManager());
		feedParserComponent = new FeedParserComponent(factFeed, config, routeIdentifier);
	}

	@Override
	public void startFeed(final Map<String, String> globalAttributes) {
		log.debug("Starting new feed with global attributes {}", globalAttributes);
		bulkoutputResolver.startFeed(globalAttributes);
		bulkOutputWriter.initialize(globalAttributes.get(BaukConstants.IMPLICIT_ATTRIBUTE_BULK_LOAD_OUTPUT_FILE_PATH));
	}

	@Override
	public void closeFeed(final int expectedResults, final Map<String, String> globalAttributes) {
		bulkOutputWriter.closeResources(globalAttributes);
		bulkoutputResolver.closeCurrentFeed();
		log.debug("Closed feed. Expected results {}", expectedResults);
	}

}
